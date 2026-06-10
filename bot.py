#!/usr/bin/env python3
"""Discord watchdog + log relay bot for trunk-recorder.

Subscribes to the MQTT topics published by the trunk-recorder-mqtt-status
plugin (user_plugins/trunk-recorder-mqtt-status) and:

  * announces when trunk-recorder goes offline/online — the plugin registers
    an MQTT Last Will, so the broker publishes "disconnected" even when the
    process crashes hard
  * alerts when telemetry goes silent (hung process / dead plugin)
  * alerts when a system's control channel decode rate drops — the usual
    symptom of an SDR falling off USB, antenna, or RF problems
  * alerts on OpenMHz / Broadcastify Calls / Rdio Scanner upload failures,
    including calls permanently lost after retries
  * relays warning/error console log lines to a Discord channel

Configuration: config.yaml (see config.example.yaml). The Discord token can
also be supplied via the DISCORD_TOKEN environment variable.
"""

import argparse
import asyncio
import json
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Optional

import aiomqtt
import discord
import yaml
from discord import app_commands

log = logging.getLogger("tr-discord-bot")

SEVERITY_RANK = {"trace": 0, "debug": 1, "info": 2, "warning": 3, "error": 4, "fatal": 5}
SEVERITY_EMOJI = {"warning": "\N{WARNING SIGN}", "error": "\N{LARGE RED CIRCLE}", "fatal": "\N{SKULL}"}

GREEN = 0x2ECC71
ORANGE = 0xE67E22
RED = 0xE74C3C
BLUE = 0x3498DB

DEFAULT_UPLOAD_SERVICES = ["OpenMHz", "Broadcastify", "Rdio Scanner"]
UPLOAD_ERROR_RE = re.compile(r"Upload (?:Error|REJECTED|failed)", re.IGNORECASE)
UPLOAD_PERMANENT_RE = re.compile(r"Upload failed after \d+ retry attempts", re.IGNORECASE)


@dataclass
class SysHealth:
    last_rate: float = 0.0
    low_intervals: int = 0
    ok_intervals: int = 0
    down: bool = False
    last_alert: float = 0.0


@dataclass
class UploadHealth:
    failures: int = 0            # since last alert
    failures_total: int = 0
    permanent: int = 0           # since last alert
    permanent_total: int = 0
    last_alert: float = 0.0
    last_message: str = ""


def load_config(path: str) -> dict:
    with open(path, "r") as f:
        cfg = yaml.safe_load(f) or {}

    defaults = {
        "mqtt": {
            "host": "localhost",
            "port": 1883,
            "username": "",
            "password": "",
            "topic": "trunkrecorder/feeds",
        },
        "discord": {
            "token": "",
            "alerts_channel_id": 0,
            "logs_channel_id": 0,
            "mention_on_critical": "",
        },
        "watchdog": {
            "telemetry_timeout_s": 30,
            "decode_rate_min": 3.0,
            "decode_rate_intervals": 10,
            "recovery_intervals": 3,
            "upload_alert_cooldown_s": 600,
            "realert_interval_s": 1800,
            "extra_upload_services": [],
        },
        "logs": {
            "min_severity": "warning",
            "batch_interval_s": 5,
        },
    }
    for section, keys in defaults.items():
        cfg.setdefault(section, {})
        for k, v in keys.items():
            cfg[section].setdefault(k, v)
    return cfg


class TRBot(discord.Client):
    def __init__(self, cfg: dict):
        super().__init__(intents=discord.Intents.default())
        self.cfg = cfg
        self.tree = app_commands.CommandTree(self)

        self.base_topic = cfg["mqtt"]["topic"].rstrip("/")
        self.instance_id = "trunk-recorder"

        # Watchdog state
        self.online: Optional[bool] = None       # None until first status message
        self.mqtt_connected = False
        self.last_rates: Optional[float] = None
        self.telemetry_silent = False
        self.systems: dict[str, SysHealth] = {}
        self.uploads: dict[str, UploadHealth] = {}
        self.started = time.time()

        # Log relay
        self.log_queue: asyncio.Queue = asyncio.Queue()
        self.relay_min_rank = SEVERITY_RANK.get(cfg["logs"]["min_severity"].lower(), 3)

        services = DEFAULT_UPLOAD_SERVICES + list(cfg["watchdog"]["extra_upload_services"])
        self.upload_service_re = re.compile(
            r"(" + "|".join(re.escape(s) for s in services) + r")\s+(?:Metadata\s+|Audio\s+)?Upload",
            re.IGNORECASE,
        )

        @self.tree.command(name="trstatus", description="Show trunk-recorder watchdog status")
        async def trstatus(interaction: discord.Interaction):
            await interaction.response.send_message(embed=self.status_embed())

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup_hook(self):
        await self.tree.sync()
        asyncio.create_task(self.mqtt_loop(), name="mqtt_loop")
        asyncio.create_task(self.watchdog_loop(), name="watchdog_loop")
        asyncio.create_task(self.log_flusher(), name="log_flusher")

    async def on_ready(self):
        log.info("Logged in to Discord as %s", self.user)

    # ------------------------------------------------------------------
    # Discord helpers
    # ------------------------------------------------------------------

    async def get_channel_checked(self, channel_id: int):
        if not channel_id:
            return None
        ch = self.get_channel(channel_id)
        if ch is None:
            try:
                ch = await self.fetch_channel(channel_id)
            except discord.DiscordException:
                log.exception("Cannot access channel %s", channel_id)
                return None
        return ch

    async def send_alert(self, title: str, description: str, color: int, critical: bool = False):
        """Send an alert embed; retries so transient Discord errors don't lose alerts."""
        await self.wait_until_ready()
        ch = await self.get_channel_checked(self.cfg["discord"]["alerts_channel_id"])
        if ch is None:
            log.error("Alert channel unavailable, alert lost: %s — %s", title, description)
            return
        content = self.cfg["discord"]["mention_on_critical"] if critical else None
        embed = discord.Embed(title=title, description=description, color=color)
        embed.set_footer(text=self.instance_id)
        for attempt in range(5):
            try:
                await ch.send(content=content or None, embed=embed)
                return
            except discord.DiscordException:
                log.exception("Failed to send alert (attempt %d)", attempt + 1)
                await asyncio.sleep(5 * (attempt + 1))
        log.error("Giving up on alert: %s", title)

    def status_embed(self) -> discord.Embed:
        now = time.time()
        if self.online is None:
            state, color = "unknown (no status message yet)", BLUE
        elif self.online:
            state, color = "online", GREEN
        else:
            state, color = "OFFLINE", RED

        embed = discord.Embed(title="Trunk Recorder status", color=color)
        embed.add_field(name="Recorder", value=state, inline=True)
        embed.add_field(name="MQTT broker", value="connected" if self.mqtt_connected else "DISCONNECTED", inline=True)
        if self.last_rates is not None:
            embed.add_field(name="Last telemetry", value=f"{now - self.last_rates:.0f}s ago", inline=True)
        for name, h in sorted(self.systems.items()):
            flag = " \N{LARGE RED CIRCLE} DOWN" if h.down else ""
            embed.add_field(name=f"System: {name}", value=f"{h.last_rate:.1f} msg/s{flag}", inline=True)
        for svc, u in sorted(self.uploads.items()):
            embed.add_field(
                name=f"Uploads: {svc}",
                value=f"{u.failures_total} failure(s), {u.permanent_total} call(s) lost",
                inline=True,
            )
        embed.set_footer(text=f"{self.instance_id} • bot up {(now - self.started) / 3600:.1f}h")
        return embed

    # ------------------------------------------------------------------
    # MQTT
    # ------------------------------------------------------------------

    async def mqtt_loop(self):
        cfg = self.cfg["mqtt"]
        delay = 5
        announced_loss = False
        while not self.is_closed():
            try:
                client_args = {"hostname": cfg["host"], "port": int(cfg["port"])}
                if cfg["username"]:
                    client_args["username"] = cfg["username"]
                    client_args["password"] = cfg["password"]
                async with aiomqtt.Client(**client_args) as client:
                    await client.subscribe(f"{self.base_topic}/trunk_recorder/status")
                    await client.subscribe(f"{self.base_topic}/rates")
                    await client.subscribe(f"{self.base_topic}/trunk_recorder/console")
                    log.info("Connected to MQTT broker %s:%s", cfg["host"], cfg["port"])
                    self.mqtt_connected = True
                    # Don't count broker downtime as recorder telemetry silence.
                    self.last_rates = time.time()
                    if announced_loss:
                        announced_loss = False
                        await self.send_alert("Watchdog reconnected",
                                              "Bot reconnected to the MQTT broker; monitoring resumed.", GREEN)
                    delay = 5
                    async for message in client.messages:
                        await self.handle_mqtt(str(message.topic), message.payload)
            except aiomqtt.MqttError as exc:
                first_loss = self.mqtt_connected
                self.mqtt_connected = False
                log.warning("MQTT connection error: %s (retry in %ds)", exc, delay)
                if first_loss and not announced_loss:
                    announced_loss = True
                    await self.send_alert(
                        "Watchdog blind",
                        f"Bot lost its connection to the MQTT broker (`{exc}`). "
                        "Monitoring is suspended until it reconnects — trunk-recorder itself may be fine.",
                        ORANGE, critical=True)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 300)

    async def handle_mqtt(self, topic: str, payload: bytes):
        try:
            data = json.loads(payload.decode("utf-8", errors="replace")) if payload else {}
        except json.JSONDecodeError:
            log.warning("Unparseable payload on %s", topic)
            return
        if not isinstance(data, dict):
            return
        self.instance_id = data.get("instance_id", self.instance_id)

        if topic.endswith("/trunk_recorder/status"):
            await self.handle_status(data)
        elif topic.endswith("/rates"):
            self.handle_rates(data)
        elif topic.endswith("/trunk_recorder/console"):
            await self.handle_console(data)

    # ------------------------------------------------------------------
    # Recorder online/offline (LWT)
    # ------------------------------------------------------------------

    async def handle_status(self, data: dict):
        status = data.get("status")
        if status not in ("connected", "disconnected"):
            return
        online = status == "connected"

        if self.online is None:
            self.online = online
            if online:
                await self.send_alert("Watchdog started",
                                      "Trunk Recorder is currently **online**.", GREEN)
            else:
                await self.send_alert(
                    "Watchdog started — recorder OFFLINE",
                    "The retained status at the broker says Trunk Recorder is **offline** "
                    "(stopped, crashed, or never started).", RED, critical=True)
            return

        if online == self.online:
            return
        self.online = online
        if online:
            self.last_rates = time.time()
            self.telemetry_silent = False
            await self.send_alert("Trunk Recorder is back online",
                                  "MQTT status changed to **connected**.", GREEN)
        else:
            await self.send_alert(
                "Trunk Recorder went OFFLINE",
                "The broker published the plugin's Last Will: the process **crashed, was stopped, "
                "or lost its MQTT connection**. If systemd manages it, check "
                "`systemctl status trunk-recorder` and the journal.", RED, critical=True)

    # ------------------------------------------------------------------
    # Decode rates (SDR / control channel health) — every ~3s
    # ------------------------------------------------------------------

    def handle_rates(self, data: dict):
        self.last_rates = time.time()
        for entry in data.get("rates") or []:
            name = entry.get("sys_name") or f"sys{entry.get('sys_num', '?')}"
            try:
                rate = float(entry.get("decoderate", 0.0))
            except (TypeError, ValueError):
                continue
            h = self.systems.setdefault(name, SysHealth())
            h.last_rate = rate
            w = self.cfg["watchdog"]
            now = time.time()

            if rate < float(w["decode_rate_min"]):
                h.low_intervals += 1
                h.ok_intervals = 0
                if not h.down and h.low_intervals >= int(w["decode_rate_intervals"]):
                    h.down = True
                    h.last_alert = now
                    asyncio.create_task(self.send_alert(
                        f"Decode rate low: {name}",
                        f"Control channel decode rate has been below {w['decode_rate_min']} msg/s for "
                        f"{h.low_intervals} consecutive updates (currently {rate:.1f} msg/s).\n"
                        "Likely causes: **SDR dropped off USB**, antenna/RF problem, or the control "
                        "channel moved. Trunk-recorder will try to retune; if it hits its retune "
                        "limit it exits (which this bot reports as OFFLINE).", ORANGE, critical=True))
                elif h.down and now - h.last_alert >= float(w["realert_interval_s"]):
                    h.last_alert = now
                    asyncio.create_task(self.send_alert(
                        f"Still down: {name}",
                        f"Decode rate is still below {w['decode_rate_min']} msg/s "
                        f"(currently {rate:.1f} msg/s).", ORANGE))
            else:
                h.ok_intervals += 1
                h.low_intervals = 0
                if h.down and h.ok_intervals >= int(w["recovery_intervals"]):
                    h.down = False
                    asyncio.create_task(self.send_alert(
                        f"Recovered: {name}",
                        f"Decode rate is back to {rate:.1f} msg/s.", GREEN))

    # ------------------------------------------------------------------
    # Console log lines: relay + upload failure classification
    # ------------------------------------------------------------------

    async def handle_console(self, data: dict):
        entry = data.get("console") or {}
        severity = str(entry.get("severity", "info")).lower()
        message = str(entry.get("log_msg", "")).strip()
        if not message:
            return
        rank = SEVERITY_RANK.get(severity, 2)

        if self.cfg["discord"]["logs_channel_id"] and rank >= self.relay_min_rank:
            stamp = str(entry.get("time", ""))[11:19]  # HH:MM:SS from ISO time
            emoji = SEVERITY_EMOJI.get(severity, "")
            line = f"[{stamp}] {emoji}({severity}) {message}"
            self.log_queue.put_nowait(line[:1800])

        if rank >= SEVERITY_RANK["error"]:
            await self.classify_upload_failure(message)

    async def classify_upload_failure(self, message: str):
        permanent = bool(UPLOAD_PERMANENT_RE.search(message))
        if not permanent and not UPLOAD_ERROR_RE.search(message):
            return
        m = self.upload_service_re.search(message)
        service = m.group(1) if m else ("call concluder" if permanent else "unknown uploader")

        u = self.uploads.setdefault(service, UploadHealth())
        if permanent:
            u.permanent += 1
            u.permanent_total += 1
        else:
            u.failures += 1
            u.failures_total += 1
        u.last_message = message[:400]

        now = time.time()
        if now - u.last_alert < float(self.cfg["watchdog"]["upload_alert_cooldown_s"]):
            return
        u.last_alert = now

        lines = []
        if u.failures:
            lines.append(f"**{u.failures}** upload failure(s) since the last alert (retries may still succeed).")
        if u.permanent:
            lines.append(f"**{u.permanent}** call(s) **permanently failed** after exhausting retries.")
        lines.append(f"Latest: `{u.last_message}`")
        await self.send_alert(
            f"Upload problems: {service}",
            "\n".join(lines),
            RED if u.permanent else ORANGE,
            critical=bool(u.permanent),
        )
        u.failures = 0
        u.permanent = 0

    # ------------------------------------------------------------------
    # Telemetry silence watchdog
    # ------------------------------------------------------------------

    async def watchdog_loop(self):
        await self.wait_until_ready()
        timeout = float(self.cfg["watchdog"]["telemetry_timeout_s"])
        while not self.is_closed():
            now = time.time()
            if self.mqtt_connected and self.online is not False and self.last_rates is not None:
                silent = (now - self.last_rates) > timeout
                if silent and not self.telemetry_silent:
                    self.telemetry_silent = True
                    await self.send_alert(
                        "Telemetry silent",
                        f"No decode-rate telemetry for over {timeout:.0f}s while the recorder still "
                        "appears connected. The process may be **hung** or the MQTT plugin is wedged.",
                        ORANGE, critical=True)
                elif not silent and self.telemetry_silent:
                    self.telemetry_silent = False
                    await self.send_alert("Telemetry resumed",
                                          "Decode-rate telemetry is flowing again.", GREEN)
            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Log relay flusher — batches lines, never drops them
    # ------------------------------------------------------------------

    async def log_flusher(self):
        await self.wait_until_ready()
        interval = float(self.cfg["logs"]["batch_interval_s"])
        while not self.is_closed():
            lines = [await self.log_queue.get()]
            await asyncio.sleep(interval)  # linger so bursts coalesce into one message
            while not self.log_queue.empty() and len(lines) < 500:
                lines.append(self.log_queue.get_nowait())

            chunks, current = [], ""
            for line in lines:
                if len(current) + len(line) + 1 > 1900:
                    chunks.append(current)
                    current = ""
                current += line + "\n"
            if current:
                chunks.append(current)

            ch = await self.get_channel_checked(self.cfg["discord"]["logs_channel_id"])
            if ch is None:
                log.error("Log channel unavailable; %d line(s) lost", len(lines))
                continue
            for chunk in chunks:
                while True:
                    try:
                        await ch.send(f"```text\n{chunk}```")
                        break
                    except discord.DiscordException:
                        log.exception("Failed to relay logs; retrying")
                        await asyncio.sleep(10)


def main():
    parser = argparse.ArgumentParser(description="trunk-recorder Discord watchdog bot")
    parser.add_argument("-c", "--config", default="config.yaml", help="path to config.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = load_config(args.config)
    token = os.environ.get("DISCORD_TOKEN") or cfg["discord"]["token"]
    if not token:
        raise SystemExit("No Discord token: set discord.token in config.yaml or the DISCORD_TOKEN env var")
    if not cfg["discord"]["alerts_channel_id"]:
        raise SystemExit("discord.alerts_channel_id must be set")

    TRBot(cfg).run(token, log_handler=None)


if __name__ == "__main__":
    main()
