#!/usr/bin/env python3
"""Discord watchdog + log relay bot for trunk-recorder.

Runs on the same host as trunk-recorder and watches it directly — no broker,
no plugin:

  * journald tail (`journalctl -u <unit> -f -o json`): relays warning/error
    log lines to a Discord channel, classifies upload failures (OpenMHz /
    Broadcastify Calls / Rdio Scanner, including calls permanently lost after
    retries), and tracks control-channel decode rates from trunk-recorder's
    own rate log lines (SDR / antenna / RF problems)
  * systemd unit watcher: online/offline transitions, single restarts, crash
    loops, and failed state — with the last journal lines attached, so
    startup errors like a missing SDR reach Discord
  * USB dongle watcher (udev): alerts the moment a configured SDR leaves or
    rejoins the bus, and checks presence at startup
  * optional log-silence hang detection: set `controlWarnRate: -1` in
    trunk-recorder's config.json so it logs its decode rate every ~3 s, then
    journal silence while the unit is active means the process is hung

Configuration: config.yaml (see config.example.yaml). The Discord token can
also be supplied via the DISCORD_TOKEN environment variable.
"""

import argparse
import asyncio
import json
import logging
import os
import re
import shlex
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional

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

# trunk-recorder's Boost.Log console format: "[<timestamp>] (<severity>)   <message>"
BOOST_LINE_RE = re.compile(
    r"^\[(?P<ts>[^\]]*)\]\s+\((?P<sev>trace|debug|info|warning|error|fatal)\)\s*(?P<body>.*)$")
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
TIME_RE = re.compile(r"(\d{2}:\d{2}:\d{2})")

# monitor_systems.cc: "[<shortname>]\tfreq: ...\tControl Channel Message Decode Rate: <n>/sec, count: ..."
RATE_RE = re.compile(r"\[(?P<sys>[^\]]+)\].*Control Channel Message Decode Rate:\s*(?P<rate>-?\d+)/sec")

# Probe output classification. Exit codes are useless here — rtl_test exits 0
# even when it fails to open the device — so the output text is the verdict.
PROBE_OK_RE = re.compile(
    r"Found .* tuner|Supported gain values|Sampling at|Serial number:|Firmware Version", re.IGNORECASE)
PROBE_BUSY_RE = re.compile(
    r"usb_claim_interface|resource busy|already in use|\bbusy\b", re.IGNORECASE)
PROBE_FAIL_RE = re.compile(
    r"Failed to open|No supported devices|no device|not found|cannot open|Permission denied", re.IGNORECASE)

DEFAULT_UPLOAD_SERVICES = ["OpenMHz", "Broadcastify", "Rdio Scanner"]
UPLOAD_ERROR_RE = re.compile(r"Upload (?:Error|REJECTED|failed)", re.IGNORECASE)
UPLOAD_PERMANENT_RE = re.compile(r"Upload failed after \d+ retry attempts", re.IGNORECASE)


@dataclass
class SysHealth:
    last_rate: float = 0.0
    last_seen: float = 0.0       # when a rate line for this system last appeared
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
        "discord": {
            "token": "",
            "alerts_channel_id": 0,
            "logs_channel_id": 0,
            "mention_on_critical": "",
        },
        "watchdog": {
            "decode_rate_min": 3.0,
            "decode_rate_intervals": 10,
            "recovery_intervals": 3,
            "decode_rate_clear_s": 30,
            "log_silence_timeout_s": 0,
            "upload_alert_cooldown_s": 600,
            "realert_interval_s": 1800,
            "extra_upload_services": [],
        },
        "logs": {
            "min_severity": "warning",
            "batch_interval_s": 5,
        },
        "usb": {
            "devices": [],
            "probe_timeout_s": 15,
        },
        "systemd": {
            "unit": "trunk-recorder.service",
            "poll_interval_s": 10,
            "journal_lines": 25,
            "restart_window_s": 300,
            "restart_threshold": 3,
            "restart_alert_cooldown_s": 600,
            "restart_cmd": "sudo -n systemctl restart {unit}",
        },
    }
    for section, keys in defaults.items():
        cfg.setdefault(section, {})
        for k, v in keys.items():
            cfg[section].setdefault(k, v)
    return cfg


class TRBot(discord.Client):
    def __init__(self, cfg: dict):
        super().__init__(
            intents=discord.Intents.default(),
            # Honor whatever mention_on_critical contains (@everyone/@here/roles);
            # the bot also needs the "Mention @everyone" permission in the channel.
            allowed_mentions=discord.AllowedMentions(everyone=True, roles=True, users=True))
        self.cfg = cfg
        self.tree = app_commands.CommandTree(self)
        self.unit = cfg["systemd"]["unit"]

        # Watchdog state
        self.online: Optional[bool] = None       # None until first systemd poll
        self.unit_state = "unknown"
        self.unit_restarts: Optional[int] = None
        self.crash_looping = False
        self.last_journal: Optional[float] = None
        self.log_silent = False
        self.systems: dict[str, SysHealth] = {}
        self.uploads: dict[str, UploadHealth] = {}
        self.usb_present: dict[str, bool] = {}    # label -> currently on the bus
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

        @self.tree.command(name="trprobe", description="Actively probe the configured SDR dongles (driver-level check)")
        async def trprobe(interaction: discord.Interaction):
            await interaction.response.defer()
            embed = await self.probe_embed()
            await interaction.followup.send(embed=embed)

        @self.tree.command(name="trrestart", description="Restart the trunk-recorder systemd service")
        @app_commands.describe(confirm="Set to True to actually restart — this stops any in-progress recordings")
        @app_commands.default_permissions(manage_guild=True)
        async def trrestart(interaction: discord.Interaction, confirm: bool = False):
            if not confirm:
                await interaction.response.send_message(
                    f"This will restart `{self.unit}` and stop any in-progress recordings.\n"
                    "Re-run as `/trrestart confirm: True` to proceed.", ephemeral=True)
                return
            await interaction.response.defer()
            embed = await self.restart_unit(str(interaction.user))
            await interaction.followup.send(embed=embed)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def setup_hook(self):
        await self.tree.sync()
        asyncio.create_task(self.journal_watcher(), name="journal_watcher")
        asyncio.create_task(self.systemd_watcher(), name="systemd_watcher")
        asyncio.create_task(self.watchdog_loop(), name="watchdog_loop")
        asyncio.create_task(self.log_flusher(), name="log_flusher")
        asyncio.create_task(self.usb_watcher(), name="usb_watcher")

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
        embed.set_footer(text=self.unit)
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
            state, color = "unknown (no systemd poll yet)", BLUE
        elif self.online:
            state, color = "online", GREEN
        else:
            state, color = "OFFLINE", RED
        if self.crash_looping:
            state += " (crash-looping)"
            color = RED

        embed = discord.Embed(title="Trunk Recorder status", color=color)
        restarts = "?" if self.unit_restarts is None else str(self.unit_restarts)
        embed.add_field(name=f"Unit: {self.unit}",
                        value=f"{state} ({self.unit_state}, {restarts} restart(s))", inline=True)
        if self.last_journal is not None:
            embed.add_field(name="Last log line", value=f"{now - self.last_journal:.0f}s ago", inline=True)
        for name, h in sorted(self.systems.items()):
            flag = " \N{LARGE RED CIRCLE} DOWN" if h.down else ""
            age = f", {now - h.last_seen:.0f}s ago" if h.last_seen else ""
            embed.add_field(name=f"System: {name}", value=f"{h.last_rate:.0f} msg/s{age}{flag}", inline=True)
        for svc, u in sorted(self.uploads.items()):
            embed.add_field(
                name=f"Uploads: {svc}",
                value=f"{u.failures_total} failure(s), {u.permanent_total} call(s) lost",
                inline=True,
            )
        for label, present in sorted(self.usb_present.items()):
            embed.add_field(name=f"SDR: {label}",
                            value="on the bus" if present else "\N{LARGE RED CIRCLE} MISSING", inline=True)
        embed.set_footer(text=f"{self.unit} • bot up {(now - self.started) / 3600:.1f}h")
        return embed

    # ------------------------------------------------------------------
    # Journal tail — log relay, upload failures, decode rates
    # ------------------------------------------------------------------

    async def journal_watcher(self):
        await self.wait_until_ready()
        delay = 5
        announced = False
        while not self.is_closed():
            proc = None
            try:
                proc = await asyncio.create_subprocess_exec(
                    "journalctl", "-u", self.unit, "-f", "-n", "0", "-o", "json",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
                log.info("Tailing journal for %s", self.unit)
                while True:
                    raw = await proc.stdout.readline()
                    if not raw:
                        break
                    delay = 5
                    try:
                        rec = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    message = rec.get("MESSAGE", "")
                    if isinstance(message, list):  # journald encodes non-UTF8 as a byte array
                        message = bytes(message).decode("utf-8", errors="replace")
                    await self.handle_log_line(ANSI_RE.sub("", str(message)))
                err = (await proc.stderr.read()).decode(errors="replace").strip()
                raise RuntimeError(err[:300] or f"journalctl exited ({proc.returncode})")
            except (FileNotFoundError, RuntimeError) as exc:
                log.error("journal tail failed: %s (retry in %ds)", exc, delay)
                if not announced:
                    announced = True
                    await self.send_alert(
                        "Watchdog blind: journal unavailable",
                        f"`journalctl -u {self.unit} -f` failed: `{exc}`\n"
                        "Log relay, upload-failure, and decode-rate monitoring are suspended. "
                        "Is the bot's user in the `systemd-journal` group?", ORANGE, critical=True)
            finally:
                if proc and proc.returncode is None:
                    proc.kill()
            await asyncio.sleep(delay)
            delay = min(delay * 2, 300)

    async def handle_log_line(self, message: str):
        self.last_journal = time.time()
        m = BOOST_LINE_RE.match(message)
        if m:
            severity, body, ts = m.group("sev"), m.group("body").strip(), m.group("ts")
        else:
            # Not a Boost.Log line (e.g. gr-osmosdr/UHD writing straight to stderr)
            severity, body, ts = "info", message.strip(), ""
        if not body:
            return
        rank = SEVERITY_RANK.get(severity, 2)

        if self.cfg["discord"]["logs_channel_id"] and rank >= self.relay_min_rank:
            tm = TIME_RE.search(ts)
            stamp = tm.group(1) if tm else time.strftime("%H:%M:%S")
            emoji = SEVERITY_EMOJI.get(severity, "")
            self.log_queue.put_nowait(f"[{stamp}] {emoji}({severity}) {body}"[:1800])

        rate_match = RATE_RE.search(body)
        if rate_match:
            await self.handle_decode_rate(rate_match.group("sys"), float(rate_match.group("rate")))

        if rank >= SEVERITY_RANK["error"]:
            await self.classify_upload_failure(body)

    # ------------------------------------------------------------------
    # Decode rates (SDR / control channel health)
    #
    # trunk-recorder logs a rate line every ~3s whenever the decode rate is
    # below controlWarnRate (default 10), and at info level every ~3s for all
    # rates if controlWarnRate is -1. Healthy systems with the default config
    # log nothing, so recovery is also detected by the *absence* of low-rate
    # lines (decode_rate_clear_s in watchdog_loop).
    # ------------------------------------------------------------------

    async def handle_decode_rate(self, name: str, rate: float):
        w = self.cfg["watchdog"]
        now = time.time()
        h = self.systems.setdefault(name, SysHealth())
        h.last_rate = rate
        h.last_seen = now

        if rate < float(w["decode_rate_min"]):
            h.low_intervals += 1
            h.ok_intervals = 0
            if not h.down and h.low_intervals >= int(w["decode_rate_intervals"]):
                h.down = True
                h.last_alert = now
                await self.send_alert(
                    f"Decode rate low: {name}",
                    f"Control channel decode rate has been below {w['decode_rate_min']} msg/s for "
                    f"{h.low_intervals} consecutive updates (currently {rate:.0f} msg/s).\n"
                    "Likely causes: **SDR dropped off USB**, antenna/RF problem, or the control "
                    "channel moved.", ORANGE, critical=True)
            elif h.down and now - h.last_alert >= float(w["realert_interval_s"]):
                h.last_alert = now
                await self.send_alert(
                    f"Still down: {name}",
                    f"Decode rate is still below {w['decode_rate_min']} msg/s "
                    f"(currently {rate:.0f} msg/s).", ORANGE)
        else:
            h.ok_intervals += 1
            h.low_intervals = 0
            if h.down and h.ok_intervals >= int(w["recovery_intervals"]):
                h.down = False
                await self.send_alert(f"Recovered: {name}",
                                      f"Decode rate is back to {rate:.0f} msg/s.", GREEN)

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
    # systemd unit watcher — online/offline, restarts, crash loops
    # ------------------------------------------------------------------

    async def journal_tail(self, lines: int) -> str:
        try:
            proc = await asyncio.create_subprocess_exec(
                "journalctl", "-u", self.unit, "-n", str(lines), "--no-pager", "-o", "cat",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, err = await proc.communicate()
            if proc.returncode != 0:
                return f"(journalctl failed: {err.decode(errors='replace').strip()[:200]})"
            return ANSI_RE.sub("", out.decode(errors="replace")).strip()[-900:] or "(journal empty)"
        except FileNotFoundError:
            return "(journalctl not available)"

    async def unit_props(self) -> Optional[dict]:
        try:
            proc = await asyncio.create_subprocess_exec(
                "systemctl", "show", self.unit, "-p", "ActiveState,SubState,NRestarts,ExecMainStatus",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            out, _ = await proc.communicate()
        except FileNotFoundError:
            return None
        return dict(line.split("=", 1) for line in out.decode(errors="replace").splitlines() if "=" in line)

    async def restart_unit(self, requested_by: str) -> discord.Embed:
        """Run the configured restart command and report the resulting unit state."""
        cmd = self.cfg["systemd"]["restart_cmd"].format(unit=self.unit)
        log.warning("Restart of %s requested by %s (%s)", self.unit, requested_by, cmd)
        await self.send_alert(f"Manual restart: {self.unit}",
                              f"Restart requested by **{requested_by}** via /trrestart.", BLUE)
        try:
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(cmd),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=30)
        except FileNotFoundError as exc:
            return discord.Embed(title="Restart failed", color=RED,
                                 description=f"Cannot run restart command: `{exc}`")
        except asyncio.TimeoutError:
            proc.kill()
            return discord.Embed(title="Restart failed", color=RED,
                                 description="Restart command timed out after 30s.")
        text = out.decode(errors="replace").strip()
        if proc.returncode != 0:
            hint = ""
            if "sudo" in cmd and ("password" in text.lower() or "sudoers" in text.lower()):
                hint = ("\nThe bot's user needs a sudoers rule, e.g.:\n"
                        f"```\n<bot-user> ALL=(root) NOPASSWD: /usr/bin/systemctl restart {self.unit}\n```")
            return discord.Embed(title="Restart failed", color=RED,
                                 description=f"`{cmd}` exited {proc.returncode}:\n"
                                             f"```text\n{text[:600] or '(no output)'}\n```{hint}")

        # Restart command succeeded — wait for the unit to come back up.
        for _ in range(10):
            await asyncio.sleep(2)
            props = await self.unit_props()
            if props and props.get("ActiveState") == "active" and props.get("SubState") == "running":
                return discord.Embed(
                    title=f"{self.unit} restarted", color=GREEN,
                    description=f"Unit is **active/running** (requested by {requested_by}).")
        props = await self.unit_props() or {}
        state = f"{props.get('ActiveState', '?')}/{props.get('SubState', '?')}"
        journal = await self.journal_tail(int(self.cfg["systemd"]["journal_lines"]))
        return discord.Embed(
            title=f"{self.unit} restart: not running", color=RED,
            description=f"The restart command succeeded but the unit is **{state}** after 20s. "
                        f"Recent journal:\n```text\n{journal}\n```")

    async def update_online(self, online: bool, exec_status: str):
        if self.online is None:
            self.online = online
            if online:
                await self.send_alert("Watchdog started",
                                      f"`{self.unit}` is currently **active**.", GREEN)
            else:
                await self.send_alert(
                    f"Watchdog started — {self.unit} not running",
                    f"The unit is **{self.unit_state}**.", RED, critical=True)
            return
        if online == self.online:
            return
        self.online = online
        if self.crash_looping:
            return  # the crash-loop alert covers the churn
        if online:
            await self.send_alert(f"{self.unit} is back online",
                                  "The unit is active and running.", GREEN)
        else:
            journal = await self.journal_tail(int(self.cfg["systemd"]["journal_lines"]))
            await self.send_alert(
                f"{self.unit} went OFFLINE",
                f"The unit is **{self.unit_state}** (last exit status {exec_status}). "
                f"Recent journal:\n```text\n{journal}\n```", RED, critical=True)

    async def systemd_watcher(self):
        sd = self.cfg["systemd"]
        await self.wait_until_ready()
        restart_times: deque = deque(maxlen=50)
        last_restart_alert = 0.0
        failed_alerted = False

        while not self.is_closed():
            props = await self.unit_props()
            if props is None:
                log.error("systemctl not available; cannot watch the unit")
                return
            active = props.get("ActiveState", "unknown")
            sub = props.get("SubState", "")
            exec_status = props.get("ExecMainStatus", "?")
            self.unit_state = f"{active}/{sub}"
            try:
                restarts = int(props.get("NRestarts", "0") or 0)
            except ValueError:
                restarts = 0
            now = time.time()

            if self.unit_restarts is not None and restarts > self.unit_restarts:
                restart_times.extend([now] * (restarts - self.unit_restarts))
                recent = [t for t in restart_times if now - t < float(sd["restart_window_s"])]
                if len(recent) >= int(sd["restart_threshold"]):
                    if not self.crash_looping:
                        self.crash_looping = True
                        journal = await self.journal_tail(int(sd["journal_lines"]))
                        await self.send_alert(
                            f"{self.unit} is crash-looping",
                            f"**{len(recent)}** restarts in the last {int(sd['restart_window_s']) // 60} "
                            f"minutes (exit status {exec_status}). Recent journal:\n"
                            f"```text\n{journal}\n```", RED, critical=True)
                        # Tell driver/hardware trouble apart from a config problem
                        await self.send_probe_report("crash-loop diagnosis")
                elif now - last_restart_alert >= float(sd["restart_alert_cooldown_s"]):
                    last_restart_alert = now
                    journal = await self.journal_tail(int(sd["journal_lines"]))
                    await self.send_alert(
                        f"{self.unit} restarted",
                        f"systemd restarted the unit (exit status {exec_status}, "
                        f"restart #{restarts}). Recent journal:\n```text\n{journal}\n```",
                        ORANGE, critical=True)
            elif self.crash_looping and active == "active" and sub == "running" \
                    and not [t for t in restart_times if now - t < float(sd["restart_window_s"])]:
                self.crash_looping = False
                await self.send_alert(f"{self.unit} stable again",
                                      "The unit has been running without restarts.", GREEN)
            self.unit_restarts = restarts

            if active == "failed" and not failed_alerted:
                failed_alerted = True
                journal = await self.journal_tail(int(sd["journal_lines"]))
                await self.send_alert(
                    f"{self.unit} FAILED",
                    f"systemd gave up on the unit (state {self.unit_state}). It will **not** restart "
                    f"on its own. Recent journal:\n```text\n{journal}\n```", RED, critical=True)
            elif active != "failed":
                failed_alerted = False

            # "activating"/auto-restart states are transient; don't flip online state on them
            if active in ("active", "inactive", "failed"):
                await self.update_online(active == "active", exec_status)

            await asyncio.sleep(float(sd["poll_interval_s"]))

    # ------------------------------------------------------------------
    # Periodic checks: log-silence hang detection, decode-rate clear
    # ------------------------------------------------------------------

    async def watchdog_loop(self):
        await self.wait_until_ready()
        w = self.cfg["watchdog"]
        silence_timeout = float(w["log_silence_timeout_s"])
        clear_s = float(w["decode_rate_clear_s"])
        while not self.is_closed():
            now = time.time()

            if silence_timeout > 0 and self.online and self.last_journal is not None:
                silent = (now - self.last_journal) > silence_timeout
                if silent and not self.log_silent:
                    self.log_silent = True
                    await self.send_alert(
                        "Log output silent",
                        f"No journal output from `{self.unit}` for over {silence_timeout:.0f}s while "
                        "the unit is still active. The process may be **hung**. (This check assumes "
                        "`controlWarnRate: -1` so trunk-recorder logs its decode rate every ~3s.)",
                        ORANGE, critical=True)
                elif not silent and self.log_silent:
                    self.log_silent = False
                    await self.send_alert("Log output resumed",
                                          "Journal output is flowing again.", GREEN)

            # With the default controlWarnRate (10), healthy systems log no rate
            # lines at all — so "the low-rate error lines stopped" means recovered.
            for name, h in self.systems.items():
                if h.down and h.last_seen and (now - h.last_seen) > clear_s and self.online:
                    h.down = False
                    h.low_intervals = 0
                    await self.send_alert(
                        f"Recovered: {name}",
                        f"No low decode-rate lines for {clear_s:.0f}s — the rate is back above "
                        "trunk-recorder's warn threshold.", GREEN)

            await asyncio.sleep(5)

    # ------------------------------------------------------------------
    # Active dongle probes — driver-level health, beyond bus presence
    #
    # Never run periodically while trunk-recorder is recording: an SDR can
    # only be opened by one process, so probing a healthy in-use dongle just
    # reports "claimed" (and the decode rate already proves the driver works
    # end-to-end). Probes run on demand (/trprobe), when a crash loop is
    # detected, and after a dongle returns to the bus.
    # ------------------------------------------------------------------

    async def probe_device(self, spec: dict) -> tuple:
        """Run the device's probe command. Returns (status, detail) where
        status is ok | claimed | failed | no-probe."""
        cmd = spec.get("probe", "")
        if not cmd:
            return "no-probe", "no probe command configured"
        try:
            proc = await asyncio.create_subprocess_exec(
                *shlex.split(cmd),
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT)
        except (FileNotFoundError, ValueError) as exc:
            return "failed", f"cannot run probe: {exc}"
        timed_out = False
        try:
            out, _ = await asyncio.wait_for(
                proc.communicate(), timeout=float(self.cfg["usb"]["probe_timeout_s"]))
        except asyncio.TimeoutError:
            timed_out = True
            proc.kill()
            out, _ = await proc.communicate()
        text = out.decode(errors="replace")
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        detail = (lines[-1] if lines else "(no output)")[:200]
        # Output beats exit codes: rtl_test exits 0 even on open failure.
        if PROBE_BUSY_RE.search(text):
            return "claimed", detail
        if PROBE_OK_RE.search(text):
            return "ok", detail
        if PROBE_FAIL_RE.search(text):
            return "failed", detail
        if timed_out:
            return "failed", f"probe timed out: {detail}"
        return ("ok" if proc.returncode == 0 else "failed"), detail

    def probe_verdict(self, status: str) -> str:
        if status == "ok":
            return "\N{WHITE HEAVY CHECK MARK} opens and responds — driver working"
        if status == "claimed":
            if self.online:
                return "\N{LOCK} claimed by another process (expected — trunk-recorder is running)"
            return ("\N{WARNING SIGN} claimed by another process while trunk-recorder is **not** "
                    "running — something else is holding the dongle and trunk-recorder will fail to open it")
        if status == "no-probe":
            return "\N{WHITE QUESTION MARK ORNAMENT} bus presence only (no probe command configured)"
        return "\N{CROSS MARK} present on the bus but the driver **cannot open it** — power-cycle / replug likely needed"

    async def probe_all(self) -> list:
        results = []
        for spec in self.cfg["usb"]["devices"]:
            label = spec.get("label", f"{spec.get('vendor_id')}:{spec.get('product_id')}")
            present = self.usb_present.get(label, None)
            status, detail = await self.probe_device(spec)
            results.append((label, present, status, detail))
        return results

    async def probe_embed(self) -> discord.Embed:
        results = await self.probe_all()
        if not results:
            return discord.Embed(title="Dongle probe", color=BLUE,
                                 description="No `usb.devices` configured.")
        if any(s == "failed" for _, _, s, _ in results):
            color = RED
        elif any(s == "claimed" for _, _, s, _ in results) and not self.online:
            color = ORANGE  # something other than trunk-recorder is holding a dongle
        else:
            color = GREEN
        embed = discord.Embed(title="Dongle probe", color=color)
        for label, present, status, detail in results:
            bus = {True: "on the bus", False: "\N{LARGE RED CIRCLE} NOT on the bus", None: "bus state unknown"}[present]
            embed.add_field(name=label,
                            value=f"{bus}\n{self.probe_verdict(status)}\n`{detail}`",
                            inline=False)
        embed.set_footer(text=self.unit)
        return embed

    async def send_probe_report(self, reason: str):
        if not any(spec.get("probe") for spec in self.cfg["usb"]["devices"]):
            return
        embed = await self.probe_embed()
        embed.title = f"Dongle probe — {reason}"
        ch = await self.get_channel_checked(self.cfg["discord"]["alerts_channel_id"])
        if ch is not None:
            try:
                await ch.send(embed=embed)
            except discord.DiscordException:
                log.exception("Failed to send probe report")

    # ------------------------------------------------------------------
    # USB dongle watcher (udev) — works even when trunk-recorder is dead
    # ------------------------------------------------------------------

    @staticmethod
    def _usb_ids(device) -> tuple:
        vid = (device.get("ID_VENDOR_ID") or "").lower()
        pid = (device.get("ID_MODEL_ID") or "").lower()
        serial = device.get("ID_SERIAL_SHORT") or ""
        if not vid and device.get("PRODUCT"):
            # remove events sometimes only carry PRODUCT=vid/pid/rev
            parts = device.get("PRODUCT").split("/")
            if len(parts) >= 2:
                vid, pid = parts[0].zfill(4).lower(), parts[1].zfill(4).lower()
        return vid, pid, serial

    def _match_usb_spec(self, vid: str, pid: str, serial: str) -> Optional[dict]:
        for spec in self.cfg["usb"]["devices"]:
            if (str(spec.get("vendor_id", "")).lower() == vid
                    and str(spec.get("product_id", "")).lower() == pid
                    and (not spec.get("serial") or str(spec["serial"]) == serial)):
                return spec
        return None

    async def usb_watcher(self):
        specs = self.cfg["usb"]["devices"]
        if not specs:
            return
        try:
            import pyudev
        except ImportError:
            log.error("usb.devices configured but pyudev is not installed; USB watching disabled")
            return
        await self.wait_until_ready()

        context = pyudev.Context()
        syspath_to_label: dict[str, str] = {}

        # Startup presence check
        for spec in specs:
            self.usb_present[spec.get("label", f"{spec.get('vendor_id')}:{spec.get('product_id')}")] = False
        for device in context.list_devices(subsystem="usb", DEVTYPE="usb_device"):
            vid, pid, serial = self._usb_ids(device)
            spec = self._match_usb_spec(vid, pid, serial)
            if spec:
                label = spec.get("label", f"{vid}:{pid}")
                self.usb_present[label] = True
                syspath_to_label[device.sys_path] = label
        missing = [label for label, present in self.usb_present.items() if not present]
        if missing:
            await self.send_alert(
                "SDR missing at startup",
                "Expected USB device(s) not on the bus: **" + ", ".join(missing) + "**.\n"
                "trunk-recorder cannot start without them — if it is crash-looping under "
                "systemd, this is why.", RED, critical=True)

        # Live udev events
        monitor = pyudev.Monitor.from_netlink(context)
        monitor.filter_by("usb")
        monitor.start()
        queue: asyncio.Queue = asyncio.Queue()
        loop = asyncio.get_running_loop()
        loop.add_reader(monitor.fileno(), lambda: queue.put_nowait(monitor.poll(0)))

        while not self.is_closed():
            device = await queue.get()
            if device is None or device.device_type != "usb_device":
                continue
            if device.action == "remove":
                label = syspath_to_label.pop(device.sys_path, None)
                if label is None:
                    vid, pid, serial = self._usb_ids(device)
                    spec = self._match_usb_spec(vid, pid, serial)
                    label = spec.get("label", f"{vid}:{pid}") if spec else None
                if label:
                    self.usb_present[label] = False
                    await self.send_alert(
                        f"SDR disconnected: {label}",
                        "The dongle **left the USB bus**. Recordings on its systems have stopped; "
                        "if trunk-recorder exits or restarts it will fail to start until the "
                        "device returns.", RED, critical=True)
            elif device.action in ("add", "bind"):
                vid, pid, serial = self._usb_ids(device)
                spec = self._match_usb_spec(vid, pid, serial)
                if spec:
                    label = spec.get("label", f"{vid}:{pid}")
                    syspath_to_label[device.sys_path] = label
                    if not self.usb_present.get(label, False):
                        self.usb_present[label] = True
                        await self.send_alert(
                            f"SDR connected: {label}",
                            "The dongle is back on the USB bus. trunk-recorder may need a "
                            "restart to pick it up.", GREEN)
                        if spec.get("probe"):
                            # Give udev a moment to settle, then verify the
                            # driver can actually open the returned dongle.
                            async def _probe_after_settle(s=spec, lb=label):
                                await asyncio.sleep(3)
                                status, detail = await self.probe_device(s)
                                await self.send_alert(
                                    f"Probe after reconnect: {lb}",
                                    f"{self.probe_verdict(status)}\n`{detail}`",
                                    GREEN if status in ("ok", "claimed") else RED,
                                    critical=(status == "failed"))
                            asyncio.create_task(_probe_after_settle())

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
    if not cfg["systemd"]["unit"]:
        raise SystemExit("systemd.unit must be set (the trunk-recorder unit to watch)")

    TRBot(cfg).run(token, log_handler=None)


if __name__ == "__main__":
    main()
