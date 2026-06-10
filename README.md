# trunk-recorder Discord watchdog bot

A small Python bot that runs on the same host as
[trunk-recorder](https://github.com/TrunkRecorder/trunk-recorder) and watches
it directly — journald for logs, systemctl for unit state, udev for the SDR
dongles. No broker, no plugin, nothing to add to trunk-recorder's build.

## What it does

| Event | How it's detected | Alert |
|---|---|---|
| trunk-recorder stops, crashes, or fails to start | systemd unit watcher (`ActiveState`), with the last journal lines attached | 🔴 critical |
| Crash loop (e.g. SDR missing at startup) | `NRestarts` climbing — N restarts within a window collapse into one alert with the journal excerpt | 🔴 critical, with "stable again" notice |
| SDR dongle leaves/rejoins the USB bus | udev events on the host, plus a presence check at bot startup | 🔴 critical, with reconnect notice |
| SDR/antenna/RF trouble on a trunked system | trunk-recorder's own decode-rate log lines (logged every ~3 s while the rate is below `controlWarnRate`) | 🟠 critical, with recovery notice |
| Upload failures to OpenMHz / Broadcastify Calls / Rdio Scanner | Error log lines matched per service, rate-limited per service | 🟠 (🔴 + ping when calls are permanently lost after retries) |
| Hung process (optional) | Unit active but journal silent — requires `controlWarnRate: -1` so trunk-recorder logs its rate every ~3 s as a heartbeat | 🟠 critical |
| Log relay | `warning`+ journal lines forwarded, batched into code blocks | posted to a logs channel |

There is also a `/trstatus` slash command showing unit state, restart count,
per-system decode rates, dongle presence, and upload-failure counters.

Because the bot reads the journal directly, it sees **everything the process
ever prints** — including startup errors like `Failed to open rtlsdr device`
that occur before any plugin could have loaded, and output from gr-osmosdr/UHD
that bypasses trunk-recorder's logger entirely.

Limitations worth knowing:

- Everything here assumes trunk-recorder runs as a **systemd unit** on the
  same host as the bot. A whole-host failure (power, kernel panic) takes the
  bot down too — that needs an external watchdog.
- Decode-rate monitoring only covers **trunked** systems; conventional
  systems don't have a decode rate. The USB watcher covers the dead-dongle
  case there.
- With trunk-recorder's default `controlWarnRate` (10), healthy systems log
  no rate lines, so `/trstatus` shows the last *unhealthy* rate seen and
  recovery is inferred when the low-rate lines stop. Set
  `controlWarnRate: -1` in trunk-recorder's `config.json` for continuous
  rates and to enable hang detection.
- A runtime dongle yank does **not** make trunk-recorder exit by default
  (`controlRetuneLimit` defaults to 0 = retry forever) — it just retunes with
  a 0 msg/s decode rate. You'll get the USB alert and the decode-rate alert;
  set `controlRetuneLimit` (e.g. 6) in trunk-recorder's config so it exits,
  systemd restarts it, and it recovers by itself once the dongle returns —
  the bot narrates the whole sequence.

## Setup

### 1. trunk-recorder side

Nothing to build or install. Recommended `config.json` tweaks:

```json
{
    "controlWarnRate": -1,
    "controlRetuneLimit": 6
}
```

`controlWarnRate: -1` makes trunk-recorder log its decode rate every ~3 s
(continuous rates in `/trstatus` + enables hang detection).
`controlRetuneLimit` makes it exit when the control channel is gone for good,
so systemd can restart it.

### 2. Discord side

1. Create an application at <https://discord.com/developers/applications>,
   add a **Bot**, and copy the token.
2. Invite it with the **bot** and **applications.commands** scopes and
   *Send Messages* + *Embed Links* permissions.
3. Enable Developer Mode in Discord, right-click your alert channel (and
   optionally a logs channel) → *Copy Channel ID*.

### 3. Bot install

```bash
git clone https://github.com/TheGreatCodeholio/tr-discord-bot.git
cd tr-discord-bot
python3 -m venv venv
venv/bin/pip install -r requirements.txt
cp config.example.yaml config.yaml   # edit: token, channel IDs, unit, SDRs
venv/bin/python bot.py -c config.yaml
```

The bot's user must be able to read the unit's journal:

```bash
sudo usermod -aG systemd-journal <bot-user>
```

### 4. Run as a service

Edit `trunk-recorder-discord-bot.service` (user + paths), then:

```bash
sudo cp trunk-recorder-discord-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now trunk-recorder-discord-bot
```

## Quick test without Discord

Watch what the bot would see:

```bash
journalctl -u trunk-recorder -f -o json | jq -r .MESSAGE
```

Kill trunk-recorder with `kill -9` — systemd restarts it, `NRestarts`
increments, and the bot posts the restart alert with the journal excerpt.
Unplug a configured dongle and the USB alert fires immediately.
