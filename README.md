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
| A system stops recording (the health signal for **conventional** systems) | Per-system activity watchdog: no `Concluding Recorded Call` log line for longer than a configurable quiet window | 🟠 (critical optional), with "recording again" notice |
| Upload failures to OpenMHz / Broadcastify Calls / Rdio Scanner | Error log lines matched per service, rate-limited per service | 🟠 (🔴 + ping when calls are permanently lost after retries) |
| Hung process (optional) | Unit active but journal silent — requires `controlWarnRate: -1` so trunk-recorder logs its rate every ~3 s as a heartbeat | 🟠 critical |
| Log relay | `warning`+ journal lines forwarded, batched into code blocks | posted to a logs channel |

Slash commands:

- `/trstatus` — unit state, restart count, per-system decode rates, dongle
  presence, upload-failure counters.
- `/trprobe` — actively probe the dongles at the driver level (see below).
- `/trrestart confirm: True` — restart the trunk-recorder service. Restricted
  to members with **Manage Server** by default (adjust per-command in Server
  Settings → Integrations), requires the explicit `confirm` flag since it
  stops in-progress recordings, posts an audit alert naming the requester,
  and reports the resulting unit state (with a journal excerpt on failure).
  Needs a sudoers rule on the host — see setup below.
- `/trclear target: logs|alerts|both confirm: True` — purge the bot's
  messages from the chosen channel(s) (`all_messages: True` to delete other
  users' messages too; pinned messages are always kept). Restricted to
  members with **Manage Messages** by default. There is also an automatic
  retention policy for the logs channel (`logs.retention_hours`). Both need
  the bot to have **Manage Messages** and **Read Message History** in the
  channel. Discord only bulk-deletes messages younger than 14 days; older
  backlog is deleted one-by-one, capped at 500 per run — re-run to continue.

### Dongle health is three layers

1. **On the bus** (udev) — physical connectivity only. A dongle can be
   present and still unusable.
2. **Openable by the driver** — an active probe (`rtl_test -d <serial> -t`,
   `airspy_info`, `hackrf_info`, `SoapySDRUtil --probe=...`, configurable per
   device). The bot classifies the probe *output*, not its exit code —
   `rtl_test` exits 0 even when it fails to open the device. A result of
   **claimed** (another process holds the dongle) is healthy while
   trunk-recorder is running and a red flag while it isn't (some other
   program is squatting on the SDR and trunk-recorder will fail to start).
3. **Producing data** — the decode-rate monitoring. While trunk-recorder is
   recording, this *is* the driver test; an SDR can only be opened by one
   process, so the bot deliberately never probes on a schedule. Probes run
   only on demand (`/trprobe`), automatically when a crash loop is detected
   (to tell hardware/driver trouble from a config problem), and ~3 s after a
   dongle returns to the bus (to confirm it actually came back usable).

Because the bot reads the journal directly, it sees **everything the process
ever prints** — including startup errors like `Failed to open rtlsdr device`
that occur before any plugin could have loaded, and output from gr-osmosdr/UHD
that bypasses trunk-recorder's logger entirely.

Limitations worth knowing:

- Everything here assumes trunk-recorder runs as a **systemd unit** on the
  same host as the bot. A whole-host failure (power, kernel panic) takes the
  bot down too — that needs an external watchdog.
- **Conventional systems (analog or digital) have no decode rate at all**, so
  decode-rate monitoring and the `controlWarnRate: -1` heartbeat only apply
  to trunked systems — and log-silence hang detection needs at least one
  trunked system to be meaningful. For conventional systems the health
  layers are: the USB watcher (dead dongle), the systemd watcher (dead
  process), and the **activity watchdog** ("this channel normally records
  something every N hours"). A quiet-but-healthy channel and a deaf SDR look
  identical in-band, so pick `max_quiet_hours` generously for low-traffic
  channels.
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
   *Send Messages*, *Embed Links*, *Read Message History*, and
   *Manage Messages* permissions (the last two are for `/trclear` and log
   retention; add *Mention @everyone* if you use `mention_on_critical`).
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

For `/trrestart`, allow the bot's user to restart exactly that one unit and
nothing else:

```bash
echo '<bot-user> ALL=(root) NOPASSWD: /usr/bin/systemctl restart trunk-recorder.service' \
  | sudo tee /etc/sudoers.d/tr-discord-bot
sudo chmod 0440 /etc/sudoers.d/tr-discord-bot
```

(Check the path with `which systemctl`; without this rule the command fails
cleanly and the bot's reply includes the rule to add.)

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
