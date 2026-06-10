# trunk-recorder Discord watchdog bot

A small Python bot that watches a trunk-recorder instance over MQTT and reports
to Discord. It pairs with the
[tr-plugin-mqtt](https://github.com/TheGreatCodeholio/tr-plugin-mqtt) MQTT
Status plugin (fork of
[TrunkRecorder/tr-plugin-mqtt](https://github.com/TrunkRecorder/tr-plugin-mqtt)
with the `console_logs_level` option).

## What it does

| Event | How it's detected | Alert |
|---|---|---|
| trunk-recorder crashes or stops | MQTT **Last Will**: the broker itself publishes `disconnected` when the process dies, even on a hard crash | 🔴 critical |
| trunk-recorder hangs (process alive, not working) | Decode-rate telemetry (published every ~3 s) goes silent | 🟠 critical |
| SDR drops off USB / antenna / RF problem | Per-system control-channel decode rate stays below a threshold | 🟠 critical, with recovery notice |
| Upload failures to OpenMHz / Broadcastify Calls / Rdio Scanner | Error log lines matched per service, rate-limited per service | 🟠 (🔴 + ping when calls are permanently lost after retries) |
| Bot loses the MQTT broker | Connection error in the bot | 🟠 "watchdog blind" |
| SDR dongle leaves/rejoins the USB bus | **udev events on the host** (plus a presence check at startup) — independent of trunk-recorder's state | 🔴 critical, with reconnect notice |
| trunk-recorder crash-loops or fails under systemd | `systemctl show` polling (`NRestarts`, `ActiveState`) with the last journal lines attached | 🔴 critical |
| Log relay | `warning`+ console lines forwarded, batched into code blocks | posted to a logs channel |

The last two host-level watchers exist because of a startup ordering fact in
trunk-recorder: sources are created in `load_config()` **before** plugins
start, so if an SDR is missing at startup the process exits before the MQTT
plugin ever connects — nothing is published, and MQTT-only monitoring would
never see the error or the resulting systemd crash loop. The journal excerpt
in the restart alert is how the actual error (e.g. `Failed to open rtlsdr
device`) reaches Discord.

There is also a `/trstatus` slash command showing recorder state, per-system
decode rates, telemetry age, and upload-failure counters.

Limitations worth knowing:

- Decode-rate monitoring only covers **trunked** systems; conventional-only
  systems don't publish a rate. The USB watcher covers the dead-dongle case
  there; other RF problems only show up via log lines / silence of call
  activity.
- The USB and systemd watchers require the bot to run **on the same host** as
  trunk-recorder. Reading the journal needs the bot's user in the
  `systemd-journal` group (`sudo usermod -aG systemd-journal <user>`).
- A runtime dongle yank does **not** make trunk-recorder exit by default
  (`controlRetuneLimit` defaults to 0 = retry forever) — it just retunes with
  a 0 msg/s decode rate. You'll get the USB alert and the decode-rate alert;
  restarting trunk-recorder after the dongle returns is on you (or set
  `controlRetuneLimit` in trunk-recorder's config so it exits and systemd
  restarts it).
- "Offline" cannot distinguish a crash from a deliberate stop — the alert text
  says so. Whole-host failures are only caught if the broker and bot run
  somewhere that survives them (currently both run on the recorder host).

## Setup

### 1. trunk-recorder side

Make the plugin visible to trunk-recorder's build by cloning (or symlinking)
the fork into `user_plugins/` — CMake picks it up automatically; it needs
`libpaho-mqtt-dev` and `libpaho-mqttpp-dev`:

```bash
git clone -b feature/console-logs-level \
    https://github.com/TheGreatCodeholio/tr-plugin-mqtt.git
ln -s ../../tr-plugin-mqtt trunk-recorder/user_plugins/tr-plugin-mqtt
```

Rebuild trunk-recorder, then add the plugin to `config.json`:

```json
"plugins": [
  {
    "name": "MQTT Status",
    "library": "libmqtt_status_plugin.so",
    "broker": "tcp://localhost:1883",
    "topic": "trunkrecorder/feeds",
    "console_logs": true,
    "console_logs_level": "info"
  }
]
```

`console_logs_level` (added in our fork of the plugin) controls how much log
traffic goes to the broker. `info` gives the bot full context; `warning` is
enough for alerting only.

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
cp config.example.yaml config.yaml   # edit: token, channel IDs, broker, topic
venv/bin/python bot.py -c config.yaml
```

### 4. Run as a service

Edit `trunk-recorder-discord-bot.service` (user + paths), then:

```bash
sudo cp trunk-recorder-discord-bot.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now trunk-recorder-discord-bot
```

## Quick test without Discord

With trunk-recorder running, you can watch the same topics the bot uses:

```bash
mosquitto_sub -h localhost -t 'trunkrecorder/feeds/trunk_recorder/status' \
              -t 'trunkrecorder/feeds/rates' \
              -t 'trunkrecorder/feeds/trunk_recorder/console' -v
```

Kill trunk-recorder with `kill -9` and you should see the broker publish the
`disconnected` Last Will on the status topic — that's the crash detection path.
