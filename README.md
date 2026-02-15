<p align="center">
  <img src="lorallama_logo.png" alt="LoRaLlama" width="300">
</p>

<h1 align="center">LoRaLlama</h1>

<p align="center">
  <strong>Connect an LLM to a Meshtastic LoRa mesh radio network</strong><br>
  Auto-responds to mesh messages with AI-generated replies. Real-time web dashboard included.
</p>

<p align="center">
  <img src="dashboard_screenshot.png" alt="LoRaLlama Dashboard" width="800">
</p>

<p align="center">
  <img src="https://img.shields.io/badge/python-3.12+-blue?logo=python" alt="Python 3.12+">
  <img src="https://img.shields.io/badge/meshtastic-LoRa-green?logo=data:image/svg+xml;base64," alt="Meshtastic">
  <img src="https://img.shields.io/badge/LLM-Ollama%20%7C%20Anthropic%20%7C%20OpenAI-orange" alt="LLM Providers">
  <img src="https://img.shields.io/badge/license-GPLv3-blue" alt="License">
</p>

---

## What is this?

LoRaLlama bridges an LLM to a [Meshtastic](https://meshtastic.org/) mesh radio network. Plug in a Meshtastic radio via BLE, serial, or TCP, and every incoming mesh message gets an AI-generated response sent back over the air. A web dashboard lets you monitor the network in real time.

**Key features:**
- Automatic LLM responses to mesh messages (Ollama, Anthropic Claude, or OpenAI)
- Real-time web dashboard with live map, node tracking, and message history
- Pond ripple animations on the map when nodes send messages
- Follow mode to auto-track active nodes on the map
- Content filtering and rate limiting
- Weather and web search tools the LLM can use in responses
- Per-user conversation memory and facts
- Interactive CLI with full control over the bridge
- Setup wizard for first-time configuration
- SQLite database stores all packets, telemetry, positions, and messages

## Quick Start

### 1. Install dependencies

```bash
pip install meshtastic bleak requests flask flask-socketio
# On Windows, you also need:
pip install winrt
```

For your LLM provider:
```bash
pip install anthropic   # for Claude
pip install openai      # for OpenAI
# Ollama needs no pip package — just install and run it
```

### 2. Start the bridge

```bash
# Launch both bridge + dashboard together
python launch.py

# Or run them separately:
python llm_mesh_bridge.py     # bridge (BLE + Ollama defaults)
python dashboard.py           # dashboard at http://localhost:5000
```

The first run launches a **setup wizard** that walks you through connection type, LLM provider, and system prompt.

### 3. Open the dashboard

Navigate to **http://localhost:5000** to see the live map, messages, node list, and network stats.

## Connection Options

```bash
# Bluetooth (default — auto-detects nearby devices)
python llm_mesh_bridge.py

# Specific BLE address
python llm_mesh_bridge.py --ble AA:BB:CC:DD:EE:FF

# Serial / USB
python llm_mesh_bridge.py --serial COM4          # Windows
python llm_mesh_bridge.py --serial /dev/ttyUSB0  # Linux

# TCP (remote node)
python llm_mesh_bridge.py --tcp 192.168.1.100

# Scan for BLE devices
python llm_mesh_bridge.py --scan

# Skip setup wizard
python llm_mesh_bridge.py --no-setup

# Listen only (no auto-responses)
python llm_mesh_bridge.py --no-auto
```

### Bluetooth Pairing (Windows)

To use Bluetooth on Windows, you must pair the device first:
1. Open **Windows Settings > Bluetooth & devices**.
2. Turn on your Meshtastic device.
3. Click **Add device > Bluetooth**.
4. Select your Meshtastic device from the list.
5. Enter PIN `123456` when prompted.
6. Once paired, `llm_mesh_bridge.py` will be able to connect to it.

## LLM Providers

| Provider | Setup | Notes |
|----------|-------|-------|
| **Ollama** (default) | `ollama serve` then `ollama pull llama3.2` | Free, runs locally, no API key needed |
| **Anthropic** | `export ANTHROPIC_API_KEY=your-key` | Claude models |
| **OpenAI** | `export OPENAI_API_KEY=your-key` | GPT models |
| **None** | `--llm none` | Listen-only, no responses |

## CLI Commands

Once the bridge is running, you have an interactive terminal:

| Command | Description |
|---------|-------------|
| `/send <msg>` | Send a message to the mesh |
| `/status` | Show connection status |
| `/nodes` | List known mesh nodes |
| `/auto on/off` | Enable/disable auto-respond |
| `/db` or `/stats` | Show database statistics |
| `/packets [type] [n]` | Show recent packets |
| `/user <name>` | Show detailed user profile |
| `/memory` | Show memory stats |
| `/remember <fact>` | Add global context the LLM will always know |
| `/weather` | Get current weather |
| `/search <query>` | Manual web search |
| `/quit` | Exit the bridge |

## Dashboard

The web dashboard runs on port 5000 and includes:

- **Live map** with node markers, short names, and status colors based on last-heard time
- **Pond ripple animations** — concentric rings pulse from nodes when they send messages
- **Follow mode** — auto-pans the map to active messaging nodes, restores your view after 30s
- **Route visualization** between nodes
- **Message panel** with channel filtering and DM badges
- **Node list** with search, sort, and detailed stats
- **Network tab** with telemetry, positions, traceroutes, and neighbor info
- **Stats panel** with time-range filters
- **Weather widget**

## Architecture

```mermaid
graph LR
    Radio["Meshtastic Radio<br>(LoRa mesh)"] <-->|BLE / Serial / TCP| Bridge["Bridge<br>(llm_mesh_bridge.py)"]
    Bridge --> LLM["Ollama / Claude / OpenAI"]
    Bridge <--> DB["SQLite<br>mesh_data.db"]
    DB <--> Dashboard["Dashboard (Flask)<br>:5000"]
```

Both processes share a single SQLite database (WAL mode) — the bridge writes packets and messages, the dashboard reads them for display.

## Configuration

Edit the constants at the top of `llm_mesh_bridge.py`:

```python
LLM_PROVIDER = "ollama"          # "ollama", "anthropic", "openai", "none"
OLLAMA_MODEL = "llama3.2"        # any Ollama model
WEATHER_LAT = 30.2672            # your latitude
WEATHER_LON = -97.7431           # your longitude
LOCATION = "Austin, Texas"       # your location name
TIMEZONE = "America/Chicago"     # your timezone
CONTENT_FILTER_ENABLED = True    # profanity/PII filtering
RATE_LIMIT_MESSAGES = 10         # max messages per user per minute
```

For the dashboard, optionally set:
```bash
export FLASK_SECRET_KEY=your-secret-here
```

## Technical Notes

- **Meshtastic packet limit is 237 bytes** — responses are truncated to 200 bytes for safety. Emojis cost 4 bytes each.
- **Channels 0-7** — channel 0 is the default primary (LongFast).
- The bridge includes **prompt injection protection** to prevent mesh users from manipulating the LLM's system prompt.
- **Content filtering** catches profanity, hate speech, scam attempts, and PII before the LLM processes or responds.
- The database auto-migrates schema changes between versions.

## Troubleshooting

### Device not found
- Ensure Bluetooth is enabled.
- Pair the device first via Windows Settings (see Bluetooth Pairing section).
- Try running `python llm_mesh_bridge.py --scan` to find the correct address.

### Connection fails
- Check the device is powered on and within range.
- For serial: verify the COM port number in Device Manager.
- For BLE: ensure the device is paired and not currently connected to another app (like the Meshtastic mobile app).

### Messages not sending
- Verify you're on the correct channel (default is channel 0 / LongFast).
- Check signal strength (SNR/RSSI) to other nodes in the dashboard.
- Keep messages under 237 bytes (the bridge auto-truncates to 200).

## References

- [Meshtastic Python API](https://python.meshtastic.org/)
- [Meshtastic BLE Interface](https://python.meshtastic.org/ble_interface.html)
- [Channel Configuration](https://meshtastic.org/docs/configuration/radio/channels/)

## Requirements

- Python 3.12+
- A [Meshtastic](https://meshtastic.org/) radio device (any hardware supported by the Python API)
- Bluetooth, USB, or network access to the radio
- [Ollama](https://ollama.ai/) installed locally (for default provider), or an API key for Anthropic/OpenAI

## Contributors

- **[SaltTheFields](https://github.com/SaltTheFields)** - Lead developer
- **[Gemini CLI](https://github.com/google-gemini/gemini-cli)** - AI development assistant
- **[Claude](https://anthropic.com)** - AI development assistant

## License

GNU General Public License v3.0
