# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**LoRaLlama** — connects an LLM (Ollama/Anthropic/OpenAI) to a Meshtastic LoRa mesh radio network. The LLM listens for incoming mesh messages and auto-responds. A separate Flask web dashboard reads the same SQLite database for monitoring.

## Running

```bash
# Bridge (main process — connects to radio + LLM, interactive CLI)
python llm_mesh_bridge.py                          # BLE + Ollama defaults
python llm_mesh_bridge.py --serial COM4 --llm anthropic
python llm_mesh_bridge.py --no-setup --no-auto     # skip wizard, listen-only
python llm_mesh_bridge.py --scan                   # scan BLE devices and exit

# Dashboard (separate process, reads shared DB)
python dashboard.py                                # http://localhost:5000

# Ollama must be running for default LLM provider
ollama serve
ollama pull llama3.2
```

Python path: use your system's Python 3.12+ (`python` or `python3`)

## Architecture

**Two independent processes share a single SQLite database (`mesh_data.db`, WAL mode):**

1. **`llm_mesh_bridge.py`** — Main runtime. Connects to Meshtastic radio via `MeshtasticConnector`, receives all packets, saves everything to DB via `MeshDatabase`, generates LLM responses via `LLMHandler`, and sends them back over the mesh. Has an interactive CLI with `/commands`. A background worker thread processes a response queue and also polls the `pending_outbox` table so the dashboard can trigger sends.

2. **`dashboard.py`** — Flask + SocketIO web UI. Single-file with inline HTML/CSS/JS template string. Reads from the shared SQLite DB. Three independent time-range filters: stats panel, messages panel, map panel. Uses polling to detect DB changes via `db_meta.last_updated`.

**Supporting modules:**
- **`meshtastic_connector.py`** — BLE/serial/TCP interface to the radio. Subscribes to all `pubsub` events (`meshtastic.receive.*`, `meshtastic.node.updated`, etc.). Provides `MeshtasticConnector` (low-level) and `LLMInterface` (high-level string I/O).
- **`mesh_database.py`** — `MeshDatabase` class. Thread-local connections, WAL mode, auto-migration for schema changes. Tables: `raw_packets`, `messages`, `nodes`, `user_facts`, `global_context`, `telemetry`, `positions`, `routing`, `neighbors`, `waypoints`, `traceroutes`, `filtered_content`, `sent_messages`, `pending_outbox`, `db_meta`.
- **`content_filter.py`** — `ContentFilter` (regex-based filtering for profanity/hate/violence/scam/PII) and `RateLimiter` (per-user message throttling).

## Key Technical Constraints

- **Meshtastic max packet: 237 bytes.** Bridge truncates responses to 200 bytes for safety. Emojis cost 4 bytes each.
- **SQLite timestamps**: Messages stored as local-time ISO strings via `datetime.now().isoformat()`. In SQL queries, use `datetime('now', 'localtime', ...)` NOT `datetime('now', ...)` (which returns UTC).
- **Node `last_heard`**: Unix timestamp (int), not ISO string. Different format from messages table.
- **Channels**: 0–7, where 0 = primary (LongFast). `send_message()` defaults to `channel_index=0` — must pass channel explicitly for multi-channel.
- **Hop count**: `hops_used = hop_start - hop_limit` from message packets; `nodes.hops_away` for network-wide view.
- **Dashboard header stats** show all-time totals, not filtered by the stats time range. Infrastructure metrics (packets, telemetry, positions) are always all-time.

## Dependencies

```
pip install meshtastic bleak requests flask flask-socketio
# Optional per LLM provider:
pip install anthropic  # or openai
```
