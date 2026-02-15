# Meshtastic LongFast Bluetooth Connector for LLM Integration

Connect an LLM to a Meshtastic mesh network via Bluetooth Low Energy (BLE).

## Installation

```bash
pip install meshtastic bleak anthropic
```

For Windows, you may also need:
```bash
pip install winrt
```

## Quick Start

### 1. Basic Connector Usage

```python
from meshtastic_connector import MeshtasticConnector, LLMInterface

# Connect via Bluetooth (auto-detect device)
connector = MeshtasticConnector(use_ble=True)
connector.connect()

# Use the LLM-friendly interface
llm = LLMInterface(connector)

# Send a message to LongFast channel
llm.send("Hello from the AI!")

# Check for received messages
print(llm.receive())

# Get mesh status
print(llm.get_status())
print(llm.get_nodes_list())

connector.disconnect()
```

### 2. Run the LLM Bridge

```bash
# With auto-detected BLE device and Claude
python llm_mesh_bridge.py

# With specific BLE address
python llm_mesh_bridge.py --ble AA:BB:CC:DD:EE:FF

# Using serial instead of BLE
python llm_mesh_bridge.py --serial COM4

# Using OpenAI instead of Anthropic
python llm_mesh_bridge.py --llm openai

# Scan for BLE devices
python llm_mesh_bridge.py --scan
```

## Configuration

### Connection Options

| Parameter | Description |
|-----------|-------------|
| `use_ble=True` | Connect via Bluetooth |
| `use_ble=False` | Connect via Serial/USB |
| `ble_address="AA:BB:CC:DD:EE:FF"` | Specific BLE device |
| `serial_port="COM4"` | Specific serial port |

### LLM Providers

Set your API key as an environment variable:

```bash
# For Claude
set ANTHROPIC_API_KEY=your-key-here

# For OpenAI
set OPENAI_API_KEY=your-key-here
```

## BLE Pairing (Windows)

1. Open Windows Settings > Bluetooth & devices
2. Turn on your Meshtastic device
3. Click "Add device" > Bluetooth
4. Select your Meshtastic device
5. Enter PIN `123456` when prompted

## Troubleshooting

### Device not found
- Ensure Bluetooth is enabled
- Pair the device first via Windows Settings
- Try running `python llm_mesh_bridge.py --scan`

### Connection fails
- Check the device is powered on
- For serial: check the COM port number in Device Manager
- For BLE: ensure device is paired and not connected to another app

### Messages not sending
- Verify you're on the LongFast channel (channel index 0)
- Check signal strength to other nodes
- Keep messages under 237 bytes

## API Reference

### MeshtasticConnector

Main connector class for low-level access.

```python
connector.connect()           # Connect to device
connector.disconnect()        # Disconnect
connector.send_message(text)  # Send to LongFast
connector.get_received_messages()  # Get received messages
connector.get_nodes()         # Get known mesh nodes
connector.get_my_info()       # Get device info
```

### LLMInterface

High-level interface designed for LLM integration.

```python
llm.send(message)      # Send message, returns status string
llm.receive()          # Get messages as formatted string
llm.get_status()       # Get connection status
llm.get_nodes_list()   # Get formatted node list
```

### MeshMessage

Data class for received messages.

```python
msg.text        # Message content
msg.from_name   # Sender's name
msg.from_id     # Sender's node ID
msg.timestamp   # When received
msg.snr         # Signal-to-noise ratio
msg.rssi        # Signal strength
```

## References

- [Meshtastic Python API](https://python.meshtastic.org/)
- [Meshtastic BLE Interface](https://python.meshtastic.org/ble_interface.html)
- [Channel Configuration](https://meshtastic.org/docs/configuration/radio/channels/)
