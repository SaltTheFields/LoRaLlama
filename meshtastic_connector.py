"""
Meshtastic LongFast Bluetooth Connector for LLM Integration

This module provides a connector that allows an LLM to communicate
over Meshtastic mesh network via Bluetooth Low Energy (BLE).

Requirements:
    pip install meshtastic bleak

Usage:
    from meshtastic_connector import MeshtasticConnector

    connector = MeshtasticConnector()
    connector.connect()
    connector.send_message("Hello mesh!")
    messages = connector.get_received_messages()
    connector.disconnect()
"""

import threading
import queue
import time
import json
import traceback
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable
from dataclasses import dataclass, asdict
import logging

# Setup verbose logging
logger = logging.getLogger(__name__)

try:
    import meshtastic
    import meshtastic.ble_interface
    import meshtastic.serial_interface
    from pubsub import pub
except ImportError as e:
    print("Required packages not installed. Run:")
    print("  pip install meshtastic bleak")
    raise e


@dataclass
class MeshMessage:
    """Represents a message received from the mesh network."""
    text: str
    from_id: str
    from_name: str
    to_id: str
    channel: int
    timestamp: datetime
    snr: float = 0.0
    rssi: int = 0
    hop_limit: int = 0
    hop_start: int = 0
    packet_id: int = 0
    raw_packet: Optional[Dict] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        d['timestamp'] = self.timestamp.isoformat()
        return d

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=2)


class MeshtasticConnector:
    """
    Connector for Meshtastic devices via Bluetooth or Serial.

    Provides a simple interface for LLMs to send and receive messages
    over the Meshtastic mesh network.
    """

    def __init__(
        self,
        ble_address: Optional[str] = None,
        serial_port: Optional[str] = None,
        use_ble: bool = True,
        message_callback: Optional[Callable[[MeshMessage], None]] = None,
        packet_callback: Optional[Callable[[Dict, str], None]] = None,
        node_callback: Optional[Callable[[Dict], None]] = None
    ):
        """
        Initialize the Meshtastic connector.

        Args:
            ble_address: Bluetooth address of the device (e.g., "AA:BB:CC:DD:EE:FF")
                        If None, will scan for available devices.
            serial_port: Serial port (e.g., "COM4" on Windows, "/dev/ttyUSB0" on Linux)
                        Used if use_ble is False.
            use_ble: If True, connect via Bluetooth. If False, use serial.
            message_callback: Optional callback function called when text messages are received.
            packet_callback: Optional callback for ALL packets (type, data).
            node_callback: Optional callback for node updates.
        """
        self.ble_address = ble_address
        self.serial_port = serial_port
        self.use_ble = use_ble
        self.message_callback = message_callback
        self.packet_callback = packet_callback
        self.node_callback = node_callback

        self.interface = None
        self.connected = False
        self.received_messages: queue.Queue[MeshMessage] = queue.Queue()
        self.node_info: Dict[str, Any] = {}
        self._lock = threading.Lock()

        # Stats for logging
        self.stats = {
            'packets_received': 0,
            'text_messages': 0,
            'position_updates': 0,
            'telemetry_updates': 0,
            'node_updates': 0,
            'messages_sent': 0,
            'send_failures': 0,
            'waypoint_updates': 0,
            'traceroute_updates': 0,
            'store_forward_updates': 0,
            'range_test_updates': 0,
            'detection_alerts': 0,
            'paxcounter_updates': 0
        }

        # Subscribe to ALL Meshtastic events
        logger.info("[INIT] Subscribing to Meshtastic events...")
        pub.subscribe(self._on_receive, "meshtastic.receive")
        pub.subscribe(self._on_connection, "meshtastic.connection.established")
        pub.subscribe(self._on_disconnect, "meshtastic.connection.lost")
        pub.subscribe(self._on_node_update, "meshtastic.node.updated")

        # Additional event subscriptions for comprehensive logging
        try:
            pub.subscribe(self._on_receive_text, "meshtastic.receive.text")
            pub.subscribe(self._on_receive_position, "meshtastic.receive.position")
            pub.subscribe(self._on_receive_telemetry, "meshtastic.receive.telemetry")
            pub.subscribe(self._on_receive_user, "meshtastic.receive.user")
            pub.subscribe(self._on_receive_routing, "meshtastic.receive.routing")
            pub.subscribe(self._on_receive_data, "meshtastic.receive.data")
            logger.info("[INIT] Subscribed to all packet type events")
        except Exception as e:
            logger.warning(f"[INIT] Some event subscriptions failed (may not be available): {e}")

        # Additional packet type subscriptions
        try:
            pub.subscribe(self._on_receive_waypoint, "meshtastic.receive.waypoint")
            pub.subscribe(self._on_receive_traceroute, "meshtastic.receive.traceroute")
            pub.subscribe(self._on_receive_storeforward, "meshtastic.receive.storeforward")
            pub.subscribe(self._on_receive_rangetest, "meshtastic.receive.rangetest")
            pub.subscribe(self._on_receive_detection, "meshtastic.receive.detection")
            pub.subscribe(self._on_receive_paxcounter, "meshtastic.receive.paxcounter")
            pub.subscribe(self._on_receive_mapreport, "meshtastic.receive.mapreport")
            logger.info("[INIT] Subscribed to extended packet type events")
        except Exception as e:
            logger.warning(f"[INIT] Extended event subscriptions failed: {e}")

    def scan_ble_devices(self, timeout: float = 10.0) -> List[Dict[str, str]]:
        """
        Scan for available Meshtastic devices over BLE.

        Args:
            timeout: How long to scan in seconds.

        Returns:
            List of dictionaries with 'address' and 'name' keys.
        """
        logger.info(f"[BLE] Starting scan for {timeout}s...")
        print(f"Scanning for Meshtastic BLE devices ({timeout}s)...")

        try:
            import asyncio
            from bleak import BleakScanner

            async def scan():
                # Meshtastic devices advertise with specific service UUID
                MESHTASTIC_SERVICE_UUID = "6ba1b218-15a8-461f-9fa8-5dcae273eafd"

                devices = await BleakScanner.discover(timeout=timeout)

                meshtastic_devices = []
                for d in devices:
                    # Check if it's a Meshtastic device by name or service UUID
                    name = d.name or ""
                    is_meshtastic = (
                        "meshtastic" in name.lower() or
                        "mesh" in name.lower() or
                        "t-beam" in name.lower() or
                        "tbeam" in name.lower() or
                        "heltec" in name.lower() or
                        "lora" in name.lower() or
                        "rak" in name.lower()
                    )

                    # Also check advertised service UUIDs if available
                    if hasattr(d, 'metadata') and d.metadata:
                        uuids = d.metadata.get('uuids', [])
                        if MESHTASTIC_SERVICE_UUID in [str(u).lower() for u in uuids]:
                            is_meshtastic = True

                    if is_meshtastic or not name:  # Include unnamed devices too
                        meshtastic_devices.append({
                            "address": d.address,
                            "name": name or "Unknown Device"
                        })

                return meshtastic_devices

            # Handle Windows event loop issues
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = None

            if loop and loop.is_running():
                # We're in an async context, create a new thread
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(asyncio.run, scan())
                    devices = future.result()
            else:
                devices = asyncio.run(scan())

            if devices:
                logger.info(f"[BLE] Found {len(devices)} Meshtastic device(s)")
                print(f"Found {len(devices)} potential Meshtastic device(s):")
                for d in devices:
                    print(f"  - {d['name']}: {d['address']}")
            else:
                logger.warning("[BLE] No Meshtastic devices found, showing all BLE devices")
                print("No devices found. Showing all BLE devices...")
                # Fallback: show all devices
                async def scan_all():
                    all_devices = await BleakScanner.discover(timeout=timeout)
                    return [{"address": d.address, "name": d.name or "Unknown"} for d in all_devices]

                all_devs = asyncio.run(scan_all())
                print(f"Found {len(all_devs)} BLE device(s):")
                for d in all_devs:
                    print(f"  - {d['name']}: {d['address']}")
                return all_devs

            return devices

        except Exception as e:
            logger.error(f"[BLE] Scan failed: {e}")
            print(f"BLE scan failed: {e}")
            print("Tip: Make sure Bluetooth is enabled and you have proper permissions.")
            return []

    def connect(self, timeout: float = 30.0) -> bool:
        """
        Connect to a Meshtastic device.

        Args:
            timeout: Connection timeout in seconds.

        Returns:
            True if connection successful, False otherwise.
        """
        if self.connected:
            logger.info("[CONN] Already connected")
            print("Already connected")
            return True

        try:
            if self.use_ble:
                logger.info("[CONN] Connecting via Bluetooth...")
                print("Connecting via Bluetooth...")
                address = self.ble_address

                # If no address provided, scan for devices
                if not address:
                    logger.info("[CONN] No BLE address specified, scanning...")
                    print("No BLE address specified, scanning for devices...")
                    devices = self.scan_ble_devices(timeout=10.0)

                    if not devices:
                        logger.error("[CONN] No devices found")
                        print("No Meshtastic devices found via BLE scan.")
                        print("Try specifying the address manually with --ble <address>")
                        print("Or use serial connection with --serial <port>")
                        return False

                    if len(devices) == 1:
                        address = devices[0]['address']
                        logger.info(f"[CONN] Auto-selecting: {devices[0]['name']} ({address})")
                        print(f"Auto-selecting: {devices[0]['name']} ({address})")
                    else:
                        print("\nMultiple devices found. Please select one:")
                        for i, d in enumerate(devices, 1):
                            print(f"  {i}. {d['name']} ({d['address']})")
                        try:
                            choice = input("Enter number (or press Enter for first): ").strip()
                            idx = int(choice) - 1 if choice else 0
                            if 0 <= idx < len(devices):
                                address = devices[idx]['address']
                            else:
                                address = devices[0]['address']
                        except (ValueError, IndexError):
                            address = devices[0]['address']
                        logger.info(f"[CONN] User selected: {address}")
                        print(f"Selected: {address}")

                logger.info(f"[CONN] Attempting BLE connection to {address}...")
                self.interface = meshtastic.ble_interface.BLEInterface(address=address)
            else:
                logger.info("[CONN] Connecting via Serial...")
                print("Connecting via Serial...")
                if self.serial_port:
                    self.interface = meshtastic.serial_interface.SerialInterface(
                        devPath=self.serial_port
                    )
                else:
                    # Auto-detect serial port
                    self.interface = meshtastic.serial_interface.SerialInterface()

            # Wait for connection
            logger.info(f"[CONN] Waiting for connection (timeout={timeout}s)...")
            start = time.time()
            while not self.connected and (time.time() - start) < timeout:
                time.sleep(0.5)

            if self.connected:
                self._load_node_info()
                device_name = self.node_info.get('user', {}).get('longName', 'Unknown')
                logger.info(f"[CONN] SUCCESS - Connected to: {device_name}")
                print(f"Connected to: {device_name}")

                # Log initial node info
                self._log_all_nodes()
                return True
            else:
                logger.error("[CONN] Connection timed out")
                print("Connection timed out")
                return False

        except Exception as e:
            logger.error(f"[CONN] Connection failed: {e}")
            logger.error(traceback.format_exc())
            print(f"Connection failed: {e}")
            return False

    def _log_all_nodes(self):
        """Log all known nodes on connection."""
        nodes = dict(self.get_nodes())
        logger.info(f"[NODES] Found {len(nodes)} nodes in mesh")
        for node_id, node_data in nodes.items():
            user = node_data.get('user', {})
            name = user.get('longName', 'Unknown')
            short = user.get('shortName', '????')
            hw = user.get('hwModel', 'Unknown')
            logger.info(f"[NODES]   {name} ({short}) - {node_id} - HW: {hw}")

            # Notify callback
            if self.node_callback:
                self.node_callback(node_data)

    def disconnect(self):
        """Disconnect from the Meshtastic device."""
        logger.info("[CONN] Disconnecting...")
        if self.interface:
            try:
                self.interface.close()
            except Exception as e:
                logger.error(f"[CONN] Error during disconnect: {e}")
                print(f"Error during disconnect: {e}")
            finally:
                self.interface = None
                self.connected = False
                logger.info("[CONN] Disconnected")
                print("Disconnected")

        # Log final stats
        logger.info(f"[STATS] Final stats: {json.dumps(self.stats)}")

    def send_message(
        self,
        text: str,
        destination: str = "^all",
        channel_index: int = 0,
        want_ack: bool = False
    ) -> bool:
        """
        Send a text message over the mesh network.

        Args:
            text: The message text to send (max 237 bytes for LongFast).
            destination: Destination node ID or "^all" for broadcast.
            channel_index: Channel index (0 = primary/LongFast).
            want_ack: Whether to request acknowledgment.

        Returns:
            True if message was sent successfully.
        """
        logger.info("[TX] Attempting to send message...")
        logger.info(f"[TX]   Text: {text[:100]}{'...' if len(text) > 100 else ''}")
        logger.info(f"[TX]   Destination: {destination}")
        logger.info(f"[TX]   Channel: {channel_index}")
        logger.info(f"[TX]   Want ACK: {want_ack}")

        if not self.connected or not self.interface:
            logger.error("[TX] FAILED - Not connected to a device")
            print("Not connected to a device")
            self.stats['send_failures'] += 1
            return False

        # Meshtastic message size limit
        max_size = 237
        original_len = len(text.encode('utf-8'))
        if original_len > max_size:
            logger.warning(f"[TX] Message too long ({original_len} bytes), truncating to {max_size}")
            print(f"Warning: Message exceeds {max_size} bytes, will be truncated")
            text = text.encode('utf-8')[:max_size].decode('utf-8', errors='ignore')

        try:
            logger.info("[TX] Calling interface.sendText()...")
            result = self.interface.sendText(
                text=text,
                destinationId=destination,
                channelIndex=channel_index,
                wantAck=want_ack
            )
            logger.info(f"[TX] sendText() returned: {result}")

            self.stats['messages_sent'] += 1
            logger.info(f"[TX] SUCCESS - Message sent (total sent: {self.stats['messages_sent']})")
            print(f"Sent: {text[:50]}{'...' if len(text) > 50 else ''}")
            return True

        except Exception as e:
            self.stats['send_failures'] += 1
            logger.error(f"[TX] FAILED - Exception: {e}")
            logger.error(traceback.format_exc())
            print(f"Failed to send message: {e}")
            return False

    def send_dm(
        self,
        text: str,
        destination: str,
        want_ack: bool = False
    ) -> bool:
        """
        Send a PKC-encrypted direct message to a specific node.

        Uses sendData() with pkiEncrypted=True and the recipient's public key
        so the message appears in the DM thread on the Meshtastic app (not in
        the LongFast channel chat).

        Falls back to a directed (non-encrypted) sendText() if recipient has
        no public key (older firmware without PKC support).

        Args:
            text: The message text to send.
            destination: Destination node ID (e.g., "!aabbccdd").
            want_ack: Whether to request acknowledgment.

        Returns:
            True if message was sent successfully.
        """
        logger.info(f"[TX-DM] Sending DM to {destination}...")
        logger.info(f"[TX-DM]   Text: {text[:100]}{'...' if len(text) > 100 else ''}")

        if not self.connected or not self.interface:
            logger.error("[TX-DM] FAILED - Not connected to a device")
            self.stats['send_failures'] += 1
            return False

        # Meshtastic message size limit
        max_size = 237
        original_len = len(text.encode('utf-8'))
        if original_len > max_size:
            logger.warning(f"[TX-DM] Message too long ({original_len} bytes), truncating to {max_size}")
            text = text.encode('utf-8')[:max_size].decode('utf-8', errors='ignore')

        try:
            # Look up recipient's public key from node info
            nodes = self.get_nodes()
            recipient_node = nodes.get(destination, {})
            public_key_b64 = recipient_node.get('user', {}).get('publicKey')

            if public_key_b64:
                import base64
                from meshtastic import portnums_pb2

                # Decode base64 public key to bytes
                if isinstance(public_key_b64, str):
                    public_key_bytes = base64.b64decode(public_key_b64)
                else:
                    public_key_bytes = public_key_b64  # Already bytes

                logger.info(f"[TX-DM] Using PKC encryption (key len={len(public_key_bytes)})")
                result = self.interface.sendData(
                    text.encode('utf-8'),
                    destinationId=destination,
                    portNum=portnums_pb2.PortNum.TEXT_MESSAGE_APP,
                    wantAck=want_ack,
                    pkiEncrypted=True,
                    publicKey=public_key_bytes
                )
                logger.info(f"[TX-DM] PKC-encrypted DM sent: {result}")
            else:
                # Fallback: directed message without PKC (older firmware)
                logger.warning(f"[TX-DM] No public key for {destination}, falling back to directed sendText")
                result = self.interface.sendText(
                    text=text,
                    destinationId=destination,
                    wantAck=want_ack
                )
                logger.info(f"[TX-DM] Directed (non-PKC) message sent: {result}")

            self.stats['messages_sent'] += 1
            logger.info(f"[TX-DM] SUCCESS - DM sent to {destination} (total: {self.stats['messages_sent']})")
            print(f"DM sent to {destination}: {text[:50]}{'...' if len(text) > 50 else ''}")
            return True

        except Exception as e:
            self.stats['send_failures'] += 1
            logger.error(f"[TX-DM] FAILED - Exception: {e}")
            logger.error(traceback.format_exc())
            print(f"Failed to send DM: {e}")
            return False

    def send_traceroute(self, destination: str, hop_limit: int = 7) -> bool:
        """Request a traceroute to the specified node (non-blocking).

        Replicates the library's sendTraceRoute() but skips the blocking
        waitForTraceRoute() call. Registers an onResponse callback so the
        response is processed whether or not the pubsub event fires.

        Args:
            destination: Destination node ID (e.g., "!aabbccdd").
            hop_limit: Maximum hops for the traceroute (default 7).

        Returns:
            True if the request was sent successfully.
        """
        if not self.connected or not self.interface:
            logger.error("[TX-TR] FAILED - Not connected to a device")
            return False
        try:
            from meshtastic import portnums_pb2, mesh_pb2
            import google.protobuf.json_format

            # Build an empty RouteDiscovery protobuf (same as library's sendTraceRoute)
            r = mesh_pb2.RouteDiscovery()

            # Track handled packet IDs to avoid duplicate saves from pubsub
            if not hasattr(self, '_handled_traceroutes'):
                self._handled_traceroutes = set()

            # Capture our node ID and destination for the closure
            my_id = self.node_info.get('user', {}).get('id') or self.node_info.get('num')
            trace_dest = destination

            # Callback to handle the traceroute response
            connector_ref = self
            def _on_traceroute_response(packet):
                try:
                    decoded = packet.get('decoded', {})
                    # Parse traceroute data from payload if not already parsed
                    if 'traceroute' not in decoded and 'payload' in decoded:
                        rd = mesh_pb2.RouteDiscovery()
                        rd.ParseFromString(decoded['payload'])
                        decoded['traceroute'] = google.protobuf.json_format.MessageToDict(rd)

                    # Fix from/to: we initiated the traceroute, destination responded
                    if my_id:
                        packet['fromId'] = my_id
                    packet['toId'] = trace_dest

                    logger.info(f"[RX-TR] Traceroute {my_id} → {trace_dest}: {decoded.get('traceroute', {})}")

                    # Mark as handled so pubsub handler skips it
                    pkt_id = packet.get('id')
                    if pkt_id:
                        connector_ref._handled_traceroutes.add(pkt_id)
                        # Keep set bounded
                        if len(connector_ref._handled_traceroutes) > 100:
                            connector_ref._handled_traceroutes = set(list(connector_ref._handled_traceroutes)[-50:])

                    # Feed into our existing handler chain
                    if connector_ref.packet_callback:
                        connector_ref.packet_callback(packet, 'TRACEROUTE_APP')
                except Exception as e:
                    logger.error(f"[RX-TR] Traceroute response callback error: {e}")

            # Send via sendData with onResponse callback — returns immediately
            self.interface.sendData(
                r,
                destinationId=destination,
                portNum=portnums_pb2.PortNum.TRACEROUTE_APP,
                wantResponse=True,
                onResponse=_on_traceroute_response,
                channelIndex=0,
                hopLimit=hop_limit,
            )
            logger.info(f"[TX-TR] Traceroute packet sent to {destination} (hop_limit={hop_limit})")
            return True
        except Exception as e:
            logger.error(f"[TX-TR] Traceroute send failed: {e}")
            logger.error(traceback.format_exc())
            return False

    def get_received_messages(self, max_count: int = 100) -> List[MeshMessage]:
        """
        Get all received messages from the queue.

        Args:
            max_count: Maximum number of messages to return.

        Returns:
            List of MeshMessage objects.
        """
        messages = []
        while len(messages) < max_count:
            try:
                msg = self.received_messages.get_nowait()
                messages.append(msg)
            except queue.Empty:
                break
        return messages

    def get_nodes(self) -> Dict[str, Any]:
        """
        Get information about all known nodes in the mesh.

        Returns:
            Dictionary of node information keyed by node ID.
        """
        if not self.interface:
            return {}

        try:
            return self.interface.nodes or {}
        except Exception:
            return {}

    def get_my_info(self) -> Dict[str, Any]:
        """Get information about the connected device."""
        return self.node_info

    def _load_node_info(self):
        """Load node information from the connected device."""
        if self.interface:
            try:
                my_info = self.interface.getMyNodeInfo()
                if my_info:
                    self.node_info = my_info
                    logger.info(f"[SELF] My node info loaded: {json.dumps(my_info.get('user', {}), default=str)}")
            except Exception as e:
                logger.error(f"[SELF] Failed to load node info: {e}")
                print(f"Failed to load node info: {e}")

    # Portnums that have dedicated handlers — skip in _on_receive to avoid double-saves
    _DEDICATED_PORTNUMS = {
        'POSITION_APP', 'TELEMETRY_APP', 'ROUTING_APP', 'NODEINFO_APP',
        'TRACEROUTE_APP', 'WAYPOINT_APP', 'STORE_FORWARD_APP',
        'RANGE_TEST_APP', 'DETECTION_SENSOR_APP', 'PAXCOUNTER_APP',
        'MAP_REPORT_APP',
    }

    def _on_receive(self, packet, interface):
        """Handle ALL received packets."""
        self.stats['packets_received'] += 1

        try:
            # Log every packet
            from_id = packet.get('fromId', 'unknown')
            to_id = packet.get('toId', 'unknown')

            decoded = packet.get('decoded', {})
            portnum = decoded.get('portnum', 'UNKNOWN')

            logger.debug(f"[RX-RAW] Packet #{self.stats['packets_received']} from {from_id} -> {to_id} | port: {portnum}")

            # Skip portnums that have dedicated handlers to avoid double-processing
            if portnum in self._DEDICATED_PORTNUMS:
                logger.debug(f"[RX-RAW] Skipping {portnum} — handled by dedicated handler")
            elif self.packet_callback:
                self.packet_callback(packet, portnum)

            # Only process text messages for the message queue
            if 'decoded' in packet and 'text' in decoded:
                self._process_text_message(packet, decoded)

        except Exception as e:
            logger.error(f"[RX-RAW] Error processing packet: {e}")
            logger.error(traceback.format_exc())

    def _process_text_message(self, packet, decoded):
        """Process a text message packet."""
        self.stats['text_messages'] += 1

        # Get sender info - fall back to raw numeric 'from' field if fromId missing
        from_id = packet.get('fromId')
        if not from_id and 'from' in packet:
            from_id = f"!{packet['from']:08x}"
        from_id = from_id or 'unknown'
        from_node = self.get_nodes().get(from_id, {})
        from_name = from_node.get('user', {}).get('longName', from_id)

        msg = MeshMessage(
            text=decoded.get('text', ''),
            from_id=from_id,
            from_name=from_name,
            to_id=packet.get('toId', 'unknown'),
            channel=packet.get('channel', 0),
            timestamp=datetime.now(),
            snr=packet.get('rxSnr', 0.0),
            rssi=packet.get('rxRssi', 0),
            hop_limit=packet.get('hopLimit', 0),
            hop_start=packet.get('hopStart', 0),
            packet_id=packet.get('id', 0),
            raw_packet=packet
        )

        self.received_messages.put(msg)

        # Detailed logging
        logger.info(f"[RX-TEXT] Message #{self.stats['text_messages']}")
        logger.info(f"[RX-TEXT]   From: {from_name} ({from_id})")
        logger.info(f"[RX-TEXT]   To: {msg.to_id}")
        logger.info(f"[RX-TEXT]   Channel: {msg.channel}")
        logger.info(f"[RX-TEXT]   Text: {msg.text}")
        logger.info(f"[RX-TEXT]   SNR: {msg.snr} dB | RSSI: {msg.rssi} dBm")
        logger.info(f"[RX-TEXT]   Hops: {msg.hop_limit}/{msg.hop_start}")

        print(f"Received from {from_name}: {msg.text[:50]}{'...' if len(msg.text) > 50 else ''}")

        # Call the callback if set
        if self.message_callback:
            self.message_callback(msg)

    def _on_receive_text(self, packet, interface):
        """Handle text message events specifically."""
        # Already handled in _on_receive, but log for debugging
        logger.debug("[EVENT] meshtastic.receive.text triggered")

    def _on_receive_position(self, packet, interface):
        """Handle position update events."""
        self.stats['position_updates'] += 1
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            position = decoded.get('position', {})

            lat = position.get('latitude', position.get('latitudeI', 0) / 1e7 if 'latitudeI' in position else None)
            lon = position.get('longitude', position.get('longitudeI', 0) / 1e7 if 'longitudeI' in position else None)
            alt = position.get('altitude')

            logger.info(f"[RX-POS] Position from {from_id}: lat={lat}, lon={lon}, alt={alt}")

            if self.packet_callback:
                self.packet_callback(packet, 'POSITION_APP')

        except Exception as e:
            logger.error(f"[RX-POS] Error: {e}")

    def _on_receive_telemetry(self, packet, interface):
        """Handle telemetry events."""
        self.stats['telemetry_updates'] += 1
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            telemetry = decoded.get('telemetry', {})

            device_metrics = telemetry.get('deviceMetrics', {})
            env_metrics = telemetry.get('environmentMetrics', {})

            logger.info(f"[RX-TEL] Telemetry from {from_id}:")
            if device_metrics:
                logger.info(f"[RX-TEL]   Battery: {device_metrics.get('batteryLevel')}%")
                logger.info(f"[RX-TEL]   Voltage: {device_metrics.get('voltage')}V")
                logger.info(f"[RX-TEL]   Ch Util: {device_metrics.get('channelUtilization')}%")
                logger.info(f"[RX-TEL]   Air TX: {device_metrics.get('airUtilTx')}%")
            if env_metrics:
                logger.info(f"[RX-TEL]   Temp: {env_metrics.get('temperature')}C")
                logger.info(f"[RX-TEL]   Humidity: {env_metrics.get('relativeHumidity')}%")

            if self.packet_callback:
                self.packet_callback(packet, 'TELEMETRY_APP')

        except Exception as e:
            logger.error(f"[RX-TEL] Error: {e}")

    def _on_receive_user(self, packet, interface):
        """Handle user info events."""
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            user = decoded.get('user', {})

            logger.info(f"[RX-USER] User info from {from_id}:")
            logger.info(f"[RX-USER]   Long name: {user.get('longName')}")
            logger.info(f"[RX-USER]   Short name: {user.get('shortName')}")
            logger.info(f"[RX-USER]   HW Model: {user.get('hwModel')}")
            logger.info(f"[RX-USER]   MAC: {user.get('macaddr')}")

            if self.packet_callback:
                self.packet_callback(packet, 'NODEINFO_APP')

        except Exception as e:
            logger.error(f"[RX-USER] Error: {e}")

    def _on_receive_routing(self, packet, interface):
        """Handle routing events."""
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            routing = decoded.get('routing', {})

            error = routing.get('errorReason')
            if error:
                logger.warning(f"[RX-ROUTE] Routing error from {from_id}: {error}")
            else:
                logger.debug(f"[RX-ROUTE] Routing update from {from_id}")

            if self.packet_callback:
                self.packet_callback(packet, 'ROUTING_APP')

        except Exception as e:
            logger.error(f"[RX-ROUTE] Error: {e}")

    def _on_receive_data(self, packet, interface):
        """Handle generic data packets (fallback for portnums without dedicated handlers)."""
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            portnum = decoded.get('portnum', 'UNKNOWN')

            logger.debug(f"[RX-DATA] Data packet from {from_id}, port: {portnum}")

            # Skip portnums already handled by dedicated handlers
            if portnum in self._DEDICATED_PORTNUMS:
                return

            if self.packet_callback:
                self.packet_callback(packet, portnum)

        except Exception as e:
            logger.error(f"[RX-DATA] Error: {e}")

    def _on_receive_waypoint(self, packet, interface):
        """Handle waypoint events."""
        self.stats['waypoint_updates'] += 1
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            waypoint = decoded.get('waypoint', {})

            name = waypoint.get('name', 'unnamed')
            lat = waypoint.get('latitudeI', 0) / 1e7 if 'latitudeI' in waypoint else waypoint.get('latitude')
            lon = waypoint.get('longitudeI', 0) / 1e7 if 'longitudeI' in waypoint else waypoint.get('longitude')
            expire = waypoint.get('expire')

            logger.info(f"[RX-WPT] Waypoint from {from_id}: {name} ({lat}, {lon}), expire={expire}")

            if self.packet_callback:
                self.packet_callback(packet, 'WAYPOINT_APP')

        except Exception as e:
            logger.error(f"[RX-WPT] Error: {e}")

    def _on_receive_traceroute(self, packet, interface):
        """Handle traceroute events from pubsub."""
        self.stats['traceroute_updates'] += 1
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            traceroute = decoded.get('traceroute', {})

            route = traceroute.get('route', [])
            snr_towards = traceroute.get('snrTowards', [])

            # Skip if already processed by onResponse callback (avoid duplicate saves)
            pkt_id = packet.get('id')
            if pkt_id and hasattr(self, '_handled_traceroutes') and pkt_id in self._handled_traceroutes:
                logger.debug(f"[RX-TR] Skipping duplicate traceroute {pkt_id} (already handled by callback)")
                return

            logger.info(f"[RX-TR] Traceroute from {from_id}: route={route}, snr_towards={snr_towards}")

            if self.packet_callback:
                self.packet_callback(packet, 'TRACEROUTE_APP')

        except Exception as e:
            logger.error(f"[RX-TR] Error: {e}")

    def _on_receive_storeforward(self, packet, interface):
        """Handle store & forward events."""
        self.stats['store_forward_updates'] += 1
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            sf = decoded.get('storeAndForward', decoded)

            sf_type = 'unknown'
            if sf.get('stats'):
                sf_type = 'stats'
            elif sf.get('heartbeat'):
                sf_type = 'heartbeat'
            elif sf.get('history'):
                sf_type = 'history'

            logger.info(f"[RX-SF] Store&Forward from {from_id}: type={sf_type}")

            if self.packet_callback:
                self.packet_callback(packet, 'STORE_FORWARD_APP')

        except Exception as e:
            logger.error(f"[RX-SF] Error: {e}")

    def _on_receive_rangetest(self, packet, interface):
        """Handle range test events."""
        self.stats['range_test_updates'] += 1
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            payload = decoded.get('text', decoded.get('payload', ''))

            logger.info(f"[RX-RT] Range test from {from_id}: payload={payload}")

            if self.packet_callback:
                self.packet_callback(packet, 'RANGE_TEST_APP')

        except Exception as e:
            logger.error(f"[RX-RT] Error: {e}")

    def _on_receive_detection(self, packet, interface):
        """Handle detection sensor events."""
        self.stats['detection_alerts'] += 1
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            alert_text = decoded.get('text', str(decoded.get('payload', '')))

            logger.info(f"[RX-DET] Detection sensor from {from_id}: {alert_text}")

            if self.packet_callback:
                self.packet_callback(packet, 'DETECTION_SENSOR_APP')

        except Exception as e:
            logger.error(f"[RX-DET] Error: {e}")

    def _on_receive_paxcounter(self, packet, interface):
        """Handle paxcounter events."""
        self.stats['paxcounter_updates'] += 1
        try:
            from_id = packet.get('fromId', 'unknown')
            decoded = packet.get('decoded', {})
            pax = decoded.get('paxcounter', decoded)

            wifi = pax.get('wifi', 0)
            ble = pax.get('ble', 0)

            logger.info(f"[RX-PAX] Paxcounter from {from_id}: wifi={wifi}, ble={ble}")

            if self.packet_callback:
                self.packet_callback(packet, 'PAXCOUNTER_APP')

        except Exception as e:
            logger.error(f"[RX-PAX] Error: {e}")

    def _on_receive_mapreport(self, packet, interface):
        """Handle map report events."""
        try:
            from_id = packet.get('fromId', 'unknown')

            logger.info(f"[RX-MAP] Map report from {from_id}")

            if self.packet_callback:
                self.packet_callback(packet, 'MAP_REPORT_APP')

        except Exception as e:
            logger.error(f"[RX-MAP] Error: {e}")

    def _on_connection(self, interface, topic=pub.AUTO_TOPIC):
        """Handle connection established event."""
        with self._lock:
            self.connected = True
        logger.info("[CONN] Connection established event received")
        print("Connection established")

    def _on_disconnect(self, interface, topic=pub.AUTO_TOPIC):
        """Handle connection lost event."""
        with self._lock:
            self.connected = False
        logger.warning("[CONN] Connection lost event received")
        print("Connection lost")

    def _on_node_update(self, node, interface):
        """Handle node update events."""
        self.stats['node_updates'] += 1
        try:
            node_id = node.get('num') or node.get('user', {}).get('id', 'unknown')
            user = node.get('user', {})
            name = user.get('longName', 'Unknown')

            logger.info(f"[NODE-UPDATE] Node update #{self.stats['node_updates']}: {name} ({node_id})")

            # Log detailed node info
            if user:
                logger.debug(f"[NODE-UPDATE]   Short name: {user.get('shortName')}")
                logger.debug(f"[NODE-UPDATE]   HW Model: {user.get('hwModel')}")

            position = node.get('position', {})
            if position:
                logger.debug(f"[NODE-UPDATE]   Position: {position.get('latitude')}, {position.get('longitude')}")

            metrics = node.get('deviceMetrics', {})
            if metrics:
                logger.debug(f"[NODE-UPDATE]   Battery: {metrics.get('batteryLevel')}%")

            # Notify callback
            if self.node_callback:
                self.node_callback(node)

        except Exception as e:
            logger.error(f"[NODE-UPDATE] Error: {e}")

    def get_stats(self) -> Dict[str, int]:
        """Get connector statistics."""
        return self.stats.copy()

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.disconnect()
        return False


class LLMInterface:
    """
    High-level interface designed for LLM integration.

    Provides simple string-based I/O that's easy for an LLM to use.
    """

    def __init__(self, connector: MeshtasticConnector):
        """
        Initialize the LLM interface.

        Args:
            connector: A MeshtasticConnector instance.
        """
        self.connector = connector
        self.conversation_history: List[Dict[str, str]] = []

    def send(self, message: str) -> str:
        """
        Send a message and return a status string.

        Args:
            message: The message to send.

        Returns:
            Status string describing the result.
        """
        if self.connector.send_message(message):
            self.conversation_history.append({
                "role": "sent",
                "content": message,
                "timestamp": datetime.now().isoformat()
            })
            return f"Message sent successfully: {message[:50]}..."
        else:
            return "Failed to send message. Check connection."

    def receive(self) -> str:
        """
        Get received messages as a formatted string.

        Returns:
            Formatted string of received messages or "No new messages."
        """
        messages = self.connector.get_received_messages()

        if not messages:
            return "No new messages."

        result = []
        for msg in messages:
            self.conversation_history.append({
                "role": "received",
                "content": msg.text,
                "from": msg.from_name,
                "timestamp": msg.timestamp.isoformat()
            })
            result.append(f"[{msg.timestamp.strftime('%H:%M:%S')}] {msg.from_name}: {msg.text}")

        return "\n".join(result)

    def get_status(self) -> str:
        """
        Get connection and device status.

        Returns:
            Status string with device information.
        """
        if not self.connector.connected:
            return "Status: Disconnected"

        info = self.connector.get_my_info()
        nodes = self.connector.get_nodes()
        stats = self.connector.get_stats()

        user = info.get('user', {})
        name = user.get('longName', 'Unknown')
        short_name = user.get('shortName', '????')

        return f"""Status: Connected
Device: {name} ({short_name})
Known nodes: {len(nodes)}
Packets received: {stats['packets_received']}
Text messages: {stats['text_messages']}
Messages sent: {stats['messages_sent']}
Send failures: {stats['send_failures']}"""

    def get_nodes_list(self) -> str:
        """
        Get a list of known nodes in the mesh.

        Returns:
            Formatted string listing all known nodes.
        """
        nodes = self.connector.get_nodes()

        if not nodes:
            return "No nodes discovered yet."

        result = ["Known nodes in mesh:"]
        for node_id, node_data in nodes.items():
            user = node_data.get('user', {})
            name = user.get('longName', 'Unknown')
            short = user.get('shortName', '????')
            hw = user.get('hwModel', 'Unknown')
            last_heard = node_data.get('lastHeard', 0)

            if last_heard:
                last_heard_str = datetime.fromtimestamp(last_heard).strftime('%Y-%m-%d %H:%M:%S')
            else:
                last_heard_str = "Never"

            result.append(f"  - {name} ({short}) [{hw}]: Last heard {last_heard_str}")

        return "\n".join(result)


def main():
    """Example usage of the Meshtastic connector."""
    print("=" * 60)
    print("Meshtastic LongFast Bluetooth Connector")
    print("=" * 60)

    # Create connector (will auto-detect device)
    # For BLE: connector = MeshtasticConnector(use_ble=True)
    # For Serial: connector = MeshtasticConnector(use_ble=False)
    # With specific address: connector = MeshtasticConnector(ble_address="AA:BB:CC:DD:EE:FF")

    connector = MeshtasticConnector(use_ble=True)

    # Optional: Scan for BLE devices first
    # devices = connector.scan_ble_devices()

    try:
        if connector.connect():
            # Create LLM-friendly interface
            llm = LLMInterface(connector)

            print("\n" + llm.get_status())
            print("\n" + llm.get_nodes_list())

            # Interactive loop
            print("\n" + "=" * 60)
            print("Interactive Mode (type 'quit' to exit)")
            print("Commands: /status, /nodes, /receive, or type a message to send")
            print("=" * 60)

            while True:
                try:
                    user_input = input("\nYou: ").strip()

                    if not user_input:
                        continue

                    if user_input.lower() == 'quit':
                        break
                    elif user_input == '/status':
                        print(llm.get_status())
                    elif user_input == '/nodes':
                        print(llm.get_nodes_list())
                    elif user_input == '/receive':
                        print(llm.receive())
                    else:
                        print(llm.send(user_input))

                except KeyboardInterrupt:
                    break
        else:
            print("Failed to connect. Make sure your Meshtastic device is:")
            print("  - Powered on")
            print("  - Bluetooth enabled (if using BLE)")
            print("  - Paired with your computer (if using BLE)")
            print("  - Connected via USB (if using Serial)")

    finally:
        connector.disconnect()


if __name__ == "__main__":
    main()
