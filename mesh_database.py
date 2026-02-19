"""
Mesh Database - SQLite storage for all Meshtastic data

Stores:
- ALL raw packets (not just text messages)
- Complete message history with full metadata
- Node information and telemetry
- User profiles and facts
- Conversation context
- Position/GPS data history
- Device telemetry history
- Routing information
- Filtered content log
"""

import sqlite3
import json
import threading
import logging
import time
from datetime import datetime
from typing import Optional, List, Dict, Any
from pathlib import Path

logger = logging.getLogger(__name__)

DB_FILE = "mesh_data.db"


class MeshDatabase:
    """SQLite database for comprehensive mesh network data storage."""

    def __init__(self, db_path: str = DB_FILE):
        self.db_path = db_path
        self._local = threading.local()
        self._init_db()
        logger.info(f"[DB] Database initialized: {self.db_path}")

    def _get_conn(self) -> sqlite3.Connection:
        """Get thread-local database connection."""
        if not hasattr(self._local, 'conn') or self._local.conn is None:
            self._local.conn = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                timeout=30.0  # Wait up to 30 seconds for lock
            )
            self._local.conn.row_factory = sqlite3.Row
            # Enable WAL mode for better concurrent access
            self._local.conn.execute('PRAGMA journal_mode=WAL')
            self._local.conn.execute('PRAGMA busy_timeout=30000')
        return self._local.conn

    def _init_db(self):
        """Initialize database schema."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Check if we need to migrate (old schema exists)
        self._migrate_if_needed(cursor)

        # ==================== RAW PACKETS TABLE ====================
        # Store EVERY packet for later analysis
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS raw_packets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                from_id TEXT,
                to_id TEXT,
                packet_id INTEGER,
                port_num TEXT,
                channel INTEGER,
                hop_limit INTEGER,
                hop_start INTEGER,
                want_ack INTEGER,
                priority TEXT,
                snr REAL,
                rssi INTEGER,
                rx_time INTEGER,
                via_mqtt INTEGER DEFAULT 0,
                packet_type TEXT,
                raw_json TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # ==================== MESSAGES TABLE ====================
        # Text messages only (for quick lookup)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                from_id TEXT,
                from_name TEXT,
                to_id TEXT,
                channel INTEGER DEFAULT 0,
                text TEXT,
                packet_id INTEGER,
                hop_limit INTEGER,
                hop_start INTEGER,
                snr REAL,
                rssi INTEGER,
                rx_time INTEGER,
                priority TEXT,
                want_ack INTEGER,
                via_mqtt INTEGER DEFAULT 0,
                is_outgoing INTEGER DEFAULT 0,
                raw_packet TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # ==================== NODES TABLE ====================
        # All known nodes with full info
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS nodes (
                node_id TEXT PRIMARY KEY,
                node_num INTEGER,
                long_name TEXT,
                short_name TEXT,
                mac_address TEXT,
                hw_model TEXT,
                hw_model_id INTEGER,
                role TEXT,
                is_licensed INTEGER DEFAULT 0,
                is_favorite INTEGER DEFAULT 0,
                latitude REAL,
                longitude REAL,
                altitude INTEGER,
                position_time INTEGER,
                position_precision INTEGER,
                battery_level INTEGER,
                voltage REAL,
                channel_utilization REAL,
                air_util_tx REAL,
                uptime_seconds INTEGER,
                last_heard INTEGER,
                snr REAL,
                hops_away INTEGER,
                via_mqtt INTEGER DEFAULT 0,
                first_seen TEXT,
                last_updated TEXT,
                times_heard INTEGER DEFAULT 1,
                raw_data TEXT
            )
        ''')

        # ==================== USER FACTS ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_facts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                fact_type TEXT,
                fact_value TEXT,
                confidence REAL DEFAULT 1.0,
                source TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(user_id, fact_type, fact_value)
            )
        ''')

        # ==================== GLOBAL CONTEXT ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS global_context (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context TEXT NOT NULL UNIQUE,
                category TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # ==================== TELEMETRY HISTORY ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS telemetry (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                telemetry_type TEXT,
                battery_level INTEGER,
                voltage REAL,
                channel_utilization REAL,
                air_util_tx REAL,
                uptime_seconds INTEGER,
                temperature REAL,
                relative_humidity REAL,
                barometric_pressure REAL,
                gas_resistance REAL,
                iaq INTEGER,
                current REAL,
                raw_data TEXT
            )
        ''')

        # ==================== POSITION HISTORY ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS positions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                latitude REAL,
                longitude REAL,
                altitude INTEGER,
                precision_bits INTEGER,
                speed INTEGER,
                ground_track INTEGER,
                sats_in_view INTEGER,
                pdop INTEGER,
                hdop INTEGER,
                vdop INTEGER,
                gps_accuracy INTEGER,
                fix_quality INTEGER,
                fix_type INTEGER,
                raw_data TEXT
            )
        ''')

        # ==================== ROUTING INFO ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS routing (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                from_id TEXT,
                to_id TEXT,
                packet_id INTEGER,
                error_reason TEXT,
                route_back TEXT,
                route_request TEXT,
                route_reply TEXT,
                snr REAL,
                raw_data TEXT
            )
        ''')

        # ==================== NEIGHBOR INFO ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS neighbors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                node_id TEXT NOT NULL,
                neighbor_id TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                snr REAL,
                last_rx_time INTEGER,
                node_broadcast_interval INTEGER,
                raw_data TEXT,
                UNIQUE(node_id, neighbor_id) ON CONFLICT REPLACE
            )
        ''')

        # ==================== WAYPOINTS ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS waypoints (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                waypoint_id INTEGER,
                node_id TEXT,
                timestamp TEXT NOT NULL,
                name TEXT,
                description TEXT,
                latitude REAL,
                longitude REAL,
                expire INTEGER,
                icon INTEGER,
                locked INTEGER DEFAULT 0,
                raw_data TEXT
            )
        ''')

        # ==================== TRACEROUTE ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS traceroutes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                from_id TEXT,
                to_id TEXT,
                route TEXT,
                snr_towards TEXT,
                snr_back TEXT,
                raw_data TEXT
            )
        ''')

        # ==================== STORE & FORWARD ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS store_forward (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                from_id TEXT,
                to_id TEXT,
                sf_type TEXT,
                messages_total INTEGER,
                messages_saved INTEGER,
                messages_max INTEGER,
                up_time INTEGER,
                requests INTEGER,
                raw_data TEXT
            )
        ''')

        # ==================== RANGE TESTS ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS range_tests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                from_id TEXT,
                to_id TEXT,
                payload TEXT,
                snr REAL,
                rssi INTEGER,
                hop_limit INTEGER,
                hop_start INTEGER,
                raw_data TEXT
            )
        ''')

        # ==================== DETECTION SENSOR ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS detection_sensor (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                from_id TEXT,
                sensor_name TEXT,
                alert_text TEXT,
                snr REAL,
                rssi INTEGER,
                raw_data TEXT
            )
        ''')

        # ==================== PAXCOUNTER ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS paxcounter (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                node_id TEXT,
                wifi_count INTEGER,
                ble_count INTEGER,
                uptime INTEGER,
                raw_data TEXT
            )
        ''')

        # ==================== FILTERED CONTENT ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS filtered_content (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                from_id TEXT,
                from_name TEXT,
                original_text TEXT,
                filter_reason TEXT,
                filter_category TEXT
            )
        ''')

        # ==================== SENT MESSAGES ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS sent_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                to_id TEXT,
                channel INTEGER DEFAULT 0,
                text TEXT,
                packet_id INTEGER,
                want_ack INTEGER DEFAULT 0,
                ack_received INTEGER DEFAULT 0,
                ack_time TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # ==================== PENDING OUTBOX (for dashboard) ====================
        # Messages queued from dashboard to be sent by bridge
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_outbox (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                message TEXT NOT NULL,
                destination TEXT DEFAULT '^all',
                channel INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                sent_at TEXT,
                error TEXT,
                msg_type TEXT DEFAULT 'text'
            )
        ''')

        # ==================== DB METADATA (for change tracking) ====================
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS db_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        # Initialize last_updated if not exists
        cursor.execute('''
            INSERT OR IGNORE INTO db_meta (key, value) VALUES ('last_updated', ?)
        ''', (datetime.now().isoformat(),))

        # ==================== CREATE INDEXES ====================
        indexes = [
            'CREATE INDEX IF NOT EXISTS idx_raw_packets_from ON raw_packets(from_id)',
            'CREATE INDEX IF NOT EXISTS idx_raw_packets_time ON raw_packets(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_raw_packets_type ON raw_packets(packet_type)',
            'CREATE INDEX IF NOT EXISTS idx_messages_from ON messages(from_id)',
            'CREATE INDEX IF NOT EXISTS idx_messages_time ON messages(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_nodes_lastheard ON nodes(last_heard)',
            'CREATE INDEX IF NOT EXISTS idx_nodes_name ON nodes(long_name)',
            'CREATE INDEX IF NOT EXISTS idx_facts_user ON user_facts(user_id)',
            'CREATE INDEX IF NOT EXISTS idx_telemetry_node ON telemetry(node_id)',
            'CREATE INDEX IF NOT EXISTS idx_telemetry_time ON telemetry(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_positions_node ON positions(node_id)',
            'CREATE INDEX IF NOT EXISTS idx_positions_time ON positions(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_routing_time ON routing(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_outbox_status ON pending_outbox(status)',
            'CREATE INDEX IF NOT EXISTS idx_waypoints_node ON waypoints(node_id)',
            'CREATE INDEX IF NOT EXISTS idx_traceroutes_time ON traceroutes(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_store_forward_time ON store_forward(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_range_tests_time ON range_tests(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_detection_sensor_time ON detection_sensor(timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_paxcounter_node_time ON paxcounter(node_id, timestamp)',
            'CREATE INDEX IF NOT EXISTS idx_neighbors_node ON neighbors(node_id)',
        ]
        for idx in indexes:
            cursor.execute(idx)

        conn.commit()

    def _migrate_if_needed(self, cursor):
        """Check for old schema and migrate if needed."""
        try:
            # Check if nodes table exists and has new columns
            cursor.execute("PRAGMA table_info(nodes)")
            columns = {row[1] for row in cursor.fetchall()}

            if 'nodes' in self._get_tables(cursor):
                # Check for new columns
                new_columns = ['times_heard', 'mac_address', 'node_num', 'hw_model_id', 'is_favorite', 'position_precision', 'via_mqtt']
                missing = [col for col in new_columns if col not in columns]

                if missing:
                    logger.warning(f"[DB] Old schema detected, missing columns: {missing}")
                    logger.warning("[DB] Migrating database schema...")

                    # Add missing columns with defaults
                    migrations = {
                        'times_heard': 'INTEGER DEFAULT 1',
                        'mac_address': 'TEXT',
                        'node_num': 'INTEGER',
                        'hw_model_id': 'INTEGER',
                        'is_favorite': 'INTEGER DEFAULT 0',
                        'position_precision': 'INTEGER',
                        'via_mqtt': 'INTEGER DEFAULT 0',
                    }

                    for col in missing:
                        if col in migrations:
                            try:
                                cursor.execute(f"ALTER TABLE nodes ADD COLUMN {col} {migrations[col]}")
                                logger.info(f"[DB] Added column: nodes.{col}")
                            except Exception as e:
                                logger.warning(f"[DB] Could not add column {col}: {e}")

            # Check for new tables
            existing_tables = self._get_tables(cursor)
            if 'raw_packets' not in existing_tables:
                logger.info("[DB] Creating new tables for comprehensive data storage...")

            # Migrate pending_outbox: add msg_type column
            if 'pending_outbox' in self._get_tables(cursor):
                cursor.execute("PRAGMA table_info(pending_outbox)")
                outbox_cols = {row[1] for row in cursor.fetchall()}
                if 'msg_type' not in outbox_cols:
                    try:
                        cursor.execute("ALTER TABLE pending_outbox ADD COLUMN msg_type TEXT DEFAULT 'text'")
                        logger.info("[DB] Added column: pending_outbox.msg_type")
                    except Exception as e:
                        logger.warning(f"[DB] Could not add msg_type column: {e}")

        except Exception as e:
            logger.error(f"[DB] Migration check failed: {e}")

    def _get_tables(self, cursor) -> set:
        """Get set of existing table names."""
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {row[0] for row in cursor.fetchall()}

    # ==================== SAFE JSON HELPERS ====================

    def _safe_json_dumps(self, obj: Any) -> Optional[str]:
        """Safely convert object to JSON, handling bytes and other non-serializable types."""
        if obj is None:
            return None
        try:
            def default_encoder(o):
                if isinstance(o, bytes):
                    return o.hex()
                elif hasattr(o, 'isoformat'):
                    return o.isoformat()
                elif hasattr(o, '__dict__'):
                    return str(o)
                else:
                    return str(o)
            return json.dumps(obj, default=default_encoder)
        except Exception as e:
            logger.warning(f"[DB] Failed to serialize to JSON: {e}")
            return str(obj)[:2000]

    # ==================== RAW PACKET OPERATIONS ====================

    def save_raw_packet(self, packet: Dict[str, Any], packet_type: str) -> int:
        """Save ANY raw packet to database for later analysis."""
        conn = self._get_conn()
        cursor = conn.cursor()

        from_id = packet.get('fromId')
        to_id = packet.get('toId')
        decoded = packet.get('decoded', {})

        logger.debug(f"[DB] Saving raw packet: type={packet_type}, from={from_id}")

        cursor.execute('''
            INSERT INTO raw_packets (
                timestamp, from_id, to_id, packet_id, port_num, channel,
                hop_limit, hop_start, want_ack, priority, snr, rssi,
                rx_time, via_mqtt, packet_type, raw_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            from_id,
            to_id,
            packet.get('id'),
            decoded.get('portnum'),
            packet.get('channel', 0),
            packet.get('hopLimit'),
            packet.get('hopStart'),
            1 if packet.get('wantAck') else 0,
            packet.get('priority'),
            packet.get('rxSnr'),
            packet.get('rxRssi'),
            packet.get('rxTime'),
            1 if packet.get('viaMqtt') else 0,
            packet_type,
            self._safe_json_dumps(packet)
        ))

        conn.commit()
        logger.debug(f"[DB] Raw packet saved with id={cursor.lastrowid}")
        return cursor.lastrowid

    def get_raw_packets(self, packet_type: Optional[str] = None, from_id: Optional[str] = None,
                        limit: int = 100, offset: int = 0) -> List[Dict]:
        """Get raw packets with optional filtering."""
        conn = self._get_conn()
        cursor = conn.cursor()

        query = 'SELECT * FROM raw_packets WHERE 1=1'
        params = []

        if packet_type:
            query += ' AND packet_type = ?'
            params.append(packet_type)
        if from_id:
            query += ' AND from_id = ?'
            params.append(from_id)

        query += ' ORDER BY timestamp DESC LIMIT ? OFFSET ?'
        params.extend([limit, offset])

        cursor.execute(query, params)
        return [dict(row) for row in cursor.fetchall()]

    # ==================== MESSAGE OPERATIONS ====================

    def save_message(self, message_data: Dict[str, Any], is_outgoing: bool = False) -> int:
        """Save a text message to the database."""
        conn = self._get_conn()
        cursor = conn.cursor()

        raw_packet_json = self._safe_json_dumps(message_data.get('raw_packet'))

        logger.info(f"[DB] Saving message: from={message_data.get('from_name')}, outgoing={is_outgoing}")

        cursor.execute('''
            INSERT INTO messages (
                timestamp, from_id, from_name, to_id, channel, text,
                packet_id, hop_limit, hop_start, snr, rssi, rx_time,
                priority, want_ack, via_mqtt, is_outgoing, raw_packet
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            message_data.get('timestamp', datetime.now().isoformat()),
            message_data.get('from_id'),
            message_data.get('from_name'),
            message_data.get('to_id'),
            message_data.get('channel', 0),
            message_data.get('text'),
            message_data.get('packet_id'),
            message_data.get('hop_limit'),
            message_data.get('hop_start'),
            message_data.get('snr'),
            message_data.get('rssi'),
            message_data.get('rx_time'),
            message_data.get('priority'),
            message_data.get('want_ack', 0),
            message_data.get('via_mqtt', 0),
            1 if is_outgoing else 0,
            raw_packet_json
        ))

        conn.commit()
        self._update_last_modified()
        logger.info(f"[DB] Message saved with id={cursor.lastrowid}")
        return cursor.lastrowid

    def save_sent_message(self, text: str, to_id: str = "^all", channel: int = 0,
                          packet_id: Optional[int] = None, want_ack: bool = False) -> int:
        """Save a message we sent."""
        conn = self._get_conn()
        cursor = conn.cursor()

        logger.info(f"[DB] Saving sent message: to={to_id}, text={text[:50]}...")

        cursor.execute('''
            INSERT INTO sent_messages (timestamp, to_id, channel, text, packet_id, want_ack)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            to_id,
            channel,
            text,
            packet_id,
            1 if want_ack else 0
        ))

        conn.commit()
        self._update_last_modified()
        return cursor.lastrowid

    def get_messages(self, from_id: Optional[str] = None, limit: int = 50, offset: int = 0,
                     include_outgoing: bool = True) -> List[Dict]:
        """Get messages, optionally filtered by sender."""
        conn = self._get_conn()
        cursor = conn.cursor()

        if from_id:
            cursor.execute('''
                SELECT * FROM messages WHERE from_id = ?
                ORDER BY timestamp DESC LIMIT ? OFFSET ?
            ''', (from_id, limit, offset))
        else:
            if include_outgoing:
                cursor.execute('''
                    SELECT * FROM messages ORDER BY timestamp DESC LIMIT ? OFFSET ?
                ''', (limit, offset))
            else:
                cursor.execute('''
                    SELECT * FROM messages WHERE is_outgoing = 0
                    ORDER BY timestamp DESC LIMIT ? OFFSET ?
                ''', (limit, offset))

        return [dict(row) for row in cursor.fetchall()]

    def get_conversation_history(self, user_id: str, limit: int = 10) -> List[Dict]:
        """Get recent conversation with a specific user."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM messages
            WHERE from_id = ? OR (from_name = 'assistant' AND to_id = ?)
            ORDER BY timestamp DESC LIMIT ?
        ''', (user_id, user_id, limit))

        messages = [dict(row) for row in cursor.fetchall()]
        messages.reverse()
        return messages

    def get_message_count(self, from_id: Optional[str] = None) -> int:
        """Get total message count."""
        conn = self._get_conn()
        cursor = conn.cursor()

        if from_id:
            cursor.execute('SELECT COUNT(*) FROM messages WHERE from_id = ?', (from_id,))
        else:
            cursor.execute('SELECT COUNT(*) FROM messages')

        return cursor.fetchone()[0]

    # ==================== NODE OPERATIONS ====================

    def save_node(self, node_data: Dict[str, Any]):
        """Save or update a node with comprehensive data."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Handle various ways node_id might be passed
        node_id = (node_data.get('node_id') or
                   node_data.get('id') or
                   node_data.get('user', {}).get('id'))

        if not node_id:
            # Try to construct from num
            num = node_data.get('num')
            if num:
                node_id = f"!{num:08x}"

        if not node_id:
            logger.warning("[DB] Cannot save node without node_id")
            return

        logger.info(f"[DB] Saving/updating node: {node_id}")

        # Check if node exists
        cursor.execute('SELECT first_seen FROM nodes WHERE node_id = ?', (node_id,))
        existing = cursor.fetchone()
        first_seen = existing['first_seen'] if existing else datetime.now().isoformat()

        # Try to get times_heard (may not exist in old schema)
        times_heard = 1
        if existing:
            try:
                cursor.execute('SELECT times_heard FROM nodes WHERE node_id = ?', (node_id,))
                th_row = cursor.fetchone()
                if th_row and th_row[0]:
                    times_heard = th_row[0] + 1
            except:
                times_heard = 1

        # Extract user info
        user = node_data.get('user', {})
        position = node_data.get('position', {})
        device_metrics = node_data.get('deviceMetrics', {})

        # Handle MAC address (might be bytes)
        mac = user.get('macaddr')
        if isinstance(mac, bytes):
            mac = mac.hex()
        elif isinstance(mac, str) and len(mac) == 12:
            mac = ':'.join(mac[i:i+2] for i in range(0, 12, 2))

        cursor.execute('''
            INSERT OR REPLACE INTO nodes (
                node_id, node_num, long_name, short_name, mac_address,
                hw_model, hw_model_id, role, is_licensed, is_favorite,
                latitude, longitude, altitude, position_time, position_precision,
                battery_level, voltage, channel_utilization, air_util_tx,
                uptime_seconds, last_heard, snr, hops_away, via_mqtt,
                first_seen, last_updated, times_heard, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            node_id,
            node_data.get('num'),
            user.get('longName') or node_data.get('long_name'),
            user.get('shortName') or node_data.get('short_name'),
            mac,
            user.get('hwModel') or node_data.get('hw_model'),
            user.get('hwModelId'),
            user.get('role') or node_data.get('role'),
            1 if user.get('isLicensed') else 0,
            1 if node_data.get('isFavorite') else 0,
            position.get('latitude') or node_data.get('latitude'),
            position.get('longitude') or node_data.get('longitude'),
            position.get('altitude') or node_data.get('altitude'),
            position.get('time') or node_data.get('position_time'),
            position.get('precisionBits'),
            device_metrics.get('batteryLevel') or node_data.get('battery_level'),
            device_metrics.get('voltage') or node_data.get('voltage'),
            device_metrics.get('channelUtilization') or node_data.get('channel_utilization'),
            device_metrics.get('airUtilTx') or node_data.get('air_util_tx'),
            device_metrics.get('uptimeSeconds') or node_data.get('uptime_seconds'),
            node_data.get('lastHeard') or node_data.get('last_heard'),
            node_data.get('snr'),
            node_data.get('hopsAway') or node_data.get('hops_away'),
            1 if node_data.get('viaMqtt') else 0,
            first_seen,
            datetime.now().isoformat(),
            times_heard,
            self._safe_json_dumps(node_data)
        ))

        conn.commit()
        self._update_last_modified()
        logger.debug(f"[DB] Node {node_id} saved (heard {times_heard} times)")

    def touch_node_last_heard(self, node_id: str, timestamp: int):
        """Update last_heard for a node from any received packet (telemetry, position, message).

        The meshtastic.node.updated pubsub event only fires during initial sync,
        so we need to update last_heard whenever we get any packet from a node.
        """
        try:
            conn = self._get_conn()
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE nodes SET last_heard = MAX(COALESCE(last_heard, 0), ?),
                                 times_heard = COALESCE(times_heard, 0) + 1,
                                 last_updated = ?
                WHERE node_id = ?
            ''', (timestamp, datetime.now().isoformat(), node_id))
            if cursor.rowcount > 0:
                conn.commit()
                self._update_last_modified()
        except Exception as e:
            logger.debug(f"[DB] touch_node_last_heard failed for {node_id}: {e}")

    def get_node(self, node_id: str) -> Optional[Dict]:
        """Get a specific node by ID."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM nodes WHERE node_id = ?', (node_id,))
        row = cursor.fetchone()
        return dict(row) if row else None

    def get_all_nodes(self) -> List[Dict]:
        """Get all known nodes."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM nodes ORDER BY last_heard DESC')
        return [dict(row) for row in cursor.fetchall()]

    def get_active_nodes(self, hours: int = 24) -> List[Dict]:
        """Get nodes seen within the last N hours."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cutoff = int(time.time()) - (hours * 3600)

        cursor.execute('''
            SELECT * FROM nodes WHERE last_heard > ? ORDER BY last_heard DESC
        ''', (cutoff,))

        return [dict(row) for row in cursor.fetchall()]

    # ==================== TELEMETRY OPERATIONS ====================

    def save_telemetry(self, node_id: str, telemetry: Dict[str, Any], telemetry_type: str = "device"):
        """Save telemetry data."""
        if not node_id:
            logger.warning("[DB] Cannot save telemetry without node_id")
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        device_metrics = telemetry.get('deviceMetrics', {})
        env_metrics = telemetry.get('environmentMetrics', {})
        power_metrics = telemetry.get('powerMetrics', {})

        logger.info(f"[DB] Saving telemetry for {node_id}, type={telemetry_type}")

        cursor.execute('''
            INSERT INTO telemetry (
                node_id, timestamp, telemetry_type, battery_level, voltage,
                channel_utilization, air_util_tx, uptime_seconds,
                temperature, relative_humidity, barometric_pressure,
                gas_resistance, iaq, current, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            node_id,
            datetime.now().isoformat(),
            telemetry_type,
            device_metrics.get('batteryLevel'),
            device_metrics.get('voltage'),
            device_metrics.get('channelUtilization'),
            device_metrics.get('airUtilTx'),
            device_metrics.get('uptimeSeconds'),
            env_metrics.get('temperature'),
            env_metrics.get('relativeHumidity'),
            env_metrics.get('barometricPressure'),
            env_metrics.get('gasResistance'),
            env_metrics.get('iaq'),
            power_metrics.get('ch1Current'),
            self._safe_json_dumps(telemetry)
        ))

        conn.commit()

    def get_telemetry_history(self, node_id: str, limit: int = 100) -> List[Dict]:
        """Get telemetry history for a node."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM telemetry WHERE node_id = ?
            ORDER BY timestamp DESC LIMIT ?
        ''', (node_id, limit))

        return [dict(row) for row in cursor.fetchall()]

    # ==================== POSITION OPERATIONS ====================

    def save_position(self, node_id: str, position: Dict[str, Any]):
        """Save position data."""
        if not node_id:
            logger.warning("[DB] Cannot save position without node_id")
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        # Handle both raw and converted coordinates
        lat = position.get('latitude')
        if lat is None and 'latitudeI' in position:
            lat = position['latitudeI'] / 1e7

        lon = position.get('longitude')
        if lon is None and 'longitudeI' in position:
            lon = position['longitudeI'] / 1e7

        logger.info(f"[DB] Saving position for {node_id}: lat={lat}, lon={lon}")

        cursor.execute('''
            INSERT INTO positions (
                node_id, timestamp, latitude, longitude, altitude,
                precision_bits, speed, ground_track, sats_in_view,
                pdop, hdop, vdop, gps_accuracy, fix_quality, fix_type, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            node_id,
            datetime.now().isoformat(),
            lat,
            lon,
            position.get('altitude'),
            position.get('precisionBits'),
            position.get('groundSpeed'),
            position.get('groundTrack'),
            position.get('satsInView'),
            position.get('PDOP'),
            position.get('HDOP'),
            position.get('VDOP'),
            position.get('gpsAccuracy'),
            position.get('fixQuality'),
            position.get('fixType'),
            self._safe_json_dumps(position)
        ))

        conn.commit()

    def get_position_history(self, node_id: str, limit: int = 100) -> List[Dict]:
        """Get position history for a node."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM positions WHERE node_id = ?
            ORDER BY timestamp DESC LIMIT ?
        ''', (node_id, limit))

        return [dict(row) for row in cursor.fetchall()]

    # ==================== ROUTING OPERATIONS ====================

    def save_routing(self, packet: Dict[str, Any]):
        """Save routing information."""
        from_id = packet.get('fromId')
        if not from_id:
            logger.debug("[DB] Skipping routing without from_id")
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        decoded = packet.get('decoded', {})
        routing = decoded.get('routing', {})

        logger.debug(f"[DB] Saving routing info from {from_id}")

        cursor.execute('''
            INSERT INTO routing (
                timestamp, from_id, to_id, packet_id, error_reason,
                route_back, route_request, route_reply, snr, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            packet.get('fromId'),
            packet.get('toId'),
            packet.get('id'),
            routing.get('errorReason'),
            self._safe_json_dumps(routing.get('routeBack')),
            self._safe_json_dumps(routing.get('routeRequest')),
            self._safe_json_dumps(routing.get('routeReply')),
            packet.get('rxSnr'),
            self._safe_json_dumps(packet)
        ))

        conn.commit()

    # ==================== NEIGHBOR OPERATIONS ====================

    def save_neighbor(self, node_id: str, neighbor_data: Dict[str, Any]):
        """Save neighbor information."""
        if not node_id:
            logger.warning("[DB] Cannot save neighbor without node_id")
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        neighbor_id = neighbor_data.get('nodeId')
        if isinstance(neighbor_id, int):
            neighbor_id = f"!{neighbor_id:08x}"

        cursor.execute('''
            INSERT OR REPLACE INTO neighbors (
                node_id, neighbor_id, timestamp, snr, last_rx_time,
                node_broadcast_interval, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            node_id,
            neighbor_id,
            datetime.now().isoformat(),
            neighbor_data.get('snr'),
            neighbor_data.get('lastRxTime'),
            neighbor_data.get('nodeBroadcastIntervalSecs'),
            self._safe_json_dumps(neighbor_data)
        ))

        conn.commit()

    # ==================== WAYPOINT OPERATIONS ====================

    def save_waypoint(self, node_id: str, waypoint_data: Dict[str, Any], packet: Dict[str, Any] = None):
        """Save or update a waypoint."""
        if not node_id:
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        # Handle latitudeI/longitudeI conversion
        lat = waypoint_data.get('latitude')
        if lat is None and 'latitudeI' in waypoint_data:
            lat = waypoint_data['latitudeI'] / 1e7

        lon = waypoint_data.get('longitude')
        if lon is None and 'longitudeI' in waypoint_data:
            lon = waypoint_data['longitudeI'] / 1e7

        waypoint_id = waypoint_data.get('id')

        logger.info(f"[DB] Saving waypoint from {node_id}: {waypoint_data.get('name', 'unnamed')}")

        # Use INSERT OR REPLACE keyed on waypoint_id if available
        if waypoint_id:
            cursor.execute('DELETE FROM waypoints WHERE waypoint_id = ?', (waypoint_id,))

        cursor.execute('''
            INSERT INTO waypoints (
                waypoint_id, node_id, timestamp, name, description,
                latitude, longitude, expire, icon, locked, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            waypoint_id,
            node_id,
            datetime.now().isoformat(),
            waypoint_data.get('name'),
            waypoint_data.get('description'),
            lat,
            lon,
            waypoint_data.get('expire'),
            waypoint_data.get('icon'),
            1 if waypoint_data.get('locked') else 0,
            self._safe_json_dumps(waypoint_data)
        ))

        conn.commit()
        self._update_last_modified()

    def get_waypoints(self, active_only: bool = True, limit: int = 100) -> List[Dict]:
        """Get waypoints, optionally filtering expired ones."""
        conn = self._get_conn()
        cursor = conn.cursor()

        if active_only:
            now_unix = int(time.time())
            cursor.execute('''
                SELECT * FROM waypoints
                WHERE expire IS NULL OR expire = 0 OR expire > ?
                ORDER BY timestamp DESC LIMIT ?
            ''', (now_unix, limit))
        else:
            cursor.execute('SELECT * FROM waypoints ORDER BY timestamp DESC LIMIT ?', (limit,))

        return [dict(row) for row in cursor.fetchall()]

    # ==================== TRACEROUTE OPERATIONS ====================

    def save_traceroute(self, packet: Dict[str, Any]):
        """Save a traceroute result."""
        from_id = packet.get('fromId')
        to_id = packet.get('toId')

        conn = self._get_conn()
        cursor = conn.cursor()

        decoded = packet.get('decoded', {})
        traceroute = decoded.get('traceroute', decoded)

        route = traceroute.get('route', [])
        snr_towards = traceroute.get('snrTowards', [])
        snr_back = traceroute.get('snrBack', [])

        logger.info(f"[DB] Saving traceroute from {from_id} to {to_id}, route: {route}")

        cursor.execute('''
            INSERT INTO traceroutes (
                timestamp, from_id, to_id, route, snr_towards, snr_back, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            from_id,
            to_id,
            self._safe_json_dumps(route),
            self._safe_json_dumps(snr_towards),
            self._safe_json_dumps(snr_back),
            self._safe_json_dumps(packet)
        ))

        conn.commit()
        self._update_last_modified()

    def get_traceroutes(self, limit: int = 50) -> List[Dict]:
        """Get recent traceroutes."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM traceroutes ORDER BY timestamp DESC LIMIT ?', (limit,))
        results = []
        for row in cursor.fetchall():
            d = dict(row)
            # Parse JSON fields
            for field in ('route', 'snr_towards', 'snr_back'):
                if d.get(field) and isinstance(d[field], str):
                    try:
                        d[field] = json.loads(d[field])
                    except:
                        pass
            results.append(d)
        return results

    # ==================== STORE & FORWARD OPERATIONS ====================

    def save_store_forward(self, node_id: str, sf_data: Dict[str, Any], packet: Dict[str, Any] = None):
        """Save store & forward stats/heartbeat."""
        if not node_id:
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        # Determine SF type from data
        sf_type = 'unknown'
        stats_data = sf_data.get('stats', {})
        heartbeat = sf_data.get('heartbeat', {})
        if stats_data:
            sf_type = 'stats'
        elif heartbeat:
            sf_type = 'heartbeat'
            stats_data = heartbeat
        elif sf_data.get('history'):
            sf_type = 'history'

        logger.info(f"[DB] Saving store_forward from {node_id}, type={sf_type}")

        cursor.execute('''
            INSERT INTO store_forward (
                timestamp, from_id, to_id, sf_type, messages_total,
                messages_saved, messages_max, up_time, requests, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            node_id,
            packet.get('toId') if packet else None,
            sf_type,
            stats_data.get('messagesTotal'),
            stats_data.get('messagesSaved'),
            stats_data.get('messagesMax'),
            stats_data.get('upTime'),
            stats_data.get('requests'),
            self._safe_json_dumps(sf_data)
        ))

        conn.commit()

    def get_store_forward_stats(self) -> List[Dict]:
        """Get latest store & forward stats per node."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT sf1.*
            FROM store_forward sf1
            INNER JOIN (
                SELECT from_id, MAX(timestamp) as max_ts
                FROM store_forward
                WHERE sf_type = 'stats' OR sf_type = 'heartbeat'
                GROUP BY from_id
            ) sf2 ON sf1.from_id = sf2.from_id AND sf1.timestamp = sf2.max_ts
            ORDER BY sf1.timestamp DESC
        ''')

        return [dict(row) for row in cursor.fetchall()]

    # ==================== RANGE TEST OPERATIONS ====================

    def save_range_test(self, from_id: str, payload: str, packet: Dict[str, Any] = None):
        """Save a range test result."""
        if not from_id:
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        logger.info(f"[DB] Saving range_test from {from_id}: {payload}")

        cursor.execute('''
            INSERT INTO range_tests (
                timestamp, from_id, to_id, payload, snr, rssi,
                hop_limit, hop_start, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            from_id,
            packet.get('toId') if packet else None,
            payload,
            packet.get('rxSnr') if packet else None,
            packet.get('rxRssi') if packet else None,
            packet.get('hopLimit') if packet else None,
            packet.get('hopStart') if packet else None,
            self._safe_json_dumps(packet)
        ))

        conn.commit()

    def get_range_tests(self, limit: int = 50) -> List[Dict]:
        """Get recent range test results."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM range_tests ORDER BY timestamp DESC LIMIT ?', (limit,))
        return [dict(row) for row in cursor.fetchall()]

    # ==================== DETECTION SENSOR OPERATIONS ====================

    def save_detection_sensor(self, from_id: str, alert_text: str, packet: Dict[str, Any] = None):
        """Save a detection sensor alert."""
        if not from_id:
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        # Try to get sensor name from node lookup
        sensor_name = None
        node = self.get_node(from_id)
        if node:
            sensor_name = node.get('long_name')

        logger.info(f"[DB] Saving detection_sensor from {from_id}: {alert_text}")

        cursor.execute('''
            INSERT INTO detection_sensor (
                timestamp, from_id, sensor_name, alert_text, snr, rssi, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            from_id,
            sensor_name,
            alert_text,
            packet.get('rxSnr') if packet else None,
            packet.get('rxRssi') if packet else None,
            self._safe_json_dumps(packet)
        ))

        conn.commit()
        self._update_last_modified()

    def get_detection_alerts(self, limit: int = 50) -> List[Dict]:
        """Get recent detection sensor alerts."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM detection_sensor ORDER BY timestamp DESC LIMIT ?', (limit,))
        return [dict(row) for row in cursor.fetchall()]

    # ==================== PAXCOUNTER OPERATIONS ====================

    def save_paxcounter(self, node_id: str, pax_data: Dict[str, Any], packet: Dict[str, Any] = None):
        """Save paxcounter data."""
        if not node_id:
            return

        conn = self._get_conn()
        cursor = conn.cursor()

        logger.info(f"[DB] Saving paxcounter from {node_id}: wifi={pax_data.get('wifi')}, ble={pax_data.get('ble')}")

        cursor.execute('''
            INSERT INTO paxcounter (
                timestamp, node_id, wifi_count, ble_count, uptime, raw_data
            ) VALUES (?, ?, ?, ?, ?, ?)
        ''', (
            datetime.now().isoformat(),
            node_id,
            pax_data.get('wifi'),
            pax_data.get('ble'),
            pax_data.get('uptime'),
            self._safe_json_dumps(pax_data)
        ))

        conn.commit()

    def get_paxcounter_history(self, node_id: Optional[str] = None, limit: int = 100) -> List[Dict]:
        """Get paxcounter history, optionally for a specific node."""
        conn = self._get_conn()
        cursor = conn.cursor()

        if node_id:
            cursor.execute('SELECT * FROM paxcounter WHERE node_id = ? ORDER BY timestamp DESC LIMIT ?', (node_id, limit))
        else:
            cursor.execute('SELECT * FROM paxcounter ORDER BY timestamp DESC LIMIT ?', (limit,))

        return [dict(row) for row in cursor.fetchall()]

    # ==================== NETWORK TOPOLOGY ====================

    def get_neighbors_graph(self) -> List[Dict]:
        """Get all neighbor relationships as a list of dicts."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('SELECT * FROM neighbors ORDER BY timestamp DESC')
        return [dict(row) for row in cursor.fetchall()]

    def get_network_topology(self) -> Dict:
        """Build network topology from traceroutes and neighbors."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Only include nodes heard in last 24h that have connections
        cutoff = int(time.time()) - (24 * 3600)
        cursor.execute('''
            SELECT node_id, long_name, short_name, latitude, longitude,
                   hops_away, battery_level, last_heard
            FROM nodes WHERE last_heard > ?
        ''', (cutoff,))
        all_nodes = {}
        for row in cursor.fetchall():
            d = dict(row)
            all_nodes[d['node_id']] = d

        # Build edges from traceroutes (primary source — much richer than neighbors)
        edges = []
        edge_set = set()
        cursor.execute('''
            SELECT from_id, to_id, route, snr_towards
            FROM traceroutes ORDER BY timestamp DESC LIMIT 100
        ''')
        for row in cursor.fetchall():
            d = dict(row)
            route = d.get('route')
            if route and isinstance(route, str):
                try:
                    route = json.loads(route)
                except:
                    route = []
            snr_towards = d.get('snr_towards')
            if snr_towards and isinstance(snr_towards, str):
                try:
                    snr_towards = json.loads(snr_towards)
                except:
                    snr_towards = []

            # Build chain: from_id → hop1 → hop2 → ... → to_id
            chain = [d['from_id']]
            if isinstance(route, list):
                for hop in route:
                    hop_id = '!' + hex(hop)[2:].zfill(8) if isinstance(hop, int) else str(hop)
                    chain.append(hop_id)
            chain.append(d['to_id'])

            for i in range(len(chain) - 1):
                a, b = chain[i], chain[i+1]
                if not a or not b:
                    continue
                key = tuple(sorted([a, b]))
                if key not in edge_set:
                    edge_set.add(key)
                    snr = None
                    if isinstance(snr_towards, list) and i < len(snr_towards):
                        snr = snr_towards[i] / 4 if snr_towards[i] else None
                    edges.append({'node_id': a, 'neighbor_id': b, 'snr': snr})

        # Also include neighbor table edges
        cursor.execute('SELECT node_id, neighbor_id, snr FROM neighbors')
        for row in cursor.fetchall():
            d = dict(row)
            key = tuple(sorted([d['node_id'], d.get('neighbor_id', '')]))
            if key not in edge_set and d.get('neighbor_id'):
                edge_set.add(key)
                edges.append(d)

        # Only include nodes that appear in edges
        connected_ids = set()
        for e in edges:
            connected_ids.add(e['node_id'])
            connected_ids.add(e['neighbor_id'])

        nodes = []
        for nid in connected_ids:
            if nid in all_nodes:
                nodes.append(all_nodes[nid])
            else:
                nodes.append({'node_id': nid, 'long_name': nid, 'short_name': nid[-4:] if len(nid) > 4 else nid})

        return {'nodes': nodes, 'edges': edges}

    # ==================== ADVANCED QUERY METHODS ====================

    def get_telemetry_summary(self, node_id: Optional[str] = None, hours: int = 24) -> Dict:
        """Get aggregated telemetry summary."""
        conn = self._get_conn()
        cursor = conn.cursor()

        from datetime import timedelta as td
        cutoff = (datetime.now() - td(hours=hours)).isoformat()

        if node_id:
            cursor.execute('''
                SELECT
                    AVG(battery_level) as avg_battery,
                    MIN(battery_level) as min_battery,
                    MAX(battery_level) as max_battery,
                    AVG(temperature) as avg_temp,
                    MIN(temperature) as min_temp,
                    MAX(temperature) as max_temp,
                    AVG(relative_humidity) as avg_humidity,
                    AVG(channel_utilization) as avg_ch_util
                FROM telemetry
                WHERE node_id = ? AND timestamp > ?
            ''', (node_id, cutoff))
        else:
            cursor.execute('''
                SELECT
                    AVG(battery_level) as avg_battery,
                    MIN(battery_level) as min_battery,
                    MAX(battery_level) as max_battery,
                    AVG(temperature) as avg_temp,
                    MIN(temperature) as min_temp,
                    MAX(temperature) as max_temp,
                    AVG(relative_humidity) as avg_humidity,
                    AVG(channel_utilization) as avg_ch_util
                FROM telemetry
                WHERE timestamp > ?
            ''', (cutoff,))

        row = cursor.fetchone()
        if row:
            return {k: (round(row[k], 1) if row[k] is not None else None) for k in row.keys()}
        return {}

    def get_signal_trends(self, node_id: str, hours: int = 24) -> List[Dict]:
        """Get SNR/RSSI trends bucketed by hour."""
        conn = self._get_conn()
        cursor = conn.cursor()

        from datetime import timedelta as td
        cutoff = (datetime.now() - td(hours=hours)).isoformat()

        cursor.execute('''
            SELECT
                strftime('%H', timestamp) as hour,
                AVG(snr) as avg_snr,
                MIN(snr) as min_snr,
                MAX(snr) as max_snr,
                AVG(rssi) as avg_rssi,
                COUNT(*) as count
            FROM messages
            WHERE from_id = ? AND timestamp > ? AND snr IS NOT NULL
            GROUP BY hour
            ORDER BY hour
        ''', (node_id, cutoff))

        results = []
        for row in cursor.fetchall():
            results.append({
                'hour': row['hour'],
                'avg_snr': round(row['avg_snr'], 1) if row['avg_snr'] else None,
                'min_snr': round(row['min_snr'], 1) if row['min_snr'] else None,
                'max_snr': round(row['max_snr'], 1) if row['max_snr'] else None,
                'avg_rssi': round(row['avg_rssi'], 1) if row['avg_rssi'] else None,
                'count': row['count']
            })
        return results

    # ==================== USER FACTS ====================

    def save_fact(self, user_id: str, fact_type: str, fact_value: str,
                  confidence: float = 1.0, source: Optional[str] = None):
        """Save a fact about a user."""
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT OR REPLACE INTO user_facts (user_id, fact_type, fact_value, confidence, source)
                VALUES (?, ?, ?, ?, ?)
            ''', (user_id, fact_type, fact_value, confidence, source))
            conn.commit()
            logger.debug(f"[DB] Saved fact for {user_id}: {fact_type}={fact_value}")
        except sqlite3.IntegrityError:
            pass

    def get_user_facts(self, user_id: str) -> List[Dict]:
        """Get all facts about a user."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM user_facts WHERE user_id = ? ORDER BY created_at DESC
        ''', (user_id,))

        return [dict(row) for row in cursor.fetchall()]

    def get_user_profile(self, user_id: str) -> Dict:
        """Get a compiled profile for a user."""
        node = self.get_node(user_id)
        facts = self.get_user_facts(user_id)
        message_count = self.get_message_count(user_id)
        recent_messages = self.get_conversation_history(user_id, limit=5)
        positions = self.get_position_history(user_id, limit=5)
        telemetry = self.get_telemetry_history(user_id, limit=5)

        return {
            'node_info': node,
            'facts': facts,
            'message_count': message_count,
            'recent_messages': recent_messages,
            'recent_positions': positions,
            'recent_telemetry': telemetry
        }

    # ==================== GLOBAL CONTEXT ====================

    def save_global_context(self, context: str, category: Optional[str] = None):
        """Save global context."""
        conn = self._get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute('''
                INSERT INTO global_context (context, category) VALUES (?, ?)
            ''', (context, category))
            conn.commit()
        except sqlite3.IntegrityError:
            pass

    def get_global_context(self, limit: int = 10) -> List[str]:
        """Get global context items."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT context FROM global_context ORDER BY created_at DESC LIMIT ?
        ''', (limit,))

        return [row['context'] for row in cursor.fetchall()]

    # ==================== CONTENT FILTERING ====================

    def log_filtered_content(self, from_id: str, from_name: str,
                             text: str, reason: Optional[str], category: str):
        """Log content that was filtered."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO filtered_content (timestamp, from_id, from_name, original_text, filter_reason, filter_category)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (datetime.now().isoformat(), from_id, from_name, text, reason, category))

        conn.commit()

    # ==================== CONTEXT BUILDING ====================

    def build_context_for_llm(self, user_id: str, user_name: str, intent: str = 'question') -> str:
        """Build context string for LLM prompts, gated by message intent.

        Intent controls which sections are included to avoid flooding small models
        with irrelevant data (e.g. stats on a casual greeting).

        Context by intent:
            greeting  — conversation history only
            casual    — conversation history + user facts
            question  — conversation history + user facts + global context + alerts
            weather   — conversation history only (weather fetched separately)
            signal    — conversation history + node/device info + telemetry
            network   — everything
        """
        parts = []

        include_global = intent in ('question', 'network')
        include_facts = intent in ('casual', 'question', 'network')
        include_node = intent in ('signal', 'network')
        include_telemetry = intent in ('signal', 'network')
        include_health = intent in ('network',)
        include_stats = intent in ('network',)
        include_alerts = intent in ('question',)

        # Global context
        if include_global:
            global_ctx = self.get_global_context(limit=5)
            if global_ctx:
                parts.append("System context: " + "; ".join(global_ctx))

        # User facts
        if include_facts:
            facts = self.get_user_facts(user_id)
            if facts:
                fact_strs = [f"{f['fact_type']}: {f['fact_value']}" for f in facts[:5]]
                parts.append(f"Known about {user_name}: " + "; ".join(fact_strs))

        # Node info (natural language to avoid LLM echoing labels)
        if include_node:
            node = self.get_node(user_id)
            if node:
                node_info = []
                if node.get('hw_model'):
                    node_info.append(f"a {node['hw_model']}")
                if node.get('battery_level'):
                    node_info.append(f"battery at {node['battery_level']}%")
                if node.get('latitude') and node.get('longitude'):
                    node_info.append("has GPS position")
                if node.get('times_heard'):
                    node_info.append(f"heard {node['times_heard']} times")
                if node.get('role'):
                    node_info.append(f"role is {node['role']}")
                if node.get('uptime_seconds'):
                    hours = node['uptime_seconds'] // 3600
                    node_info.append(f"uptime {hours}h")
                if node_info:
                    parts.append(f"Their device is " + ", ".join(node_info))

        # Latest telemetry
        if include_telemetry:
            try:
                telemetry = self.get_telemetry_history(user_id, limit=1)
                if telemetry:
                    t = telemetry[0]
                    tel_parts = []
                    if t.get('temperature'):
                        tel_parts.append(f"{t['temperature']}C")
                    if t.get('relative_humidity'):
                        tel_parts.append(f"{t['relative_humidity']}% humidity")
                    if t.get('channel_utilization'):
                        tel_parts.append(f"channel util {t['channel_utilization']}%")
                    if tel_parts:
                        parts.append("Latest sensor readings: " + ", ".join(tel_parts))
            except:
                pass

        # Network health summary
        if include_health:
            try:
                conn = self._get_conn()
                cursor = conn.cursor()
                total = cursor.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
                cutoff = int(time.time()) - (24 * 3600)
                active = cursor.execute('SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,)).fetchone()[0]
                parts.append(f"The mesh has {active} of {total} nodes active in the last 24h")
            except:
                pass

        # User's message stats
        if include_stats:
            try:
                from datetime import timedelta as td
                conn = self._get_conn()
                cursor = conn.cursor()
                user_msg_count = cursor.execute(
                    'SELECT COUNT(*) FROM messages WHERE from_id = ?', (user_id,)
                ).fetchone()[0]
                two_hours_ago = (datetime.now() - td(hours=2)).isoformat()
                recent_count = cursor.execute(
                    'SELECT COUNT(*) FROM messages WHERE from_id = ? AND timestamp > ?',
                    (user_id, two_hours_ago)
                ).fetchone()[0]
                parts.append(f"This user has sent {user_msg_count} total messages, {recent_count} in the last 2 hours")
            except:
                pass

        # Recent detection alerts
        if include_alerts:
            try:
                alerts = self.get_detection_alerts(limit=3)
                if alerts:
                    alert_strs = [f"{a.get('sensor_name', a['from_id'])}: {a['alert_text']}" for a in alerts]
                    parts.append("Recent alerts: " + "; ".join(alert_strs))
            except:
                pass

        # Conversation history — always included
        history = self.get_conversation_history(user_id, limit=4)
        if history:
            conv_parts = []
            for msg in history:
                if msg.get('from_name') == 'assistant' or msg.get('is_outgoing'):
                    conv_parts.append(f"You: {msg['text']}")
                else:
                    conv_parts.append(f"{msg.get('from_name', 'User')}: {msg['text']}")
            parts.append("Recent conversation:\n" + "\n".join(conv_parts))

        # Trim to ~2000 chars
        context = "\n\n".join(parts) if parts else ""
        if len(context) > 2000:
            # Trim conversation history first
            while len(context) > 2000 and history and len(history) > 2:
                history.pop(0)
                conv_parts = []
                for msg in history:
                    if msg.get('from_name') == 'assistant' or msg.get('is_outgoing'):
                        conv_parts.append(f"You: {msg['text']}")
                    else:
                        conv_parts.append(f"{msg.get('from_name', 'User')}: {msg['text']}")
                parts[-1] = "Recent conversation:\n" + "\n".join(conv_parts)
                context = "\n\n".join(parts)

        return context

    def build_network_summary_for_llm(self) -> str:
        """Build a network-wide summary for LLM context on mesh/network questions."""
        parts = []

        try:
            conn = self._get_conn()
            cursor = conn.cursor()

            # Node counts
            total = cursor.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
            cutoff = int(time.time()) - (24 * 3600)
            active = cursor.execute('SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,)).fetchone()[0]
            with_gps = cursor.execute(
                'SELECT COUNT(*) FROM nodes WHERE latitude IS NOT NULL AND longitude IS NOT NULL'
            ).fetchone()[0]
            parts.append(f"Mesh network: {total} total nodes ({active} active in last 24h, {with_gps} with GPS)")

            # Hop distribution
            cursor.execute('''
                SELECT
                    SUM(CASE WHEN hops_away IS NULL OR hops_away = 0 THEN 1 ELSE 0 END) as direct,
                    SUM(CASE WHEN hops_away = 1 THEN 1 ELSE 0 END) as h1,
                    SUM(CASE WHEN hops_away = 2 THEN 1 ELSE 0 END) as h2,
                    SUM(CASE WHEN hops_away >= 3 THEN 1 ELSE 0 END) as h3
                FROM nodes
            ''')
            hop = cursor.fetchone()
            if hop:
                parts.append(f"Hops: {hop[0]} direct, {hop[1]} 1-hop, {hop[2]} 2-hop, {hop[3]} 3+hop")

            # Channel utilization
            row = cursor.execute('''
                SELECT AVG(channel_utilization) FROM nodes
                WHERE channel_utilization IS NOT NULL AND channel_utilization > 0
            ''').fetchone()
            if row and row[0]:
                parts.append(f"Avg channel utilization: {round(row[0], 1)}%")

            # Traffic stats
            msg_count = cursor.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
            pkt_count = cursor.execute('SELECT COUNT(*) FROM raw_packets').fetchone()[0]
            parts.append(f"Traffic: {msg_count} text msgs, {pkt_count} total packets")

            # Recent traceroutes
            traceroutes = self.get_traceroutes(limit=3)
            if traceroutes:
                tr_parts = []
                for tr in traceroutes:
                    route = tr.get('route', [])
                    if isinstance(route, list) and route:
                        route_str = ' -> '.join(str(r) for r in route)
                        tr_parts.append(f"{tr['from_id']} -> {route_str} -> {tr['to_id']}")
                if tr_parts:
                    parts.append("Recent routes: " + "; ".join(tr_parts))

            # Store & Forward status
            sf_stats = self.get_store_forward_stats()
            if sf_stats:
                sf_parts = []
                for sf in sf_stats[:2]:
                    if sf.get('messages_saved'):
                        sf_parts.append(f"{sf['from_id']}: {sf['messages_saved']} msgs stored")
                if sf_parts:
                    parts.append("Store&Forward: " + ", ".join(sf_parts))

        except Exception as e:
            logger.error(f"[DB] Error building network summary: {e}")

        return "\n".join(parts) if parts else ""

    # ==================== STATISTICS ====================

    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics."""
        conn = self._get_conn()
        cursor = conn.cursor()

        stats = {}

        # Count all tables
        tables = [
            ('raw_packets', 'total_packets'),
            ('messages', 'total_messages'),
            ('nodes', 'total_nodes'),
            ('user_facts', 'total_facts'),
            ('global_context', 'global_context_items'),
            ('telemetry', 'telemetry_records'),
            ('positions', 'position_records'),
            ('routing', 'routing_records'),
            ('neighbors', 'neighbor_records'),
            ('filtered_content', 'filtered_messages'),
            ('sent_messages', 'sent_messages'),
            ('waypoints', 'waypoint_records'),
            ('traceroutes', 'traceroute_records'),
            ('store_forward', 'store_forward_records'),
            ('range_tests', 'range_test_records'),
            ('detection_sensor', 'detection_alerts'),
            ('paxcounter', 'paxcounter_records'),
        ]

        for table, stat_name in tables:
            try:
                cursor.execute(f'SELECT COUNT(*) FROM {table}')
                stats[stat_name] = cursor.fetchone()[0]
            except:
                stats[stat_name] = 0

        # Users with facts
        cursor.execute('SELECT COUNT(DISTINCT user_id) FROM user_facts')
        stats['users_with_facts'] = cursor.fetchone()[0]

        # Active nodes (last 24h)
        cutoff = int(time.time()) - (24 * 3600)
        cursor.execute('SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,))
        stats['active_nodes_24h'] = cursor.fetchone()[0]

        # Packet type breakdown
        cursor.execute('''
            SELECT packet_type, COUNT(*) as count
            FROM raw_packets
            GROUP BY packet_type
            ORDER BY count DESC
            LIMIT 10
        ''')
        stats['packet_types'] = {row['packet_type']: row['count'] for row in cursor.fetchall()}

        # Database file size
        db_size = Path(self.db_path).stat().st_size if Path(self.db_path).exists() else 0
        stats['database_size_mb'] = round(db_size / (1024 * 1024), 2)

        return stats

    # ==================== CLEANUP ====================

    def clear_all(self):
        """Clear all data (use with caution!)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        tables = ['raw_packets', 'messages', 'nodes', 'user_facts', 'global_context',
                  'telemetry', 'positions', 'routing', 'neighbors', 'waypoints',
                  'traceroutes', 'filtered_content', 'sent_messages',
                  'store_forward', 'range_tests', 'detection_sensor', 'paxcounter']

        for table in tables:
            cursor.execute(f'DELETE FROM {table}')

        conn.commit()
        logger.warning("[DB] All database data cleared!")

    def vacuum(self):
        """Optimize database file size."""
        conn = self._get_conn()
        conn.execute('VACUUM')
        logger.info("[DB] Database vacuumed")

    def close(self):
        """Close database connection."""
        if hasattr(self._local, 'conn') and self._local.conn:
            self._local.conn.close()
            self._local.conn = None
            logger.info("[DB] Database connection closed")

    # ==================== OUTBOX (for dashboard messaging) ====================

    def add_to_outbox(self, message: str, destination: str = "^all", channel: int = 0, msg_type: str = "text") -> int:
        """Add a message to the outbox for the bridge to send.

        Args:
            message: Text to send.
            destination: Node ID or '^all' for broadcast.
            channel: Channel index (0 = primary).
            msg_type: 'text' for broadcast/channel, 'dm' for PKC-encrypted DM.
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO pending_outbox (message, destination, channel, status, msg_type)
            VALUES (?, ?, ?, 'pending', ?)
        ''', (message, destination, channel, msg_type))

        conn.commit()
        msg_id = cursor.lastrowid
        self._update_last_modified()
        logger.info(f"[DB] Added {msg_type} message to outbox: id={msg_id}, dest={destination}")
        return msg_id

    def add_traceroute_request(self, destination: str) -> int:
        """Queue a traceroute request to be processed by the bridge."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT INTO pending_outbox (message, destination, channel, status, msg_type)
            VALUES ('traceroute', ?, 0, 'pending', 'traceroute')
        ''', (destination,))

        conn.commit()
        msg_id = cursor.lastrowid
        self._update_last_modified()
        logger.info(f"[DB] Added traceroute request to outbox: id={msg_id}, dest={destination}")
        return msg_id

    def get_pending_outbox(self) -> List[Dict]:
        """Get all pending outbox messages."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT * FROM pending_outbox WHERE status = 'pending' ORDER BY created_at ASC
        ''')

        return [dict(row) for row in cursor.fetchall()]

    def mark_outbox_sent(self, msg_id: int):
        """Mark an outbox message as sent."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE pending_outbox SET status = 'sent', sent_at = ? WHERE id = ?
        ''', (datetime.now().isoformat(), msg_id))

        conn.commit()
        self._update_last_modified()

    def mark_outbox_failed(self, msg_id: int, error: str):
        """Mark an outbox message as failed."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            UPDATE pending_outbox SET status = 'failed', error = ? WHERE id = ?
        ''', (error, msg_id))

        conn.commit()
        self._update_last_modified()

    def clear_old_outbox(self, hours: int = 24):
        """Clear sent/failed outbox messages older than N hours."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            DELETE FROM pending_outbox
            WHERE status IN ('sent', 'failed')
            AND created_at < datetime('now', '-' || ? || ' hours')
        ''', (hours,))

        conn.commit()

    # ==================== DB METADATA (for change tracking) ====================

    def _update_last_modified(self):
        """Update the last_modified timestamp."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('''
            INSERT OR REPLACE INTO db_meta (key, value) VALUES ('last_updated', ?)
        ''', (str(time.time()),))

        conn.commit()

    def get_last_modified(self) -> float:
        """Get the last modification timestamp."""
        conn = self._get_conn()
        cursor = conn.cursor()

        cursor.execute('SELECT value FROM db_meta WHERE key = ?', ('last_updated',))
        row = cursor.fetchone()

        if row:
            try:
                return float(row[0])
            except:
                return 0.0
        return 0.0

    # ==================== HISTORICAL DATA QUERIES ====================

    def get_time_range(self, days: int = 7) -> Dict:
        """Get the time range of available data (limited to N days)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Calculate the cutoff time (N days ago)
        cutoff = datetime.now().timestamp() - (days * 24 * 3600)
        cutoff_iso = datetime.fromtimestamp(cutoff).isoformat()

        # Find earliest data point within the range
        cursor.execute('''
            SELECT MIN(timestamp) as earliest FROM (
                SELECT timestamp FROM messages WHERE timestamp >= ?
                UNION ALL
                SELECT timestamp FROM positions WHERE timestamp >= ?
                UNION ALL
                SELECT timestamp FROM raw_packets WHERE timestamp >= ?
            )
        ''', (cutoff_iso, cutoff_iso, cutoff_iso))

        row = cursor.fetchone()
        earliest = row['earliest'] if row and row['earliest'] else cutoff_iso

        return {
            'earliest': earliest,
            'latest': datetime.now().isoformat(),
            'days': days
        }

    def get_messages_before(self, timestamp: str, limit: int = 50) -> List[Dict]:
        """Get messages up to a specific timestamp."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Get received messages before timestamp
        cursor.execute('''
            SELECT timestamp, from_id, from_name, to_id, text, snr, rssi, channel
            FROM messages
            WHERE timestamp <= ?
            ORDER BY timestamp DESC
            LIMIT ?
        ''', (timestamp, limit))

        messages = []
        for row in cursor.fetchall():
            messages.append({
                'timestamp': row['timestamp'],
                'from_id': row['from_id'],
                'from_name': row['from_name'],
                'to_id': row['to_id'],
                'text': row['text'],
                'snr': row['snr'],
                'rssi': row['rssi'],
                'channel': row['channel'],
                'is_sent': False
            })

        # Get sent messages before timestamp
        cursor.execute('''
            SELECT timestamp, to_id, channel, text
            FROM sent_messages
            WHERE timestamp <= ?
            ORDER BY timestamp DESC
            LIMIT ?
        ''', (timestamp, limit))

        for row in cursor.fetchall():
            messages.append({
                'timestamp': row['timestamp'],
                'from_id': 'self',
                'from_name': 'Me',
                'to_id': row['to_id'],
                'text': row['text'],
                'snr': None,
                'rssi': None,
                'channel': row['channel'],
                'is_sent': True
            })

        # Sort combined list by timestamp descending
        messages.sort(key=lambda x: x['timestamp'], reverse=True)
        return messages[:limit]

    def get_nodes_at_time(self, timestamp: str) -> List[Dict]:
        """
        Get node states as they were at a specific time.
        For each node, gets the last known position before the timestamp.
        Only returns nodes that had activity (message or position) before that time.
        """
        conn = self._get_conn()
        cursor = conn.cursor()

        # Get only nodes that had activity before the timestamp
        cursor.execute('''
            SELECT DISTINCT n.node_id, n.long_name, n.short_name, n.hw_model, n.hops_away
            FROM nodes n
            WHERE EXISTS (
                SELECT 1 FROM messages m WHERE m.from_id = n.node_id AND m.timestamp <= ?
            ) OR EXISTS (
                SELECT 1 FROM positions p WHERE p.node_id = n.node_id AND p.timestamp <= ?
            )
        ''', (timestamp, timestamp))
        nodes_base = {row['node_id']: dict(row) for row in cursor.fetchall()}

        # For each node, get last position before timestamp
        cursor.execute('''
            SELECT p1.*
            FROM positions p1
            INNER JOIN (
                SELECT node_id, MAX(timestamp) as max_ts
                FROM positions
                WHERE timestamp <= ?
                GROUP BY node_id
            ) p2 ON p1.node_id = p2.node_id AND p1.timestamp = p2.max_ts
        ''', (timestamp,))

        positions = {row['node_id']: dict(row) for row in cursor.fetchall()}

        # Get last telemetry (battery, etc) before timestamp
        cursor.execute('''
            SELECT t1.*
            FROM telemetry t1
            INNER JOIN (
                SELECT node_id, MAX(timestamp) as max_ts
                FROM telemetry
                WHERE timestamp <= ? AND telemetry_type = 'device'
                GROUP BY node_id
            ) t2 ON t1.node_id = t2.node_id AND t1.timestamp = t2.max_ts
        ''', (timestamp,))

        telemetry = {}
        for row in cursor.fetchall():
            try:
                data = json.loads(row['raw_data']) if row['raw_data'] else {}
                device_metrics = data.get('deviceMetrics', {})
                telemetry[row['node_id']] = {
                    'battery_level': device_metrics.get('batteryLevel'),
                    'voltage': device_metrics.get('voltage'),
                    'channel_utilization': device_metrics.get('channelUtilization'),
                    'air_util_tx': device_metrics.get('airUtilTx')
                }
            except:
                pass

        # Get last SNR/RSSI from messages before timestamp
        cursor.execute('''
            SELECT m1.from_id, m1.snr, m1.rssi
            FROM messages m1
            INNER JOIN (
                SELECT from_id, MAX(timestamp) as max_ts
                FROM messages
                WHERE timestamp <= ? AND snr IS NOT NULL
                GROUP BY from_id
            ) m2 ON m1.from_id = m2.from_id AND m1.timestamp = m2.max_ts
        ''', (timestamp,))

        signal_data = {row['from_id']: {'snr': row['snr'], 'rssi': row['rssi']}
                       for row in cursor.fetchall()}

        # Get calculated hops from messages (hop_start - hop_limit) before timestamp
        cursor.execute('''
            SELECT m1.from_id, (m1.hop_start - m1.hop_limit) as hops_used
            FROM messages m1
            INNER JOIN (
                SELECT from_id, MAX(timestamp) as max_ts
                FROM messages
                WHERE timestamp <= ? AND hop_start > 0
                GROUP BY from_id
            ) m2 ON m1.from_id = m2.from_id AND m1.timestamp = m2.max_ts
        ''', (timestamp,))

        hop_data = {row['from_id']: row['hops_used'] for row in cursor.fetchall()}

        # Build combined node list
        result = []
        for node_id, node in nodes_base.items():
            pos = positions.get(node_id, {})
            tel = telemetry.get(node_id, {})
            sig = signal_data.get(node_id, {})

            result.append({
                'node_id': node_id,
                'long_name': node.get('long_name'),
                'short_name': node.get('short_name'),
                'hw_model': node.get('hw_model'),
                'hops_away': node.get('hops_away'),
                'hops_used': hop_data.get(node_id),
                'latitude': pos.get('latitude'),
                'longitude': pos.get('longitude'),
                'altitude': pos.get('altitude'),
                'last_heard': pos.get('timestamp'),
                'battery_level': tel.get('battery_level'),
                'voltage': tel.get('voltage'),
                'snr': sig.get('snr'),
                'rssi': sig.get('rssi')
            })

        return result

    def get_stats_at_time(self, timestamp: str) -> Dict:
        """Get stats calculated up to a specific time (matching live get_stats keys)."""
        conn = self._get_conn()
        cursor = conn.cursor()

        # Count messages up to timestamp
        cursor.execute('SELECT COUNT(*) FROM messages WHERE timestamp <= ?', (timestamp,))
        total_messages = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM sent_messages WHERE timestamp <= ?', (timestamp,))
        sent_messages = cursor.fetchone()[0]

        # Count packets up to timestamp
        cursor.execute('SELECT COUNT(*) FROM raw_packets WHERE timestamp <= ?', (timestamp,))
        total_packets = cursor.fetchone()[0]

        # Count total unique nodes seen up to timestamp
        cursor.execute('''
            SELECT COUNT(DISTINCT node_id) FROM nodes
            WHERE first_seen <= ? OR last_updated <= ?
        ''', (timestamp, timestamp))
        total_nodes = cursor.fetchone()[0]

        # Count active nodes (seen in last 24h relative to the timestamp)
        cursor.execute('''
            SELECT COUNT(DISTINCT from_id) FROM messages
            WHERE timestamp <= ? AND timestamp > datetime(?, '-24 hours')
        ''', (timestamp, timestamp))
        active_nodes_24h = cursor.fetchone()[0]

        # Packet type breakdown up to timestamp
        cursor.execute('''
            SELECT packet_type, COUNT(*) as count
            FROM raw_packets
            WHERE timestamp <= ?
            GROUP BY packet_type
            ORDER BY count DESC
            LIMIT 10
        ''', (timestamp,))
        packet_types = {}
        for row in cursor.fetchall():
            packet_types[row['packet_type']] = row['count']

        cursor.execute('SELECT COUNT(*) FROM telemetry WHERE timestamp <= ?', (timestamp,))
        telemetry_records = cursor.fetchone()[0]

        cursor.execute('SELECT COUNT(*) FROM positions WHERE timestamp <= ?', (timestamp,))
        position_records = cursor.fetchone()[0]

        return {
            'total_messages': total_messages,
            'sent_messages': sent_messages,
            'total_packets': total_packets,
            'total_nodes': total_nodes,
            'active_nodes_24h': active_nodes_24h,
            'packet_types': packet_types,
            'telemetry_records': telemetry_records,
            'position_records': position_records,
            'as_of': timestamp
        }
