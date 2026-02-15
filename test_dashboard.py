"""
Tests to validate dashboard API responses against the SQLite database.

Run: python -m pytest test_dashboard.py -v
"""

import json
import sqlite3
import time
import sys
import os
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

DB_PATH = os.path.join(os.path.dirname(__file__), 'mesh_data.db')


# ── Fixtures ─────────────────────────────────────────────────────────

@pytest.fixture(scope='session')
def db_conn():
    """Direct SQLite connection for ground-truth queries."""
    if not os.path.exists(DB_PATH):
        pytest.skip('mesh_data.db not found — run the bridge first')
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture(scope='session')
def client():
    """Flask test client."""
    import dashboard
    dashboard.app.config['TESTING'] = True
    with dashboard.app.test_client() as c:
        yield c


# ── Helpers ──────────────────────────────────────────────────────────

def api_json(client, url):
    resp = client.get(url)
    assert resp.status_code == 200, f'{url} returned {resp.status_code}'
    return json.loads(resp.data)


# ── /api/stats-enhanced ──────────────────────────────────────────────

class TestStatsEnhanced:

    def test_total_nodes_matches_db(self, client, db_conn):
        data = api_json(client, '/api/stats-enhanced?range=all')
        db_total = db_conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        assert data['total_nodes'] == db_total, (
            f"API total_nodes={data['total_nodes']} != DB={db_total}"
        )

    def test_active_nodes_all_range(self, client, db_conn):
        """When range=all, active_nodes should equal total_nodes."""
        data = api_json(client, '/api/stats-enhanced?range=all')
        assert data['active_nodes'] == data['total_nodes']

    def test_active_nodes_24h(self, client, db_conn):
        data = api_json(client, '/api/stats-enhanced?range=24h')
        cutoff = int(time.time()) - (24 * 3600)
        db_active = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,)
        ).fetchone()[0]
        assert data['active_nodes'] == db_active, (
            f"API active_nodes={data['active_nodes']} != DB={db_active}"
        )

    def test_total_messages_matches_db(self, client, db_conn):
        data = api_json(client, '/api/stats-enhanced?range=all')
        db_msgs = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        assert data['all_time_messages'] == db_msgs, (
            f"API all_time_messages={data['all_time_messages']} != DB={db_msgs}"
        )

    def test_sent_messages_matches_db(self, client, db_conn):
        data = api_json(client, '/api/stats-enhanced?range=all')
        db_sent = db_conn.execute('SELECT COUNT(*) FROM sent_messages').fetchone()[0]
        assert data['all_time_sent'] == db_sent, (
            f"API all_time_sent={data['all_time_sent']} != DB={db_sent}"
        )

    def test_total_packets_matches_db(self, client, db_conn):
        data = api_json(client, '/api/stats-enhanced?range=all')
        db_pkts = db_conn.execute('SELECT COUNT(*) FROM raw_packets').fetchone()[0]
        assert data['total_packets'] == db_pkts, (
            f"API total_packets={data['total_packets']} != DB={db_pkts}"
        )

    def test_telemetry_matches_db(self, client, db_conn):
        data = api_json(client, '/api/stats-enhanced?range=all')
        db_tel = db_conn.execute('SELECT COUNT(*) FROM telemetry').fetchone()[0]
        assert data['telemetry_records'] == db_tel

    def test_position_matches_db(self, client, db_conn):
        data = api_json(client, '/api/stats-enhanced?range=all')
        db_pos = db_conn.execute('SELECT COUNT(*) FROM positions').fetchone()[0]
        assert data['position_records'] == db_pos

    def test_routing_matches_db(self, client, db_conn):
        data = api_json(client, '/api/stats-enhanced?range=all')
        db_route = db_conn.execute('SELECT COUNT(*) FROM routing').fetchone()[0]
        assert data['routing_records'] == db_route

    def test_hop_distribution_sums_within_range(self, client, db_conn):
        """Hop distribution may exclude nodes with NULL hops, so sum <= total."""
        data = api_json(client, '/api/stats-enhanced?range=all')
        hops = data.get('hop_distribution', {})
        if hops:
            # Exclude the 'total' summary key — only sum the individual hop buckets
            hop_sum = sum(v for k, v in hops.items() if k != 'total')
            db_total = db_conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
            assert hop_sum <= db_total, (
                f"Hop distribution sum={hop_sum} > total nodes={db_total}"
            )

    def test_stats_range_filtering(self, client, db_conn):
        """Filtered message count should be <= all-time count."""
        all_data = api_json(client, '/api/stats-enhanced?range=all')
        day_data = api_json(client, '/api/stats-enhanced?range=24h')
        assert day_data['total_messages'] <= all_data['total_messages']


# ── /api/nodes ───────────────────────────────────────────────────────

class TestNodes:

    def test_all_nodes_count_matches_db(self, client, db_conn):
        nodes = api_json(client, '/api/nodes?range=all')
        db_count = db_conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        assert len(nodes) == db_count, (
            f"API returned {len(nodes)} nodes, DB has {db_count}"
        )

    def test_24h_nodes_count_matches_db(self, client, db_conn):
        nodes = api_json(client, '/api/nodes?range=24h')
        cutoff = int(time.time()) - (24 * 3600)
        db_count = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,)
        ).fetchone()[0]
        assert len(nodes) == db_count, (
            f"API returned {len(nodes)} 24h nodes, DB has {db_count}"
        )

    def test_nodes_have_required_fields(self, client):
        nodes = api_json(client, '/api/nodes?range=all')
        if nodes:
            required = {'node_id', 'long_name', 'short_name', 'last_heard'}
            first = nodes[0]
            for field in required:
                assert field in first, f"Missing field: {field}"

    def test_nodes_with_gps_count(self, client, db_conn):
        nodes = api_json(client, '/api/nodes?range=all')
        api_gps = sum(1 for n in nodes if n.get('latitude') and n.get('longitude'))
        db_gps = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE latitude IS NOT NULL AND longitude IS NOT NULL'
        ).fetchone()[0]
        assert api_gps == db_gps, (
            f"API GPS nodes={api_gps} != DB={db_gps}"
        )

    def test_node_ids_match_db(self, client, db_conn):
        nodes = api_json(client, '/api/nodes?range=all')
        api_ids = {n['node_id'] for n in nodes}
        rows = db_conn.execute('SELECT node_id FROM nodes').fetchall()
        db_ids = {r[0] for r in rows}
        assert api_ids == db_ids, (
            f"Missing from API: {db_ids - api_ids}, Extra in API: {api_ids - db_ids}"
        )

    def test_7d_subset_of_all(self, client):
        all_nodes = api_json(client, '/api/nodes?range=all')
        week_nodes = api_json(client, '/api/nodes?range=7d')
        assert len(week_nodes) <= len(all_nodes)

    def test_1h_subset_of_24h(self, client):
        day_nodes = api_json(client, '/api/nodes?range=24h')
        hour_nodes = api_json(client, '/api/nodes?range=1h')
        assert len(hour_nodes) <= len(day_nodes)


# ── /api/messages ────────────────────────────────────────────────────

class TestMessages:

    def test_all_messages_count(self, client, db_conn):
        """API combines received (excluding assistant) + sent, capped by limit."""
        msgs = api_json(client, '/api/messages?limit=9999&range=all')
        db_received = db_conn.execute(
            "SELECT COUNT(*) FROM messages WHERE from_id != 'assistant'"
        ).fetchone()[0]
        db_sent = db_conn.execute('SELECT COUNT(*) FROM sent_messages').fetchone()[0]
        db_total = db_received + db_sent
        assert len(msgs) == db_total, (
            f"API returned {len(msgs)} messages, DB has {db_received} received + {db_sent} sent = {db_total}"
        )

    def test_received_messages_have_required_fields(self, client):
        msgs = api_json(client, '/api/messages?limit=50&range=all')
        received = [m for m in msgs if m.get('direction') == 'received']
        if received:
            required = {'timestamp', 'from_name', 'text', 'direction'}
            for field in required:
                assert field in received[0], f"Missing field in received msg: {field}"

    def test_sent_messages_have_required_fields(self, client):
        msgs = api_json(client, '/api/messages?limit=50&range=all')
        sent = [m for m in msgs if m.get('direction') == 'sent']
        if sent:
            required = {'timestamp', 'text', 'direction', 'channel'}
            for field in required:
                assert field in sent[0], f"Missing field in sent msg: {field}"

    def test_messages_sorted_by_time(self, client):
        msgs = api_json(client, '/api/messages?limit=50&range=all')
        if len(msgs) > 1:
            timestamps = [m['timestamp'] for m in msgs]
            assert timestamps == sorted(timestamps, reverse=True), (
                "Messages not sorted by timestamp descending"
            )

    def test_limit_parameter(self, client):
        msgs_5 = api_json(client, '/api/messages?limit=5&range=all')
        msgs_10 = api_json(client, '/api/messages?limit=10&range=all')
        assert len(msgs_5) <= 5
        assert len(msgs_10) <= 10

    def test_24h_subset_of_all(self, client):
        all_msgs = api_json(client, '/api/messages?limit=9999&range=all')
        day_msgs = api_json(client, '/api/messages?limit=9999&range=24h')
        assert len(day_msgs) <= len(all_msgs)


# ── /api/stats ───────────────────────────────────────────────────────

class TestBasicStats:

    def test_stats_returns_200(self, client):
        resp = client.get('/api/stats')
        assert resp.status_code == 200

    def test_stats_has_key_fields(self, client):
        data = api_json(client, '/api/stats')
        for key in ['total_messages', 'total_nodes', 'total_packets']:
            assert key in data, f"Missing key: {key}"

    def test_stats_node_count(self, client, db_conn):
        data = api_json(client, '/api/stats')
        db_count = db_conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        assert data['total_nodes'] == db_count


# ── /api/topology ────────────────────────────────────────────────────

class TestTopology:

    def test_topology_structure(self, client):
        data = api_json(client, '/api/topology')
        assert 'nodes' in data
        assert 'edges' in data
        assert isinstance(data['nodes'], list)
        assert isinstance(data['edges'], list)

    def test_topology_nodes_subset_of_db(self, client, db_conn):
        data = api_json(client, '/api/topology')
        db_count = db_conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        assert len(data['nodes']) <= db_count


# ── /api/telemetry-history ───────────────────────────────────────────

class TestTelemetry:

    def test_telemetry_returns_list(self, client):
        data = api_json(client, '/api/telemetry-history')
        assert isinstance(data, list)

    def test_telemetry_count_bounded(self, client, db_conn):
        data = api_json(client, '/api/telemetry-history?limit=100')
        assert len(data) <= 100


# ── /api/position-trail ──────────────────────────────────────────────

class TestPositions:

    def test_positions_returns_list(self, client):
        data = api_json(client, '/api/position-trail')
        assert isinstance(data, list)

    def test_positions_have_coords(self, client):
        data = api_json(client, '/api/position-trail?limit=5')
        for pos in data:
            assert 'latitude' in pos
            assert 'longitude' in pos


# ── /api/traceroutes ─────────────────────────────────────────────────

class TestTraceroutes:

    def test_traceroutes_returns_list(self, client):
        data = api_json(client, '/api/traceroutes')
        assert isinstance(data, list)

    def test_traceroutes_count_matches_db(self, client, db_conn):
        data = api_json(client, '/api/traceroutes?limit=9999')
        db_count = db_conn.execute('SELECT COUNT(*) FROM traceroutes').fetchone()[0]
        assert len(data) == db_count


# ── /api/waypoints ───────────────────────────────────────────────────

class TestWaypoints:

    def test_waypoints_returns_list(self, client):
        data = api_json(client, '/api/waypoints')
        assert isinstance(data, list)


# ── /api/time-range ──────────────────────────────────────────────────

class TestTimeRange:

    def test_time_range_structure(self, client):
        data = api_json(client, '/api/time-range')
        assert 'earliest' in data
        assert 'latest' in data
        assert 'days' in data


# ── /api/check-updates ──────────────────────────────────────────────

class TestCheckUpdates:

    def test_check_updates_structure(self, client):
        data = api_json(client, '/api/check-updates?since=0')
        assert 'has_updates' in data
        assert 'last_update' in data


# ── /api/activity ────────────────────────────────────────────────────

class TestActivity:

    def test_activity_returns_list(self, client):
        data = api_json(client, '/api/activity')
        assert isinstance(data, list)

    def test_activity_entries_have_hour_and_count(self, client):
        data = api_json(client, '/api/activity')
        for entry in data:
            assert 'hour' in entry
            assert 'count' in entry


# ── /api/paxcounter, range-tests, detection-alerts, store-forward ────

class TestSpecializedEndpoints:

    def test_paxcounter_returns_list(self, client):
        data = api_json(client, '/api/paxcounter')
        assert isinstance(data, list)

    def test_range_tests_returns_list(self, client):
        data = api_json(client, '/api/range-tests')
        assert isinstance(data, list)

    def test_detection_alerts_returns_list(self, client):
        data = api_json(client, '/api/detection-alerts')
        assert isinstance(data, list)

    def test_store_forward_returns_list(self, client):
        data = api_json(client, '/api/store-forward-stats')
        assert isinstance(data, list)

    def test_paxcounter_count_matches_db(self, client, db_conn):
        data = api_json(client, '/api/paxcounter?limit=9999')
        db_count = db_conn.execute('SELECT COUNT(*) FROM paxcounter').fetchone()[0]
        assert len(data) == db_count

    def test_range_tests_count_matches_db(self, client, db_conn):
        data = api_json(client, '/api/range-tests?limit=9999')
        db_count = db_conn.execute('SELECT COUNT(*) FROM range_tests').fetchone()[0]
        assert len(data) == db_count

    def test_detection_alerts_count_matches_db(self, client, db_conn):
        data = api_json(client, '/api/detection-alerts?limit=9999')
        db_count = db_conn.execute('SELECT COUNT(*) FROM detection_sensor').fetchone()[0]
        assert len(data) == db_count


# ── Cross-validation: API consistency ────────────────────────────────

class TestCrossValidation:

    def test_stats_nodes_matches_nodes_endpoint(self, client):
        """Total nodes from /stats-enhanced should match /nodes count."""
        stats = api_json(client, '/api/stats-enhanced?range=all')
        nodes = api_json(client, '/api/nodes?range=all')
        assert stats['total_nodes'] == len(nodes), (
            f"/stats-enhanced total_nodes={stats['total_nodes']} != /nodes count={len(nodes)}"
        )

    def test_stats_messages_consistent(self, client, db_conn):
        """Stats message counts should match direct DB queries."""
        stats = api_json(client, '/api/stats-enhanced?range=all')
        db_msgs = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        db_sent = db_conn.execute('SELECT COUNT(*) FROM sent_messages').fetchone()[0]
        assert stats['all_time_messages'] == db_msgs, (
            f"stats all_time_messages={stats['all_time_messages']} != DB={db_msgs}"
        )
        assert stats['all_time_sent'] == db_sent, (
            f"stats all_time_sent={stats['all_time_sent']} != DB={db_sent}"
        )

    def test_position_trail_count_matches_db(self, client, db_conn):
        """Position trail results should come from the positions table."""
        positions = api_json(client, '/api/position-trail?limit=100')
        db_count = db_conn.execute('SELECT COUNT(*) FROM positions').fetchone()[0]
        assert len(positions) <= min(100, db_count)

    def test_telemetry_count_matches_db(self, client, db_conn):
        """Telemetry results should come from the telemetry table."""
        telemetry = api_json(client, '/api/telemetry-history?limit=100')
        db_count = db_conn.execute('SELECT COUNT(*) FROM telemetry').fetchone()[0]
        assert len(telemetry) <= min(100, db_count)


# ── Database schema validation ───────────────────────────────────────

class TestDatabaseSchema:

    EXPECTED_TABLES = [
        'raw_packets', 'messages', 'nodes', 'user_facts', 'global_context',
        'telemetry', 'positions', 'routing', 'neighbors', 'waypoints',
        'traceroutes', 'store_forward', 'range_tests', 'detection_sensor',
        'paxcounter', 'filtered_content', 'sent_messages', 'pending_outbox',
        'db_meta',
    ]

    def test_all_tables_exist(self, db_conn):
        rows = db_conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        tables = {r[0] for r in rows}
        for t in self.EXPECTED_TABLES:
            assert t in tables, f"Missing table: {t}"

    def test_nodes_table_columns(self, db_conn):
        rows = db_conn.execute('PRAGMA table_info(nodes)').fetchall()
        cols = {r[1] for r in rows}
        required = {'node_id', 'long_name', 'short_name', 'hw_model',
                     'last_heard', 'latitude', 'longitude', 'battery_level',
                     'hops_away', 'snr'}
        missing = required - cols
        assert not missing, f"nodes table missing columns: {missing}"

    def test_messages_table_columns(self, db_conn):
        rows = db_conn.execute('PRAGMA table_info(messages)').fetchall()
        cols = {r[1] for r in rows}
        required = {'id', 'timestamp', 'from_id', 'from_name', 'to_id',
                     'text', 'channel', 'snr', 'rssi', 'is_outgoing'}
        missing = required - cols
        assert not missing, f"messages table missing columns: {missing}"

    def test_wal_mode_enabled(self, db_conn):
        mode = db_conn.execute('PRAGMA journal_mode').fetchone()[0]
        assert mode == 'wal', f"Expected WAL mode, got: {mode}"

    def test_db_meta_has_last_updated(self, db_conn):
        row = db_conn.execute(
            "SELECT value FROM db_meta WHERE key='last_updated'"
        ).fetchone()
        assert row is not None, "db_meta missing last_updated key"
        val = float(row[0])
        assert val > 0, "last_updated should be a positive timestamp"
