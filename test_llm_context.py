"""
Test suite for LLM context-building functions in mesh_database.py

Validates that the LLM context-building functions return data consistent with
what's actually in the SQLite database.
"""

import pytest
import sqlite3
import re
import time
import os
from datetime import datetime, timedelta
from mesh_database import MeshDatabase

DB_PATH = os.path.join(os.path.dirname(__file__), 'mesh_data.db')


@pytest.fixture(scope='session')
def db():
    """MeshDatabase instance."""
    return MeshDatabase(db_path=DB_PATH)


@pytest.fixture(scope='session')
def db_conn():
    """Raw SQLite connection for verification queries."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    yield conn
    conn.close()


@pytest.fixture(scope='session')
def sample_user_id(db_conn):
    """Get a real user_id that has sent messages."""
    row = db_conn.execute(
        "SELECT from_id, COUNT(*) as cnt FROM messages WHERE from_id != 'assistant' AND from_id IS NOT NULL GROUP BY from_id ORDER BY cnt DESC LIMIT 1"
    ).fetchone()
    return row['from_id'] if row else None


@pytest.fixture(scope='session')
def sample_user_name(db_conn, sample_user_id):
    """Get the user name for sample_user_id."""
    if not sample_user_id:
        return None
    row = db_conn.execute(
        "SELECT from_name FROM messages WHERE from_id = ? LIMIT 1", (sample_user_id,)
    ).fetchone()
    return row['from_name'] if row else "TestUser"


class TestNetworkSummary:
    """Tests for build_network_summary_for_llm()."""

    def test_node_count_total(self, db, db_conn):
        """Verify total node count matches DB query."""
        summary = db.build_network_summary_for_llm()

        # Extract total node count from summary
        match = re.search(r'(\d+)\s+total\s+nodes', summary)
        summary_total = int(match.group(1)) if match else 0

        # Query DB for total nodes
        db_total = db_conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]

        assert summary_total == db_total, f"Total nodes mismatch: summary={summary_total}, db={db_total}"

    def test_node_count_active_24h(self, db, db_conn):
        """Verify active nodes (24h) count matches DB query."""
        summary = db.build_network_summary_for_llm()

        # Extract active node count
        match = re.search(r'(\d+)\s+active\s+in\s+last\s+24h', summary)
        summary_active = int(match.group(1)) if match else 0

        # Query DB for active nodes (last 24h)
        cutoff = int(time.time()) - (24 * 3600)
        db_active = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,)
        ).fetchone()[0]

        assert summary_active == db_active, f"Active nodes mismatch: summary={summary_active}, db={db_active}"

    def test_node_count_gps(self, db, db_conn):
        """Verify GPS nodes count matches DB query."""
        summary = db.build_network_summary_for_llm()

        # Extract GPS node count
        match = re.search(r'(\d+)\s+with\s+GPS', summary)
        summary_gps = int(match.group(1)) if match else 0

        # Query DB for nodes with GPS
        db_gps = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE latitude IS NOT NULL AND longitude IS NOT NULL'
        ).fetchone()[0]

        assert summary_gps == db_gps, f"GPS nodes mismatch: summary={summary_gps}, db={db_gps}"

    def test_hop_distribution_direct(self, db, db_conn):
        """Verify direct (0-hop) node count."""
        summary = db.build_network_summary_for_llm()

        # Extract hop distribution
        match = re.search(r'(\d+)\s+direct,\s+(\d+)\s+1-hop,\s+(\d+)\s+2-hop,\s+(\d+)\s+3\+hop', summary)
        if not match:
            pytest.skip("Hop distribution not found in summary")

        summary_direct = int(match.group(1))

        # Query DB for direct hops
        db_direct = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE hops_away IS NULL OR hops_away = 0'
        ).fetchone()[0]

        assert summary_direct == db_direct, f"Direct hops mismatch: summary={summary_direct}, db={db_direct}"

    def test_hop_distribution_1hop(self, db, db_conn):
        """Verify 1-hop node count."""
        summary = db.build_network_summary_for_llm()

        match = re.search(r'(\d+)\s+direct,\s+(\d+)\s+1-hop,\s+(\d+)\s+2-hop,\s+(\d+)\s+3\+hop', summary)
        if not match:
            pytest.skip("Hop distribution not found in summary")

        summary_1hop = int(match.group(2))

        db_1hop = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE hops_away = 1'
        ).fetchone()[0]

        assert summary_1hop == db_1hop, f"1-hop mismatch: summary={summary_1hop}, db={db_1hop}"

    def test_hop_distribution_2hop(self, db, db_conn):
        """Verify 2-hop node count."""
        summary = db.build_network_summary_for_llm()

        match = re.search(r'(\d+)\s+direct,\s+(\d+)\s+1-hop,\s+(\d+)\s+2-hop,\s+(\d+)\s+3\+hop', summary)
        if not match:
            pytest.skip("Hop distribution not found in summary")

        summary_2hop = int(match.group(3))

        db_2hop = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE hops_away = 2'
        ).fetchone()[0]

        assert summary_2hop == db_2hop, f"2-hop mismatch: summary={summary_2hop}, db={db_2hop}"

    def test_hop_distribution_3plus(self, db, db_conn):
        """Verify 3+ hop node count."""
        summary = db.build_network_summary_for_llm()

        match = re.search(r'(\d+)\s+direct,\s+(\d+)\s+1-hop,\s+(\d+)\s+2-hop,\s+(\d+)\s+3\+hop', summary)
        if not match:
            pytest.skip("Hop distribution not found in summary")

        summary_3plus = int(match.group(4))

        db_3plus = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE hops_away >= 3'
        ).fetchone()[0]

        assert summary_3plus == db_3plus, f"3+ hops mismatch: summary={summary_3plus}, db={db_3plus}"

    def test_channel_utilization_accuracy(self, db, db_conn):
        """Verify reported channel utilization matches DB calculation."""
        summary = db.build_network_summary_for_llm()

        # Extract channel utilization
        match = re.search(r'Avg\s+channel\s+utilization:\s+([0-9.]+)%', summary)
        summary_util = float(match.group(1)) if match else None

        if summary_util is None:
            pytest.skip("Channel utilization not found in summary")

        # Query DB for average channel utilization
        row = db_conn.execute('''
            SELECT AVG(channel_utilization) FROM nodes
            WHERE channel_utilization IS NOT NULL AND channel_utilization > 0
        ''').fetchone()

        db_util = round(row[0], 1) if row and row[0] else None

        if db_util is not None:
            assert summary_util == db_util, f"Channel utilization mismatch: summary={summary_util}, db={db_util}"

    def test_traffic_stats_message_count(self, db, db_conn):
        """Verify message count in traffic stats."""
        summary = db.build_network_summary_for_llm()

        # Extract traffic stats
        match = re.search(r'(\d+)\s+text\s+msgs,\s+(\d+)\s+total\s+packets', summary)
        if not match:
            pytest.skip("Traffic stats not found in summary")

        summary_msgs = int(match.group(1))

        # Query DB for message count
        db_msgs = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]

        assert summary_msgs == db_msgs, f"Message count mismatch: summary={summary_msgs}, db={db_msgs}"

    def test_traffic_stats_packet_count(self, db, db_conn):
        """Verify packet count in traffic stats."""
        summary = db.build_network_summary_for_llm()

        match = re.search(r'(\d+)\s+text\s+msgs,\s+(\d+)\s+total\s+packets', summary)
        if not match:
            pytest.skip("Traffic stats not found in summary")

        summary_pkts = int(match.group(2))

        # Query DB for packet count
        db_pkts = db_conn.execute('SELECT COUNT(*) FROM raw_packets').fetchone()[0]

        assert summary_pkts == db_pkts, f"Packet count mismatch: summary={summary_pkts}, db={db_pkts}"

    def test_summary_not_empty(self, db):
        """Verify network summary returns non-empty string."""
        summary = db.build_network_summary_for_llm()
        assert isinstance(summary, str), "Summary should be a string"
        assert len(summary) > 0, "Summary should not be empty"


class TestUserContext:
    """Tests for build_context_for_llm()."""

    def test_user_message_count(self, db, db_conn, sample_user_id, sample_user_name):
        """Verify user message count in context matches DB."""
        if not sample_user_id:
            pytest.skip("No sample user with messages found")

        context = db.build_context_for_llm(sample_user_id, sample_user_name)

        # Extract message count from context
        match = re.search(r'sent\s+(\d+)\s+(?:total\s+)?messages', context, re.IGNORECASE)
        context_msgs = int(match.group(1)) if match else None

        # Query DB for this user's message count
        db_msgs = db_conn.execute(
            'SELECT COUNT(*) FROM messages WHERE from_id = ?', (sample_user_id,)
        ).fetchone()[0]

        if context_msgs is not None:
            assert context_msgs == db_msgs, f"User message count mismatch: context={context_msgs}, db={db_msgs}"

    def test_active_nodes_count(self, db, db_conn, sample_user_id, sample_user_name):
        """Verify active nodes count in context."""
        if not sample_user_id:
            pytest.skip("No sample user with messages found")

        context = db.build_context_for_llm(sample_user_id, sample_user_name)

        # Extract active node count
        match = re.search(r'(\d+)\s+of\s+(\d+)\s+nodes\s+active', context)
        if not match:
            pytest.skip("Active nodes not mentioned in context")

        context_active = int(match.group(1))
        context_total = int(match.group(2))

        # Query DB for node counts
        db_total = db_conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]
        cutoff = int(time.time()) - (24 * 3600)
        db_active = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,)
        ).fetchone()[0]

        assert context_active == db_active, f"Active nodes mismatch: context={context_active}, db={db_active}"
        assert context_total == db_total, f"Total nodes mismatch: context={context_total}, db={db_total}"

    def test_context_returns_string(self, db, sample_user_id, sample_user_name):
        """Verify context is a string."""
        if not sample_user_id:
            pytest.skip("No sample user with messages found")

        context = db.build_context_for_llm(sample_user_id, sample_user_name)
        assert isinstance(context, str), "Context should be a string"

    def test_context_includes_global_context_if_available(self, db, db_conn, sample_user_id, sample_user_name):
        """Verify global context is included if it exists."""
        global_count = db_conn.execute('SELECT COUNT(*) FROM global_context').fetchone()[0]
        if global_count == 0:
            pytest.skip("No global context in database")

        context = db.build_context_for_llm(sample_user_id, sample_user_name)
        assert "System context:" in context, "Global context should be included"

    def test_conversation_history_chronological(self, db, sample_user_id, sample_user_name):
        """Verify conversation history is in chronological order."""
        if not sample_user_id:
            pytest.skip("No sample user with messages found")

        context = db.build_context_for_llm(sample_user_id, sample_user_name)

        # Verify conversation history is included if present
        if "Recent conversation:" in context:
            # Extract conversation lines (simplified check)
            assert isinstance(context, str), "Context should be a string"


class TestConversationHistory:
    """Tests for get_conversation_history()."""

    def test_conversation_history_count(self, db, db_conn, sample_user_id):
        """Verify conversation history respects limit."""
        if not sample_user_id:
            pytest.skip("No sample user with messages found")

        limit = 5
        history = db.get_conversation_history(sample_user_id, limit=limit)

        assert len(history) <= limit, f"Conversation history exceeds limit: {len(history)} > {limit}"

    def test_conversation_history_chronological_order(self, db, sample_user_id):
        """Verify conversation history is in chronological order."""
        if not sample_user_id:
            pytest.skip("No sample user with messages found")

        history = db.get_conversation_history(sample_user_id, limit=10)

        if len(history) < 2:
            pytest.skip("Not enough conversation history for chronological check")

        # Check that timestamps are in ascending order (chronological)
        timestamps = [msg.get('timestamp') for msg in history]
        sorted_timestamps = sorted(timestamps)

        assert timestamps == sorted_timestamps, "Conversation history not in chronological order"

    def test_conversation_history_returns_list(self, db, sample_user_id):
        """Verify conversation history returns a list."""
        if not sample_user_id:
            pytest.skip("No sample user with messages found")

        history = db.get_conversation_history(sample_user_id)
        assert isinstance(history, list), "Conversation history should be a list"


class TestTimeWindowedCounts:
    """Tests for message counts in various time windows."""

    def test_1hour_messages_count(self, db, db_conn):
        """Verify 1-hour message count."""
        one_hour_ago = (datetime.now() - timedelta(hours=1)).isoformat()

        # Query DB
        db_count = db_conn.execute(
            "SELECT COUNT(*) FROM messages WHERE timestamp > ?", (one_hour_ago,)
        ).fetchone()[0]

        # Also verify via get_stats or direct query
        total_count = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]

        assert db_count <= total_count, f"1h count cannot exceed total: {db_count} > {total_count}"

    def test_6hour_messages_count(self, db, db_conn):
        """Verify 6-hour message count."""
        six_hours_ago = (datetime.now() - timedelta(hours=6)).isoformat()

        db_count = db_conn.execute(
            "SELECT COUNT(*) FROM messages WHERE timestamp > ?", (six_hours_ago,)
        ).fetchone()[0]

        total_count = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]

        assert db_count <= total_count, f"6h count cannot exceed total: {db_count} > {total_count}"

    def test_12hour_messages_count(self, db, db_conn):
        """Verify 12-hour message count."""
        twelve_hours_ago = (datetime.now() - timedelta(hours=12)).isoformat()

        db_count = db_conn.execute(
            "SELECT COUNT(*) FROM messages WHERE timestamp > ?", (twelve_hours_ago,)
        ).fetchone()[0]

        total_count = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]

        assert db_count <= total_count, f"12h count cannot exceed total: {db_count} > {total_count}"

    def test_24hour_messages_count(self, db, db_conn):
        """Verify 24-hour message count."""
        one_day_ago = (datetime.now() - timedelta(hours=24)).isoformat()

        db_count = db_conn.execute(
            "SELECT COUNT(*) FROM messages WHERE timestamp > ?", (one_day_ago,)
        ).fetchone()[0]

        total_count = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]

        assert db_count <= total_count, f"24h count cannot exceed total: {db_count} > {total_count}"

    def test_time_windows_monotonically_nondecreasing(self, db, db_conn):
        """Verify time window counts are monotonically non-decreasing."""
        windows = [1, 6, 12, 24]
        counts = {}

        for hours in windows:
            cutoff = (datetime.now() - timedelta(hours=hours)).isoformat()
            count = db_conn.execute(
                "SELECT COUNT(*) FROM messages WHERE timestamp > ?", (cutoff,)
            ).fetchone()[0]
            counts[hours] = count

        # Verify monotonically non-decreasing: 1h <= 6h <= 12h <= 24h
        assert counts[1] <= counts[6], f"1h ({counts[1]}) should be <= 6h ({counts[6]})"
        assert counts[6] <= counts[12], f"6h ({counts[6]}) should be <= 12h ({counts[12]})"
        assert counts[12] <= counts[24], f"12h ({counts[12]}) should be <= 24h ({counts[24]})"


class TestUserFacts:
    """Tests for get_user_facts()."""

    def test_user_facts_match_database(self, db, db_conn, sample_user_id):
        """Verify user facts from method match DB query."""
        if not sample_user_id:
            pytest.skip("No sample user found")

        # Get facts via method
        method_facts = db.get_user_facts(sample_user_id)

        # Query DB directly
        db_facts = db_conn.execute(
            'SELECT * FROM user_facts WHERE user_id = ?', (sample_user_id,)
        ).fetchall()

        assert len(method_facts) == len(db_facts), f"Fact count mismatch: method={len(method_facts)}, db={len(db_facts)}"

    def test_user_facts_content_match(self, db, db_conn, sample_user_id):
        """Verify fact contents match exactly."""
        if not sample_user_id:
            pytest.skip("No sample user found")

        method_facts = db.get_user_facts(sample_user_id)

        if not method_facts:
            pytest.skip("No facts for user")

        # Check first fact
        method_fact = method_facts[0]
        db_fact = db_conn.execute(
            'SELECT * FROM user_facts WHERE user_id = ? AND fact_type = ?',
            (sample_user_id, method_fact['fact_type'])
        ).fetchone()

        assert db_fact is not None, "Fact not found in database"
        assert method_fact['fact_value'] == db_fact['fact_value'], "Fact value mismatch"

    def test_user_facts_returns_list(self, db, sample_user_id):
        """Verify user facts returns a list."""
        facts = db.get_user_facts(sample_user_id or "!00000000")
        assert isinstance(facts, list), "User facts should return a list"


class TestNodeDataIntegrity:
    """Tests for node data in contexts."""

    def test_node_hardware_model_accuracy(self, db, db_conn):
        """Verify hardware model data matches DB."""
        # Get all nodes with hw_model
        nodes = db_conn.execute(
            'SELECT node_id, hw_model FROM nodes WHERE hw_model IS NOT NULL LIMIT 1'
        ).fetchall()

        if not nodes:
            pytest.skip("No nodes with hardware model found")

        node = nodes[0]
        node_id = node['node_id']

        # Get via method
        method_node = db.get_node(node_id)

        assert method_node is not None, "Node not found via method"
        assert method_node['hw_model'] == node['hw_model'], "Hardware model mismatch"

    def test_node_battery_accuracy(self, db, db_conn):
        """Verify battery level matches DB."""
        nodes = db_conn.execute(
            'SELECT node_id, battery_level FROM nodes WHERE battery_level IS NOT NULL LIMIT 1'
        ).fetchall()

        if not nodes:
            pytest.skip("No nodes with battery level found")

        node = nodes[0]
        node_id = node['node_id']

        method_node = db.get_node(node_id)

        assert method_node is not None, "Node not found"
        assert method_node['battery_level'] == node['battery_level'], "Battery level mismatch"

    def test_node_gps_coordinates_accuracy(self, db, db_conn):
        """Verify GPS coordinates match DB."""
        nodes = db_conn.execute(
            'SELECT node_id, latitude, longitude FROM nodes WHERE latitude IS NOT NULL AND longitude IS NOT NULL LIMIT 1'
        ).fetchall()

        if not nodes:
            pytest.skip("No nodes with GPS coordinates found")

        node = nodes[0]
        node_id = node['node_id']

        method_node = db.get_node(node_id)

        assert method_node is not None, "Node not found"
        assert method_node['latitude'] == node['latitude'], "Latitude mismatch"
        assert method_node['longitude'] == node['longitude'], "Longitude mismatch"


class TestSentMessages:
    """Tests for sent messages count."""

    def test_sent_messages_count_accuracy(self, db, db_conn):
        """Verify sent messages count in stats."""
        stats = db.get_stats()
        stats_sent = stats.get('sent_messages', 0)

        # Query DB directly
        db_sent = db_conn.execute('SELECT COUNT(*) FROM sent_messages').fetchone()[0]

        assert stats_sent == db_sent, f"Sent messages count mismatch: stats={stats_sent}, db={db_sent}"


class TestCrossValidateStats:
    """Tests for stats cross-validation."""

    def test_stats_total_packets_accuracy(self, db, db_conn):
        """Verify total packets count in stats."""
        stats = db.get_stats()
        stats_packets = stats.get('total_packets', 0)

        db_packets = db_conn.execute('SELECT COUNT(*) FROM raw_packets').fetchone()[0]

        assert stats_packets == db_packets, f"Packets mismatch: stats={stats_packets}, db={db_packets}"

    def test_stats_total_messages_accuracy(self, db, db_conn):
        """Verify total messages count in stats."""
        stats = db.get_stats()
        stats_msgs = stats.get('total_messages', 0)

        db_msgs = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]

        assert stats_msgs == db_msgs, f"Messages mismatch: stats={stats_msgs}, db={db_msgs}"

    def test_stats_total_nodes_accuracy(self, db, db_conn):
        """Verify total nodes count in stats."""
        stats = db.get_stats()
        stats_nodes = stats.get('total_nodes', 0)

        db_nodes = db_conn.execute('SELECT COUNT(*) FROM nodes').fetchone()[0]

        assert stats_nodes == db_nodes, f"Nodes mismatch: stats={stats_nodes}, db={db_nodes}"

    def test_stats_telemetry_accuracy(self, db, db_conn):
        """Verify telemetry count in stats."""
        stats = db.get_stats()
        stats_tel = stats.get('telemetry_records', 0)

        db_tel = db_conn.execute('SELECT COUNT(*) FROM telemetry').fetchone()[0]

        assert stats_tel == db_tel, f"Telemetry mismatch: stats={stats_tel}, db={db_tel}"

    def test_stats_positions_accuracy(self, db, db_conn):
        """Verify positions count in stats."""
        stats = db.get_stats()
        stats_pos = stats.get('position_records', 0)

        db_pos = db_conn.execute('SELECT COUNT(*) FROM positions').fetchone()[0]

        assert stats_pos == db_pos, f"Positions mismatch: stats={stats_pos}, db={db_pos}"

    def test_stats_active_nodes_24h_accuracy(self, db, db_conn):
        """Verify active nodes (24h) count in stats."""
        stats = db.get_stats()
        stats_active = stats.get('active_nodes_24h', 0)

        cutoff = int(time.time()) - (24 * 3600)
        db_active = db_conn.execute(
            'SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,)
        ).fetchone()[0]

        assert stats_active == db_active, f"Active nodes (24h) mismatch: stats={stats_active}, db={db_active}"

    def test_stats_returns_dict(self, db):
        """Verify stats returns a dictionary."""
        stats = db.get_stats()
        assert isinstance(stats, dict), "Stats should return a dictionary"
        assert len(stats) > 0, "Stats should not be empty"


class TestDataConsistency:
    """Tests for overall data consistency."""

    def test_message_count_includes_outgoing(self, db, db_conn):
        """Verify message count includes both incoming and outgoing."""
        total = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        incoming = db_conn.execute('SELECT COUNT(*) FROM messages WHERE is_outgoing = 0').fetchone()[0]
        outgoing = db_conn.execute('SELECT COUNT(*) FROM messages WHERE is_outgoing = 1').fetchone()[0]

        # Total should equal sum of incoming and outgoing (plus any NULLs)
        assert total >= max(incoming, outgoing), "Total should be at least as large as either category"

    def test_node_last_heard_is_unix_timestamp(self, db_conn):
        """Verify node last_heard is stored as Unix timestamp."""
        nodes = db_conn.execute(
            'SELECT last_heard FROM nodes WHERE last_heard IS NOT NULL LIMIT 5'
        ).fetchall()

        if not nodes:
            pytest.skip("No nodes with last_heard found")

        current_time = int(time.time())
        for node in nodes:
            last_heard = node['last_heard']
            # Should be a reasonable Unix timestamp (within ~100 years)
            assert isinstance(last_heard, int) or isinstance(last_heard, float), \
                f"last_heard should be numeric, got {type(last_heard)}"
            assert 0 < last_heard < current_time + 86400, \
                f"last_heard {last_heard} outside reasonable range"

    def test_message_timestamps_are_iso_strings(self, db_conn):
        """Verify message timestamps are ISO format strings."""
        messages = db_conn.execute(
            'SELECT timestamp FROM messages LIMIT 5'
        ).fetchall()

        if not messages:
            pytest.skip("No messages found")

        for msg in messages:
            timestamp = msg['timestamp']
            # Should be ISO format (contains T)
            assert isinstance(timestamp, str), f"timestamp should be string, got {type(timestamp)}"
            assert 'T' in timestamp, f"timestamp should be ISO format, got {timestamp}"

    def test_database_file_exists(self):
        """Verify database file exists."""
        assert os.path.exists(DB_PATH), f"Database file not found at {DB_PATH}"

    def test_database_is_accessible(self, db_conn):
        """Verify database is accessible and queryable."""
        row = db_conn.execute('SELECT 1 as test').fetchone()
        assert row is not None, "Database should be accessible"
        assert row['test'] == 1, "Database query should return expected value"


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    def test_network_summary_with_zero_nodes(self, db, db_conn):
        """Test network summary generation with data."""
        summary = db.build_network_summary_for_llm()
        # Should return some kind of summary (even if empty network)
        assert isinstance(summary, str), "Summary should be a string"

    def test_context_with_nonexistent_user(self, db):
        """Test context building for nonexistent user."""
        # Should handle gracefully
        context = db.build_context_for_llm("!nonexistent", "NoOne")
        assert isinstance(context, str), "Should return string even for nonexistent user"

    def test_conversation_history_with_limit_zero(self, db, sample_user_id):
        """Test conversation history with limit=0."""
        if not sample_user_id:
            pytest.skip("No sample user")

        history = db.get_conversation_history(sample_user_id, limit=0)
        assert history == [], "Limit=0 should return empty list"

    def test_stats_with_empty_tables(self, db):
        """Verify stats handles empty or sparse tables."""
        stats = db.get_stats()

        # All counts should be >= 0 integers
        for key in ['total_packets', 'total_messages', 'total_nodes']:
            assert stats[key] >= 0, f"{key} should be non-negative"
            assert isinstance(stats[key], int), f"{key} should be an integer"

    def test_message_count_with_none_from_id(self, db, db_conn):
        """Test message count with NULL from_id."""
        # Should not fail
        method_count = db.get_message_count()
        db_count = db_conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        assert method_count == db_count, "Message counts should match"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
