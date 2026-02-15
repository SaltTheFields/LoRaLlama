"""
LoRaLlama Dashboard
A web-based GUI for monitoring the mesh network and LLM interactions.

Requirements:
    pip install flask flask-socketio

Usage:
    python dashboard.py
    Then open http://localhost:5000 in your browser
"""

import os
import time
from datetime import datetime, timedelta
from flask import Flask, render_template_string, jsonify, request
from flask_socketio import SocketIO

# Import our modules
from mesh_database import MeshDatabase

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('FLASK_SECRET_KEY', 'change-me-in-production')
socketio = SocketIO(app, cors_allowed_origins="*")

# Global state
db = None

# Dashboard HTML template
DASHBOARD_HTML = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>LoRaLlama Dashboard</title>
    <link rel="icon" type="image/png" href="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAACAAAAAZCAYAAABQDyyRAAAFl0lEQVR4nHVWW28bRRT+5rK7jmPHLQkxaUnSGylpSy8vCAQSqD+EFx75c/wAqBDijQcKUqsUqeotJI3TNI0dX9a7O4POOTNrN1KTyD7ZnTmX73znm1FXrm17AFAeEANiqJl59ofX0suwHHHLnI/5vR/0A0DHf5z2wYOHp2/e9AFbnbWDj7nnbIedZNdp1ras0bw3pk6e+I9+QyU+biRbavHzdtjOH/F5CBLRqWPylujb81ortoJyUg0voC8NKKc4ttihBqWgKkDRKwVoWhMd0xra7BR02Dfz58VfLCzYNhQCbzgDKHhU2kN7LTAGm76pEHZsYs8VnOaoEsQIeTgHChjQEFvXrYu+6Z2u4eNMVYA3UIMr1DWUmrOV/2QtJUWVz6NAJUQ2xjbqD9qW17kzMIVSVSUVVN6hKpywVlNCUo1UK1VxW+reBtoTXLXvGez+/RZ43vgeTBE+41G6ElmSYnl1CVobDN4NMCknQlPnZ3MU0PMlY8LtONsCsiOBKQYnwJBx5QK1ZCd2WZVYaCxg8+o60gULYwymPkf/VR9pI0WYISkAFYpxhayZwpUeKANRQ+XsO6IQpo4S0QxZXBCyI1hphYFGd3UFJgGaH2XwrRJ5mePm/etIMssIMOGcQ9Zu4LNvLqGclFAGUFbBefI9G1OemHp0JXkt408wyZyxzSz37CRXU/SHp0g3ErSvt3Dp23Xc/mEbymp4CkCBKo+sleKrn+7h6neXUIxLai5UquBIiHg6pM2OUKAW6TBhKmTCsIuWSKYeTL7e/iHeHB/h4YNHGD6eYnlzCfs7Pa4URqEqKtjUIO9PsbdzgLV7y2ieb8IVoqqUhCoDsYkyUQvCxOioXrEF4GUITD12CtXYYfByhINeDyY3aHQy+AVgaaOF7u1ltqnKrNlggnYuLqHKJUH2zHJ3RiXDeWJjI5gbgZAMrSb4nHxPPVzuoBYJIYPysMTXP36B7tYyssUUu3+/xmB/ggoVNKFSVURv4Yd3UImGz6n8ueBB2Cy5pIA1NEEX5I0CKgAG0JnGeDBBf3CMtbsr+OTmJopxAQ2NrfsbyE8L7P7VQ/HO4fTNENYY6gBUkG6eGBrbcFQK2RGmIKpa0G4KKPoMZjRttG2Lsihw7f4mPrnxMaajAtZSloKOTTW2vl9n7ZiWU+g0CARDHfRirgXxuZXDRgSCBmH+qI0azEyHw8bdC/zu2R97yFoSfOXyeZz0TuDGABKP1c87aK00MeyNWQuiUDHgfOQH9QzPrZy0ilvAgWluqS0xCZpOCySpRe/fIzQfpDjsH6BzsYML19awu3OAaV5id2cXC0kTbqAwGU5hjEblK6maWxBISO0NpyvVZwUaQYGrVUEmeWwk4zhCw9EQ21vbuPzPNg6PX+PCjYtI2hYnj06R/NnGnS9v4dcnv6CcVLAqli7VhlvAnESHCwlCL+IJxkIUyahF3+knH01x5dY6JmsDjD/tI+ta7D76D2+eH6H3sofOrSZeLj7F6t0OGq0MZSHHdDiy6htQ7GtE2NbzGQlK+uziWMozQhKVwsJKhsb5DMWdMcM9eTvB/sPX6G534a1HmU/hjxwaSwlGe2MYq2dIahEeDj93PuiakWEu43EZHsszUsW8xNsXA2SpRaYMpkcFkiSF9imsSTB8cYoin8BNgMHeUBIn+OPNKMBKgjWTfy8J1P0PtkhyvCqJI5tYPP39FZ789hyLyxlMw2P4boS0Wo3oTVwrnsOzcUGtDHQJvQ9XlT48hqmifJhtOXa5ozD/9UqRpnPOt2kAAAAAElFTkSuQmCC">
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/leaflet@1.9.4/dist/leaflet.css" />
    <script src="https://unpkg.com/leaflet@1.9.4/dist/leaflet.js"></script>
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.css" />
    <link rel="stylesheet" href="https://unpkg.com/leaflet.markercluster@1.5.3/dist/MarkerCluster.Default.css" />
    <script src="https://unpkg.com/leaflet.markercluster@1.5.3/dist/leaflet.markercluster.js"></script>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }

        body {
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f0f1a;
            color: #e0e0e0;
            height: 100vh;
            overflow: hidden;
            display: flex;
            flex-direction: column;
        }

        /* Header */
        .header {
            background: linear-gradient(135deg, #1a1a2e 0%, #0f0f1a 100%);
            padding: 12px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid #2a2a4a;
            z-index: 1000;
            flex-shrink: 0;
        }

        .logo {
            display: flex;
            align-items: center;
            gap: 12px;
        }

        .logo-icon { height: 48px; flex-shrink: 0; border-radius: 8px; }

        .logo h1 {
            font-size: 1.4em;
            font-weight: 600;
            background: linear-gradient(135deg, #00d4ff, #00ff88);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }

        .header-stats {
            display: flex;
            gap: 24px;
            align-items: center;
        }

        .header-stat {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 6px 12px;
            background: rgba(255,255,255,0.05);
            border-radius: 20px;
            font-size: 0.9em;
        }

        .header-stat .value {
            font-weight: 600;
            color: #00d4ff;
        }

        .weather-stat .weather-loc {
            color: rgba(255,255,255,0.5);
            font-size: 0.85em;
        }

        .status-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #00ff88;
            animation: pulse 2s infinite;
        }

        .status-dot.offline { background: #ff4757; animation: none; }

        @keyframes pulse {
            0%, 100% { opacity: 1; box-shadow: 0 0 0 0 rgba(0,255,136,0.4); }
            50% { opacity: 0.8; box-shadow: 0 0 0 8px rgba(0,255,136,0); }
        }

        /* Main Layout */
        .main-container {
            display: grid;
            grid-template-columns: 1fr 400px;
            grid-template-rows: auto 1fr;
            flex: 1;
            min-height: 0;
            gap: 1px;
            background: #1a1a2e;
            transition: padding-bottom 0.3s ease;
        }
        .timeline-visible .main-container {
            padding-bottom: 70px;
        }

        /* Map Section */
        .map-section {
            grid-row: 1 / 3;
            position: relative;
        }

        #map {
            width: 100%;
            height: 100%;
            background: #0a0a15;
        }

        .map-overlay {
            position: absolute;
            top: 80px;
            left: 16px;
            z-index: 1000;
            display: flex;
            flex-direction: column;
            gap: 8px;
        }

        .map-stat {
            background: rgba(15, 15, 26, 0.9);
            backdrop-filter: blur(10px);
            padding: 12px 16px;
            border-radius: 10px;
            border: 1px solid rgba(255,255,255,0.1);
        }

        .map-stat-label {
            font-size: 0.75em;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }

        .map-stat-value {
            font-size: 1.8em;
            font-weight: 700;
            color: #00d4ff;
            line-height: 1.2;
        }

        /* Right Sidebar */
        .sidebar {
            display: flex;
            flex-direction: column;
            background: #0f0f1a;
            overflow: hidden;
        }

        /* Tabs */
        .tabs {
            display: flex;
            background: rgba(255,255,255,0.03);
            border-bottom: 1px solid #2a2a4a;
            overflow: hidden;
            flex-shrink: 0;
        }

        .tab-header-info {
            display: none;
        }
        .tab-header-info span {
            font-weight: bold;
        }
        .tab-count {
            font-size: 0.7em;
            color: #00d4ff;
            font-weight: 700;
        }

        /* Quick-send buttons */
        .quick-btns {
            display: flex;
            gap: 4px;
            margin-right: 8px;
        }
        .quick-btn {
            background: #2a2a4a;
            border: 1px solid #3a3a5a;
            border-radius: 6px;
            padding: 6px 10px;
            cursor: pointer;
            font-size: 16px;
            transition: background 0.2s;
        }
        .quick-btn:hover {
            background: #3a3a5a;
        }

        .tab {
            flex: 1;
            padding: 8px 4px;
            text-align: center;
            cursor: pointer;
            font-size: 0.85em;
            font-weight: 500;
            color: #666;
            transition: all 0.2s;
            border-bottom: 2px solid transparent;
            line-height: 1.3;
        }

        .tab:hover { color: #aaa; background: rgba(255,255,255,0.02); }
        .tab.active {
            color: #00d4ff;
            border-bottom-color: #00d4ff;
            background: rgba(0,212,255,0.05);
        }

        .tab-content {
            display: none;
            flex: 1;
            overflow: hidden;
            flex-direction: column;
        }

        .tab-content.active {
            display: flex;
        }

        /* Messages Panel */
        .message-list {
            flex: 1;
            overflow-y: auto;
            padding: 12px;
        }

        .message-item {
            padding: 12px;
            margin-bottom: 8px;
            background: rgba(255,255,255,0.03);
            border-radius: 10px;
            border-left: 3px solid #00d4ff;
            transition: transform 0.2s, background 0.2s;
        }

        .message-item:hover {
            background: rgba(255,255,255,0.05);
            transform: translateX(4px);
        }

        .message-item.sent {
            border-left: none;
            border-right: 3px solid #00ff88;
            background: rgba(0,255,136,0.04);
            margin-left: 24px;
            margin-right: 0;
            text-align: right;
        }

        .message-item.sent .message-header {
            flex-direction: row-reverse;
        }

        .message-item.sent .message-text {
            color: #b8e6c8;
        }

        .message-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }

        .message-from {
            font-weight: 600;
            color: #00d4ff;
            font-size: 0.9em;
        }

        .message-item.sent .message-from { color: #00ff88; }

        .message-item.dm {
            border-left-color: #ff9f43;
        }

        .message-item.dm.sent {
            border-right-color: #ff9f43;
            border-left: none;
        }

        .dm-badge {
            display: inline-block;
            padding: 1px 6px;
            background: rgba(255,159,67,0.2);
            border: 1px solid rgba(255,159,67,0.4);
            border-radius: 4px;
            color: #ff9f43;
            font-size: 0.7em;
            font-weight: 700;
            margin-left: 6px;
        }

        #msg-view-all, #msg-view-dms {
            flex-direction: column;
            flex: 1;
            overflow: hidden;
            min-height: 0;
        }

        /* Message view toggle */
        .msg-view-toggle {
            display: flex;
            gap: 0;
            padding: 8px 12px 0;
        }

        .msg-view-btn {
            flex: 1;
            padding: 6px 12px;
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.1);
            color: #888;
            font-size: 0.85em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
        }

        .msg-view-btn:first-child { border-radius: 6px 0 0 6px; }
        .msg-view-btn:last-child { border-radius: 0 6px 6px 0; }

        .msg-view-btn.active {
            background: rgba(0,212,255,0.15);
            border-color: #00d4ff;
            color: #00d4ff;
        }

        /* DM Conversations list */
        .dm-conversations {
            flex: 1;
            overflow-y: auto;
            padding: 12px;
        }

        .dm-conv-item {
            display: flex;
            align-items: center;
            padding: 10px 12px;
            margin-bottom: 6px;
            background: rgba(255,255,255,0.03);
            border-radius: 8px;
            cursor: pointer;
            transition: all 0.2s;
            border-left: 3px solid #ff9f43;
        }

        .dm-conv-item:hover {
            background: rgba(255,255,255,0.06);
            transform: translateX(4px);
        }

        .dm-conv-avatar {
            width: 36px;
            height: 36px;
            border-radius: 8px;
            background: linear-gradient(135deg, #ff9f4333, #ff6b6b33);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 0.8em;
            color: #ff9f43;
            margin-right: 10px;
            flex-shrink: 0;
        }

        .dm-conv-info { flex: 1; min-width: 0; }

        .dm-conv-name {
            font-weight: 600;
            color: #fff;
            font-size: 0.9em;
        }

        .dm-conv-preview {
            font-size: 0.8em;
            color: #777;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }

        .dm-conv-meta {
            text-align: right;
            flex-shrink: 0;
            margin-left: 8px;
        }

        .dm-conv-time {
            font-size: 0.7em;
            color: #555;
        }

        .dm-conv-count {
            display: inline-block;
            padding: 1px 6px;
            background: rgba(255,159,67,0.2);
            border-radius: 10px;
            color: #ff9f43;
            font-size: 0.7em;
            font-weight: 700;
            margin-top: 2px;
        }

        /* DM Thread */
        .dm-thread {
            flex: 1;
            display: flex;
            flex-direction: column;
            overflow: hidden;
        }

        .dm-thread-header {
            display: flex;
            align-items: center;
            gap: 8px;
            padding: 10px 12px;
            background: rgba(255,159,67,0.08);
            border-bottom: 1px solid rgba(255,159,67,0.2);
        }

        .dm-thread-back {
            background: none;
            border: none;
            color: #ff9f43;
            font-size: 1.2em;
            cursor: pointer;
            padding: 2px 8px;
            border-radius: 4px;
        }

        .dm-thread-back:hover {
            background: rgba(255,159,67,0.15);
        }

        .dm-thread-name {
            font-weight: 600;
            color: #ff9f43;
            font-size: 0.95em;
        }

        .dm-thread-messages {
            flex: 1;
            overflow-y: auto;
            padding: 12px;
        }

        .message-time {
            font-size: 0.75em;
            color: #555;
        }

        .message-text {
            color: #ccc;
            font-size: 0.9em;
            line-height: 1.4;
            word-wrap: break-word;
        }

        .message-meta {
            margin-top: 8px;
            display: flex;
            gap: 12px;
            font-size: 0.75em;
            color: #555;
        }

        .message-meta span {
            display: flex;
            align-items: center;
            gap: 4px;
        }

        /* Chat Input */
        .chat-input-container {
            padding: 12px;
            background: rgba(0,0,0,0.3);
            border-top: 1px solid #2a2a4a;
        }

        .chat-input-wrapper {
            display: flex;
            gap: 8px;
        }

        .chat-input {
            flex: 1;
            background: rgba(255,255,255,0.05);
            border: 1px solid #2a2a4a;
            border-radius: 8px;
            padding: 10px 14px;
            color: #e0e0e0;
            font-size: 0.9em;
            outline: none;
            transition: border-color 0.2s;
        }

        .chat-input:focus {
            border-color: #00d4ff;
        }

        .chat-input::placeholder {
            color: #555;
        }

        .channel-select {
            background: #1a1a2e;
            border: 1px solid #333;
            border-radius: 8px;
            color: #00d4ff;
            padding: 8px 4px;
            font-size: 0.75em;
            font-weight: 600;
            cursor: pointer;
            outline: none;
            min-width: 58px;
        }

        .channel-select:focus {
            border-color: #00d4ff;
        }

        .chat-send-btn {
            background: linear-gradient(135deg, #00d4ff, #00ff88);
            border: none;
            border-radius: 8px;
            padding: 10px 20px;
            color: #000;
            font-weight: 600;
            cursor: pointer;
            transition: opacity 0.2s, transform 0.2s;
        }

        .chat-send-btn:hover {
            opacity: 0.9;
            transform: scale(1.02);
        }

        .chat-send-btn:disabled {
            opacity: 0.5;
            cursor: not-allowed;
        }

        .chat-status {
            margin-top: 8px;
            font-size: 0.8em;
            display: flex;
            justify-content: space-between;
        }

        .char-counter {
            color: #555;
        }

        .char-counter.warning { color: #ffa502; }
        .char-counter.danger { color: #ff4757; }

        .send-status {
            color: #00ff88;
        }

        .send-status.error { color: #ff4757; }

        /* Nodes Panel */
        .node-controls {
            display: flex;
            gap: 8px;
            padding: 12px 12px 0 12px;
            align-items: center;
        }

        .node-search {
            flex: 1;
            padding: 8px 12px;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 8px;
            color: #e0e0e0;
            font-size: 0.9em;
            outline: none;
            transition: border-color 0.2s;
        }

        .node-search:focus {
            border-color: #00d4ff;
        }

        .node-search::placeholder {
            color: rgba(255,255,255,0.3);
        }

        .node-sort {
            padding: 8px 12px;
            background: rgba(255,255,255,0.06);
            border: 1px solid rgba(255,255,255,0.1);
            border-radius: 8px;
            color: #e0e0e0;
            font-size: 0.9em;
            outline: none;
            cursor: pointer;
        }

        .node-sort:focus {
            border-color: #00d4ff;
        }

        .node-list {
            flex: 1;
            overflow-y: auto;
            padding: 12px;
        }

        .node-item {
            display: flex;
            align-items: center;
            padding: 12px;
            margin-bottom: 8px;
            background: rgba(255,255,255,0.03);
            border-radius: 10px;
            cursor: pointer;
            transition: all 0.2s;
        }

        .node-item:hover {
            background: rgba(255,255,255,0.06);
            transform: translateX(4px);
        }

        .node-avatar {
            width: 40px;
            height: 40px;
            border-radius: 10px;
            background: linear-gradient(135deg, #00d4ff33, #00ff8833);
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 0.85em;
            color: #00d4ff;
            margin-right: 12px;
        }

        .node-trace-btn {
            padding: 4px 8px;
            background: rgba(0,255,136,0.15);
            border: 1px solid rgba(0,255,136,0.3);
            border-radius: 6px;
            color: #00ff88;
            font-size: 0.7em;
            font-weight: 700;
            cursor: pointer;
            margin-left: 8px;
            transition: all 0.2s;
        }

        .node-trace-btn:hover {
            background: rgba(0,255,136,0.3);
            border-color: #00ff88;
        }

        .node-info { flex: 1; }

        .node-name {
            font-weight: 600;
            color: #fff;
            font-size: 0.95em;
        }

        .node-id {
            font-size: 0.75em;
            color: #555;
            font-family: 'Monaco', 'Consolas', monospace;
        }

        .node-stats { text-align: right; }

        .node-signal {
            font-weight: 600;
            font-size: 0.85em;
        }

        .signal-excellent { color: #00ff88; }
        .signal-good { color: #7bed9f; }
        .signal-okay { color: #b8e986; }
        .signal-fair { color: #ffa502; }
        .signal-weak { color: #ff4757; }

        .node-lastseen {
            font-size: 0.7em;
            color: #555;
        }

        /* ==================== REDESIGNED STATS PANEL ==================== */
        .stats-panel {
            padding: 8px 12px;
            overflow-y: hidden;
            display: flex;
            flex-direction: column;
            gap: 6px;
            height: 100%;
        }

        /* Filter Bar */
        .stats-filter-bar {
            display: flex;
            align-items: center;
            gap: 4px;
            padding: 2px 0;
        }
        .filter-label {
            font-size: 0.7em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-right: 4px;
        }
        .filter-btn {
            background: rgba(255,255,255,0.05);
            border: 1px solid #2a2a4a;
            border-radius: 4px;
            padding: 3px 10px;
            color: #888;
            font-size: 0.72em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s;
        }
        .filter-btn:hover { background: rgba(0,212,255,0.1); color: #aaa; }
        .filter-btn.active { background: rgba(0,212,255,0.2); border-color: #00d4ff; color: #00d4ff; }
        .filter-btn:disabled { opacity: 0.4; cursor: not-allowed; }

        /* Range buttons for messages and map */
        .msg-range-bar {
            display: flex;
            gap: 4px;
            padding: 4px 12px;
            background: rgba(0,0,0,0.2);
            border-bottom: 1px solid #2a2a4a;
        }
        .map-range-bar {
            display: flex;
            gap: 4px;
            padding: 6px 8px;
        }
        .range-btn {
            background: rgba(255,255,255,0.05);
            border: 1px solid #2a2a4a;
            border-radius: 4px;
            padding: 2px 8px;
            color: #888;
            font-size: 0.65em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s;
        }
        .range-btn:hover { background: rgba(0,212,255,0.1); color: #aaa; }
        .range-btn.active { background: rgba(0,212,255,0.2); border-color: #00d4ff; color: #00d4ff; }
        .range-btn:disabled { opacity: 0.4; cursor: not-allowed; }

        /* Key Metrics - 4 across */
        .stats-grid-4 {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 6px;
        }
        .stat-mini {
            background: rgba(255,255,255,0.03);
            border-radius: 8px;
            padding: 8px 4px;
            text-align: center;
        }
        .stat-mini-value {
            font-size: 1.4em;
            font-weight: 700;
            background: linear-gradient(135deg, #00d4ff, #00ff88);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            line-height: 1.2;
        }
        .stat-mini-label {
            font-size: 0.6em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.3px;
            margin-top: 2px;
        }

        /* Secondary Metrics */
        .stats-grid-4.secondary { gap: 4px; }
        .stat-micro {
            background: rgba(255,255,255,0.02);
            border-radius: 6px;
            padding: 5px 4px;
            text-align: center;
        }
        .stat-micro-value { font-size: 0.95em; font-weight: 600; color: #00d4ff; line-height: 1.2; }
        .stat-micro-label { font-size: 0.55em; color: #555; text-transform: uppercase; letter-spacing: 0.3px; }

        /* Collapsible Sections */
        .collapsible-section {
            background: rgba(255,255,255,0.03);
            border-radius: 8px;
            overflow: hidden;
        }
        .collapsible-header {
            padding: 6px 10px;
            font-size: 0.7em;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            cursor: pointer;
            user-select: none;
            transition: color 0.15s;
        }
        .collapsible-header:hover { color: #00d4ff; }
        .collapsible-header .collapse-arrow {
            display: inline-block;
            transition: transform 0.2s;
            font-size: 0.8em;
        }
        .collapsible-header.collapsed .collapse-arrow { transform: rotate(-90deg); }
        .collapsible-content { padding: 0 10px 8px; }
        .collapsible-content.hidden { display: none; }

        /* Hop Distribution Bar */
        .health-label { font-size: 0.6em; color: #666; margin-bottom: 3px; }
        .hop-bar {
            display: flex;
            height: 18px;
            border-radius: 4px;
            overflow: hidden;
            background: rgba(0,0,0,0.3);
            margin-bottom: 2px;
        }
        .hop-seg {
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 0.55em;
            font-weight: 700;
            color: #000;
            transition: width 0.3s ease;
            min-width: 0;
            overflow: hidden;
        }
        .hop-0 { background: #00ff88; }
        .hop-1 { background: #00e5a0; }
        .hop-2 { background: #00cce0; }
        .hop-3 { background: #00aaff; }
        .hop-4 { background: #5577ff; }
        .hop-5 { background: #8855dd; }
        .hop-6 { background: #cc44aa; }
        .hop-7 { background: #ff3366; }

        /* Health Metrics Row */
        .health-metrics-row { display: flex; gap: 6px; }
        .health-metric {
            flex: 1;
            text-align: center;
            padding: 4px;
            background: rgba(0,0,0,0.2);
            border-radius: 4px;
        }
        .health-metric-label { font-size: 0.55em; color: #666; display: block; }
        .health-metric-value { font-size: 1em; font-weight: 700; color: #00d4ff; }
        .health-metric-unit { font-size: 0.6em; color: #555; }

        /* Top Nodes */
        .top-node-row {
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 3px 0;
            font-size: 0.75em;
        }
        .top-node-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            flex-shrink: 0;
        }
        .top-node-name {
            flex: 1;
            color: #ccc;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .top-node-count { color: #00d4ff; font-weight: 600; }
        .top-node-batt { color: #666; font-size: 0.85em; }

        /* Packet Types (horizontal bars) */
        .packet-breakdown {
            background: rgba(255,255,255,0.03);
            border-radius: 8px;
            padding: 6px 10px;
        }
        .packet-breakdown h3 {
            font-size: 0.7em;
            color: #888;
            margin-bottom: 4px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        .packet-row {
            display: flex;
            align-items: center;
            gap: 6px;
            padding: 2px 0;
        }
        .packet-type {
            font-size: 0.68em;
            color: #aaa;
            width: 75px;
            flex-shrink: 0;
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }
        .packet-bar-track {
            flex: 1;
            height: 10px;
            background: rgba(0,0,0,0.3);
            border-radius: 3px;
            overflow: hidden;
        }
        .packet-bar-fill {
            height: 100%;
            background: linear-gradient(90deg, #00d4ff, #00ff88);
            border-radius: 3px;
            transition: width 0.3s ease;
        }
        .packet-count {
            font-size: 0.68em;
            font-weight: 600;
            color: #00d4ff;
            width: 40px;
            text-align: right;
            flex-shrink: 0;
        }

        /* Activity Chart (improved) */
        .chart-section {
            background: rgba(255,255,255,0.03);
            border-radius: 8px;
            padding: 6px 10px;
            flex: 1;
            display: flex;
            flex-direction: column;
            min-height: 0;
        }
        .chart-section h3 {
            font-size: 0.7em;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
            display: flex;
            justify-content: space-between;
        }
        .chart-range-label { color: #00d4ff; font-weight: 600; }
        .chart-wrapper {
            flex: 1;
            display: flex;
            gap: 4px;
            min-height: 0;
        }
        .chart-y-axis {
            display: flex;
            flex-direction: column;
            justify-content: space-between;
            align-items: flex-end;
            width: 26px;
            flex-shrink: 0;
            padding-bottom: 14px;
        }
        .chart-y-axis span { font-size: 0.55em; color: #555; line-height: 1; }
        .chart-body {
            flex: 1;
            display: flex;
            flex-direction: column;
            min-height: 0;
        }
        .chart-container {
            flex: 1;
            display: flex;
            align-items: flex-end;
            gap: 2px;
            position: relative;
            min-height: 60px;
        }
        .chart-bar {
            flex: 1;
            background: linear-gradient(to top, #00d4ff, #00ff88);
            border-radius: 2px 2px 0 0;
            min-height: 2px;
            transition: height 0.3s ease;
            cursor: pointer;
            position: relative;
        }
        .chart-bar:hover { opacity: 0.8; }
        .chart-bar:hover::after {
            content: attr(data-tooltip);
            position: absolute;
            bottom: calc(100% + 4px);
            left: 50%;
            transform: translateX(-50%);
            background: #1a1a2e;
            border: 1px solid #2a2a4a;
            color: #00d4ff;
            font-size: 0.6em;
            padding: 2px 5px;
            border-radius: 3px;
            white-space: nowrap;
            pointer-events: none;
            z-index: 10;
        }
        .chart-bar.current-hour {
            background: linear-gradient(to top, #00ff88, #00ffcc);
            box-shadow: 0 0 6px rgba(0,255,136,0.4);
        }
        .chart-x-axis {
            display: flex;
            justify-content: space-between;
            height: 14px;
            padding-top: 2px;
        }
        .chart-x-axis span { font-size: 0.5em; color: #555; }

        /* Hop Legend */
        .hop-legend {
            display: flex;
            justify-content: space-between;
            font-size: 0.55em;
            color: #888;
            margin-top: 2px;
            padding: 0 2px;
        }

        /* Scrollbar */
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: #333; border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: #444; }

        /* ==================== TIMELINE SCRUBBER ==================== */
        .timeline-container {
            position: fixed;
            bottom: 0;
            left: 0;
            right: 0;
            background: linear-gradient(to top, #0a0a0f, rgba(10,10,15,0.98));
            padding: 8px 20px 12px;
            border-top: 1px solid #1a1a2e;
            z-index: 100;
            height: 70px;
            box-sizing: border-box;
            transform: translateY(100%);
            transition: transform 0.3s ease;
        }
        .timeline-visible .timeline-container {
            transform: translateY(0);
        }
        .timeline-info {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 6px;
        }
        #timeline-date {
            color: #00d4ff;
            font-weight: bold;
            font-size: 14px;
        }
        #timeline-date.historical {
            color: #ffa502;
        }
        .timeline-slider {
            width: 100%;
            height: 12px;
            -webkit-appearance: none;
            background: #1a1a2e;
            border-radius: 6px;
            outline: none;
            cursor: pointer;
        }
        .timeline-slider::-webkit-slider-thumb {
            -webkit-appearance: none;
            width: 24px;
            height: 24px;
            background: #00d4ff;
            border-radius: 50%;
            cursor: pointer;
            box-shadow: 0 0 10px rgba(0,212,255,0.5);
            transition: transform 0.1s;
        }
        .timeline-slider::-webkit-slider-thumb:hover {
            transform: scale(1.2);
        }
        .timeline-slider::-moz-range-thumb {
            width: 24px;
            height: 24px;
            background: #00d4ff;
            border-radius: 50%;
            cursor: pointer;
            border: none;
        }
        .timeline-labels {
            display: flex;
            justify-content: space-between;
            color: #666;
            font-size: 11px;
            margin-top: 4px;
        }
        .sync-now-btn {
            background: #ffa502;
            color: #0a0a0f;
            border: none;
            padding: 6px 16px;
            border-radius: 15px;
            cursor: pointer;
            font-weight: bold;
            font-size: 12px;
            transition: all 0.2s;
        }
        .sync-now-btn:hover {
            background: #ffb732;
            transform: scale(1.05);
        }
        .sync-now-btn.hidden { display: none; }

        /* Historical mode indicator */
        .historical-mode .map-section {
            border: 2px solid #ffa502;
            border-radius: 12px;
        }
        .historical-mode .sidebar {
            border-left: 2px solid #ffa502;
        }
        .historical-mode .header {
            background: linear-gradient(135deg, #1a1a2e 0%, #2a1a1a 100%);
        }

        /* Timeline height handled by .timeline-visible class above */
        .sidebar {
            padding-bottom: 10px;
        }

        /* ==================== DM MODAL ==================== */
        .dm-modal {
            position: fixed;
            top: 0;
            left: 0;
            right: 0;
            bottom: 0;
            background: rgba(0,0,0,0.85);
            display: flex;
            align-items: center;
            justify-content: center;
            z-index: 1000;
        }
        .dm-modal.hidden { display: none; }
        .dm-modal-content {
            background: #16213e;
            padding: 20px;
            border-radius: 12px;
            width: 400px;
            max-width: 90%;
            border: 1px solid #00d4ff;
            box-shadow: 0 0 30px rgba(0,212,255,0.3);
        }
        .dm-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 15px;
            color: #e0e0e0;
        }
        .dm-header strong {
            color: #00d4ff;
        }
        .dm-close-btn {
            background: transparent;
            border: none;
            color: #888;
            font-size: 20px;
            cursor: pointer;
        }
        .dm-close-btn:hover { color: #ff4757; }
        .dm-footer {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-top: 10px;
        }
        .dm-btn {
            margin-top: 10px;
            width: 100%;
            padding: 10px;
            background: #00d4ff;
            color: #0a0a0f;
            border: none;
            border-radius: 6px;
            cursor: pointer;
            font-weight: bold;
            font-size: 14px;
            transition: all 0.2s;
        }
        .dm-btn:hover {
            background: #33ddff;
        }

        /* ==================== NODE ANIMATIONS ON PACKET ==================== */
        /* 10s throbbing green/cyan glow for TEXT messages (2s x 5 iterations) */
        .node-msg-throb div {
            animation: msg-throb 2s ease-in-out 5;
        }
        @keyframes msg-throb {
            0%   { box-shadow: 0 0 4px 2px rgba(0,255,136,0.3); transform: scale(1); }
            50%  { box-shadow: 0 0 18px 8px rgba(0,212,255,0.7); transform: scale(1.35); }
            100% { box-shadow: 0 0 4px 2px rgba(0,255,136,0.3); transform: scale(1); }
        }
        /* 3s subtle grey pulse for non-message data packets (1.5s x 2 iterations) */
        .node-data-glow div {
            animation: data-glow 1.5s ease-in-out 2;
        }
        @keyframes data-glow {
            0%   { box-shadow: 0 0 2px 1px rgba(150,150,150,0.2); transform: scale(1); }
            50%  { box-shadow: 0 0 10px 4px rgba(150,150,150,0.4); transform: scale(1.1); }
            100% { box-shadow: 0 0 2px 1px rgba(150,150,150,0.2); transform: scale(1); }
        }

        /* Pond ripple rings — must escape marker bounds */
        .leaflet-div-icon.custom-marker,
        .leaflet-marker-icon.custom-marker { overflow: visible !important; }
        .marker-label {
            position: absolute;
            top: 26px; left: 12px;
            transform: translateX(-50%);
            white-space: nowrap;
            font-size: 10px;
            font-weight: 600;
            color: #e0e0e0;
            text-shadow: 0 0 3px #000, 0 0 6px #000, 1px 1px 2px #000;
            pointer-events: none;
            line-height: 1;
        }
        .ripple-ring {
            position: absolute;
            top: 12px; left: 12px;
            width: 0; height: 0;
            border: 3px solid #00d4ff;
            border-radius: 50%;
            pointer-events: none;
            opacity: 0;
        }
        @keyframes pond-ripple {
            0%   { width: 0; height: 0; top: 12px; left: 12px; opacity: 0.9; border-width: 3px; }
            100% { width: 120px; height: 120px; top: -48px; left: -48px; opacity: 0; border-width: 1px; }
        }
        .ripple-ring-1 { animation: pond-ripple 1.5s ease-out 0s 5; }
        .ripple-ring-2 { animation: pond-ripple 1.5s ease-out 0.5s 5; }
        .ripple-ring-3 { animation: pond-ripple 1.5s ease-out 1.0s 5; }

        /* ==================== NETWORK TAB ==================== */
        .network-panel {
            padding: 12px;
            overflow-y: auto;
            height: 100%;
        }
        .network-section {
            background: rgba(255,255,255,0.03);
            border-radius: 8px;
            margin-bottom: 10px;
            overflow: hidden;
        }
        .network-section-header {
            padding: 8px 12px;
            font-size: 0.75em;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            cursor: pointer;
            user-select: none;
            transition: color 0.15s;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        .network-section-header:hover { color: #00d4ff; }
        .network-section-header .section-arrow {
            display: inline-block;
            transition: transform 0.2s;
            font-size: 0.8em;
        }
        .network-section-header.collapsed .section-arrow { transform: rotate(-90deg); }
        .network-section-body { padding: 0 12px 10px; }
        .network-section-body.hidden { display: none; }

        /* Topology canvas */
        #topology-canvas {
            width: 100%;
            height: 300px;
            background: rgba(0,0,0,0.2);
            border-radius: 6px;
        }

        /* Trace All button */
        .trace-all-btn {
            background: rgba(0, 212, 255, 0.15);
            border: 1px solid rgba(0, 212, 255, 0.3);
            color: #00d4ff;
            font-size: 0.85em;
            padding: 2px 8px;
            border-radius: 4px;
            cursor: pointer;
        }
        .trace-all-btn:hover { background: rgba(0, 212, 255, 0.3); }
        .trace-all-btn.cancel {
            background: rgba(255, 71, 87, 0.15);
            border-color: rgba(255, 71, 87, 0.3);
            color: #ff4757;
        }
        .trace-all-btn.cancel:hover { background: rgba(255, 71, 87, 0.3); }

        /* Traceroute list */
        .traceroute-item {
            padding: 6px 0;
            border-bottom: 1px solid rgba(255,255,255,0.05);
            font-size: 0.8em;
        }
        .traceroute-item:last-child { border-bottom: none; }
        .traceroute-route {
            color: #00d4ff;
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 0.9em;
        }
        .traceroute-time { color: #555; font-size: 0.85em; }
        .traceroute-snr { color: #7bed9f; font-size: 0.85em; }

        /* SF stats */
        .sf-item {
            display: flex;
            justify-content: space-between;
            padding: 4px 0;
            font-size: 0.8em;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .sf-item:last-child { border-bottom: none; }
        .sf-node { color: #ccc; }
        .sf-stat { color: #00d4ff; font-weight: 600; }

        /* Alert list */
        .alert-item {
            padding: 6px 0;
            border-bottom: 1px solid rgba(255,255,255,0.05);
            font-size: 0.8em;
        }
        .alert-item:last-child { border-bottom: none; }
        .alert-text { color: #ffa502; }
        .alert-from { color: #888; font-size: 0.85em; }
        .alert-time { color: #555; font-size: 0.85em; }

        /* Paxcounter */
        .pax-item {
            display: flex;
            justify-content: space-between;
            padding: 4px 0;
            font-size: 0.8em;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .pax-item:last-child { border-bottom: none; }
        .pax-counts { color: #00d4ff; }

        /* Signal sparkline in popups */
        .sparkline-container { margin-top: 8px; }
        .sparkline-label { font-size: 0.7em; color: #888; margin-bottom: 2px; }

        /* Map trail toggle */
        .map-trail-btn {
            background: rgba(255,255,255,0.05);
            border: 1px solid #2a2a4a;
            border-radius: 4px;
            padding: 3px 10px;
            color: #888;
            font-size: 0.72em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s;
        }
        .map-trail-btn:hover { background: rgba(0,212,255,0.1); color: #aaa; }
        .map-trail-btn.active { background: rgba(0,212,255,0.2); border-color: #00d4ff; color: #00d4ff; }

        .map-follow-label {
            display: flex;
            align-items: center;
            gap: 4px;
            border: 1px solid #333;
            background: rgba(0,0,0,0.3);
            border-radius: 4px;
            padding: 3px 10px;
            color: #888;
            font-size: 0.72em;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s;
        }
        .map-follow-label:hover { background: rgba(0,212,255,0.1); color: #aaa; }
        .map-follow-label.active { background: rgba(0,212,255,0.2); border-color: #00d4ff; color: #00d4ff; }
        .map-follow-label input[type="checkbox"] { accent-color: #00d4ff; cursor: pointer; }

        /* Leaflet popup customization */
        .leaflet-popup-content-wrapper {
            background: rgba(15, 15, 26, 0.95);
            border: 1px solid #2a2a4a;
            border-radius: 10px;
            color: #e0e0e0;
        }

        .leaflet-popup-tip { background: rgba(15, 15, 26, 0.95); }

        .node-popup { min-width: 240px; }

        .node-popup h4 {
            color: #00d4ff;
            margin-bottom: 4px;
            font-size: 1.1em;
        }

        .node-popup-id {
            font-size: 0.75em;
            color: #666;
            font-family: 'Monaco', 'Consolas', monospace;
            margin-bottom: 8px;
        }

        .node-popup-section {
            border-top: 1px solid rgba(255,255,255,0.08);
            padding-top: 6px;
            margin-top: 4px;
        }

        .node-popup-row {
            display: flex;
            justify-content: space-between;
            padding: 3px 0;
            font-size: 0.85em;
            border-bottom: 1px solid rgba(255,255,255,0.06);
        }

        .node-popup-row:last-child { border-bottom: none; }

        .node-popup-label { color: #888; }
        .node-popup-value { color: #fff; font-weight: 500; }

        .node-popup-actions {
            display: flex;
            gap: 6px;
            margin-top: 8px;
        }
        .node-popup-actions button {
            flex: 1;
            padding: 6px 8px;
            border-radius: 6px;
            border: none;
            cursor: pointer;
            font-size: 0.8em;
            font-weight: 600;
            transition: all 0.15s;
        }
        .node-popup-actions .btn-dm {
            background: #00d4ff;
            color: #0a0a0f;
        }
        .node-popup-actions .btn-dm:hover { background: #33ddff; }
        .node-popup-actions .btn-trace {
            background: #00ff88;
            color: #0a0a0f;
        }
        .node-popup-actions .btn-trace:hover { background: #33ff99; }
        .node-popup-actions .btn-detail {
            background: transparent;
            border: 1px solid #2a2a4a;
            color: #aaa;
        }
        .node-popup-actions .btn-detail:hover { border-color: #00d4ff; color: #00d4ff; }
        .node-popup-actions .btn-center {
            background: rgba(255,255,255,0.08);
            color: #aaa;
        }
        .node-popup-actions .btn-center:hover { background: rgba(255,255,255,0.15); color: #fff; }

        .node-popup-traceroutes {
            display: none;
            margin-top: 6px;
            padding-top: 6px;
            border-top: 1px solid rgba(255,255,255,0.08);
            font-size: 0.8em;
        }
        .node-popup-traceroutes.visible { display: block; }
        .popup-tr-item {
            color: #00d4ff;
            font-family: 'Monaco', 'Consolas', monospace;
            font-size: 0.85em;
            padding: 2px 0;
        }
        .popup-tr-time { color: #555; font-size: 0.85em; }

        /* Hop legend on map */
        .map-hop-legend {
            position: absolute;
            bottom: 16px;
            left: 16px;
            z-index: 1000;
            background: rgba(15, 15, 26, 0.88);
            backdrop-filter: blur(8px);
            padding: 8px 12px;
            border-radius: 8px;
            border: 1px solid rgba(255,255,255,0.1);
            display: flex;
            flex-wrap: wrap;
            gap: 6px 12px;
            max-width: 280px;
        }
        .map-hop-legend-title {
            width: 100%;
            font-size: 0.6em;
            color: #888;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 2px;
        }
        .map-hop-legend-item {
            display: flex;
            align-items: center;
            gap: 4px;
            font-size: 0.7em;
            color: #aaa;
        }
        .map-hop-legend-dot {
            width: 8px;
            height: 8px;
            border-radius: 50%;
            flex-shrink: 0;
        }
        .legend-toggle {
            background: rgba(0, 212, 255, 0.15);
            border: 1px solid rgba(0, 212, 255, 0.3);
            color: #00d4ff;
            font-size: 0.6em;
            padding: 2px 6px;
            border-radius: 4px;
            cursor: pointer;
            margin-left: auto;
            text-transform: uppercase;
            letter-spacing: 0.3px;
        }
        .legend-toggle:hover {
            background: rgba(0, 212, 255, 0.3);
        }
        .map-hop-legend-title-row {
            width: 100%;
            display: flex;
            align-items: center;
            margin-bottom: 2px;
        }

        /* Empty state */
        .empty-state {
            text-align: center;
            padding: 40px 20px;
            color: #555;
        }

        .empty-state-icon {
            font-size: 3em;
            margin-bottom: 12px;
        }

        /* Connection indicator */
        .connection-bar {
            padding: 8px 16px;
            background: rgba(255,71,87,0.1);
            border-bottom: 1px solid rgba(255,71,87,0.3);
            display: none;
            align-items: center;
            gap: 8px;
            font-size: 0.85em;
            color: #ff4757;
        }

        .connection-bar.show { display: flex; }

        /* Update Info in Header */
        .update-info {
            font-size: 0.75em;
            color: #666;
            gap: 6px;
        }
        .update-info span { color: #888; }
        .update-info #refresh-mode { color: #00d4ff; font-size: 0.95em; }
        .update-info .historical { color: #ffa502; }
        .header-divider { color: #444; }
        .timeline-toggle-btn {
            background: rgba(255,255,255,0.05);
            border: 1px solid #2a2a4a;
            border-radius: 12px;
            padding: 3px 10px;
            color: #888;
            font-size: 1em;
            cursor: pointer;
            transition: all 0.15s;
        }
        .timeline-toggle-btn:hover { background: rgba(0,212,255,0.1); color: #aaa; }
        .timeline-visible .timeline-toggle-btn {
            background: rgba(0,212,255,0.2);
            border-color: #00d4ff;
            color: #00d4ff;
        }

        @media (max-width: 900px) {
            .main-container {
                grid-template-columns: 1fr;
                grid-template-rows: 300px 1fr;
            }
            .map-section { grid-row: 1; }
            .sidebar { grid-row: 2; }
        }

        /* MarkerCluster overrides for dark theme */
        .marker-cluster-small {
            background-color: rgba(0,212,255,0.3);
        }
        .marker-cluster-small div {
            background-color: rgba(0,212,255,0.6);
        }
        .marker-cluster-medium {
            background-color: rgba(0,170,255,0.3);
        }
        .marker-cluster-medium div {
            background-color: rgba(0,170,255,0.6);
        }
        .marker-cluster-large {
            background-color: rgba(85,119,255,0.3);
        }
        .marker-cluster-large div {
            background-color: rgba(85,119,255,0.6);
        }
        .marker-cluster div {
            width: 30px; height: 30px; margin-left: 5px; margin-top: 5px;
            text-align: center; border-radius: 15px;
            font: 12px 'Segoe UI', sans-serif; color: #fff; font-weight: 700;
            line-height: 30px;
        }
        .marker-cluster {
            background-clip: padding-box; border-radius: 20px;
        }
    </style>
</head>
<body>
    <div class="header">
        <div class="logo">
            <img class="logo-icon" src="data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAEYAAABACAYAAACndwGZAAAS0klEQVR42u2aeXCdZ3XGf++33FVX+2o7XqVrrY6txbLsLHYSHAIJiUnSJKShQwt0KFDolBYYWgptp5ShLaFl6AxtBwK0pcMSEkJZEoKz2I4sW46txVq9yLJl7brS1V2/7zv945PlSHZIIiuEzujoD+l+uvddnvec5zznvBdWbMVWbMVWbMVWbMVWbMVWbMVWbMVWbMVWbMVWbMVW7JJV1W2T/8/rV8s1UHVdrWypriIvLxelFCKAuDNMTkxx7PgJOo61vu75SmsrpK/1pHqrgDGWY5B7H3pQ1l63msEL5zlw+BCtLx6e39C2XY0SXl9Kblb2/PvL6ipEaQpBwIbeqwBwNVDKaisE3QW898ibC9o1D/7xT/ypzMRm+I+vfV0B7Lr9ZllTshrTMInFY3R39dJx9GW1qW6zmH4TJ+3Q09y5LJsqq68UpaCnpVP9VgHzwT/6sExMj/P973xX3XTHLbKzoREHh0g0QspKEwwGyM3Mo7Ong5dPttH3UtebdspldRUC0Ht0eTxpyYPcve9eycgM8p+PfUvdce9dctuePfzkFz/l2SefVqVNmyWjyMfk5DQhMgmZmRx65oXfCF+U1lVI3zKAs+QB/vjjH5d/fvRRtaWxTu65805+/PRTxCXG3pveRk5OLpoByXiaFw8f5OL5YTRLo6u1fX6+LXW1cuJoqyqrr5DXwxdl9ZWidPdvJy28FjGX1VdK75Glh9iSyPfeBx6Uc+cHAWjYVkdzy2GOPX9Ufe6fPi+TU2M88b+PY6Vt6utrKSzMZ3hoBD3g7urW39krb/uznRiZJt96n5ITB4++rsUv3uS+P98njR/dStcTp/nmR76pXu394foq6TnS8YYB0pYCTGFhPv2nTwPg83rpG+7jnb9/l4yNjvPT539OzT1buPFju+mb6qfrbBeZa4Kk0il2PnCz7P3ybijSEK8QyPYv2dUz1gdJazZ1D1fwV8986lU1U8+RDhXeXiW/EWCUpjhx5KjacfNNkpEfJC5xSjJLGDh7lqLwKnY8UM+O22uofscWDNMkOp6gsKSIfV/aS8cvujnz8gVSSZuZi7NLBmZ6MErQ46Pjf3rwbvDywe98QADKGq8Eoedwhyp7g+AsCZivfeVfXNcMCcOTo1w4PKhO9Z1mfHICK2ERi8xiWzbTFyIkJ9Oc/NUJdesndjF5aoLun5zEl+0lMhmlo/WYKmuslHBDpZTVVsirCb2y+koJN1ZKuLFKwnMbT4wl8Ps9jI9O8tTnn2XT2zdy90f2SW9zhyq/oeaKsXoPd6iy+tcPzpIF3uYd1TI5McUL+w9S0bRFBobOYQRNsvNCPP1v+1lXtIHciXxOvtim9n36PklnC83/foTiqjxyV+Vwvv2iu+DX0DSvRrKzkQQzsSShTdkkjp+i78le9nxsB0989XG6XmxTZfWVIiK8MkP1HnHB6X0dnKMtFZjul9pV98E21d96UllJC2/ISzDfT2omzYXWEZ797n4CKsieu26T2n1V9P+8l3gyjqa8BLODpCbS15ROpwYiRCNxQnkhcq/LZKD9FLPpBPf/xQNyiXz7jp68gl96j3SocMNre47GMpjSFWZQJzGZIjacQBKAaLx0rIXbbr2VdCzN9NAEnoCJLYLmBZmxr2nO9kPHVGImRVZRJobuwTHTdD/fy9pbr7uCX8qbFoZWT0uHCjdWy5teKxkBk9hIip6WdhWurZKu1jZV0bRVzkXOMdgzTHwoApkWakLhzTSxxSIdcz2mbHuliAaarkBd+q3AAXGcy1JLBCftcsX8xEnwFnrQNRPMJOc7zrN65zq23rRdXn7+cr3WdahNbd5RI90vtc0/62luV+U7aqTrFc+W1WM276iRzudeVj0trnjrae1Q4foaUV7h9IFuNeIfJIcCdKVjejSCuUEccUhZLjBGloFmustQouh6rl117W9T6VkLZWguLhqIrlBeRfiGKindUSkAhmjoXg1RYDuQiichJRRXFlwl9NvU5kVe0vVSm9pcf3XPuSZgwg1VC07hkul+jc79xxVA1i2ZpHNSZI4VIppFRlYGIiBzKlY0sC0b0aH7QLu6+ZE9csen7pDcgiy69rcpPeBW4WILSnOlgtIVVY21Ypo6tuagGRqOJWgehRWzyFgVuup67bRDWd1CfhH1JnBMT8uV7F7eVCMnXzg+/zwjM0TvRDc56UKY8uALmGBBRknABTGpkV+YS3ZGlkuqIxEa31/NPY/u5t6/3CedP29ToYwgpq3T/Vy76n6+XfUe6FCGVwevwrHsOc9SCGClLHTTnb5810Ju6WvtVEpXi/bQrjY3Xpnel8wx4e3V0nO4fcEsmxurpevQQg/yOSb+XJOR8SEK0mtJkyQzEMDAdZk19cXUva8cJ+Kw/Q8rZfrMDImhOIkMjS0Pl+Ek75a1e4r45RcP07hvh2SvyiQyEMXRFI5PcCwH3dBRhkIsIa0sYjNxN1QOtKnwzmrpOXh5nT2H21V5U7V0Hbr8rLv5Sq/XltYHqboClPD2aulubr9iguR4ktUbV3FBP0V+Xh7TxxP89LP7Ge8e4/6/vVuyCgMkphKI7lCwMZuyt6/DyDBQlmDNptn63jLMDJNdH9vKLZ9rYNdHt7Lt4UpsyyIRSyAAtoNmgKZpeENeJs5OXfaIg+1qsRruOtR+hdgLL/KaJXnM1QTSYqDKGiqlt6VTjXVPsq5pFZpHMSj9xA86rP7dbDbVXoedcrDsNFbcQhDSKQuVFJSmMHQDpSCdsDB9GrnXhbCTwtR4lMHjQ4RyMzBtAyftEIvGsD0WwawQmaEAU32RhYtzrsIhxqKQWuQ1b9hjwleR1eHtVzK78ik2NVbI6cPnyCgOEAxlMxOYYEqbwOvxMnVxmuRMGitmo6HQlHJ3oEAhc68VSoGm3P97DAN/jkn9B6rY9nApXY/3IXEBj00qZlFYXsLs2RgdzccWbLK3pUNtXqRlul9qV2WLhF55w+X3vGFgFpfwZbVXhlVZU5WIBaZfZ+z8OKMnxjFSXoYOR1hXX0woy4vp0UFz0PS5rKNwm7kiiCOIY4MImq7QNQ1N09A0hXKE6NAMgRIvvmyDtv9uZ6JvhplTKcp2bOD4Dzvcw1pEvN2H2lRp7UIgdH3h9h3bWT6Bt5jlS2srRWkgDoi4Ieb/sldq7t7Ehg82kF0eQmzAwgVEACVoKFCaK+jEmdN1gsLVMjJ344BSeLwGTlIovXUVqUSagsFMZrrj7P+7F3n620+rsp3VIs6V8uRStnqljllw6K2XD/2aBd4lYTcPlNe9OtFMje7n2lXd3gbZ+8UmKu/byOrrS1ApHSyFZdvIHHqadilsNJQCpRSars2Do+kamgaO47hgKYVp6CjAF/IQWhNkzW2FFN6YPdfhc0++bOfCEL9acti8iAbCde7raxR4V1GNc/tRcyNvum0tDg7jQxGiU7N4dC+mboISHByUAh0DTSkcW+bvokRAzf3YtltX6WiIJTiOjaZpmJoHTTQMpTHcM47jUzS95wbpb+lUmu7yU2ltpSyUFAtDTBbtoOeoC941ASP2wlHLmuZieC4iSusrpO2HvcRG43h8BraA5diIAo/mITaVBE2b+8Al+a9wRFyQUDiOYDuCYzPvUYjCEXCUIIA/4MWf7SHaE+PQf72oyhqrRMQNU+VduEVZhMRij1+WUOptXUjESnfrP6W54dR35KR6xyd3kV+RTXLawppNMx2fxtEdUhFBxXR8GT6isVls5eAL+HFsBwE03UQEbHHAcRVuIp3ASoKOF9t2iCcTxKeSxGeS+ENeSveu5v6/vld6mzsuK1z918sKgNKrZNolAxNeVHOU1l3qwLn8kJxOcdPDu6WgPBctaZAYSzE5FCGQ5cWf4aXrmX66nziNpB0y8gJIGvZ/5QizF5N4vV7iqRhig6l7iU7FiU7F8GYZJJJpLr48Sl5BJp4MD2NDk0wORAhlZTA9MkNRfQ5Vu7aKnbTdQwI2LVrrYnGnlGJxpl1yVnKd+BWDG8qtyJSbdc+0dqvyXRslQQrDb+DxGRz+RhupmQ1cODKK5oW8mmye/vuDhHIziM3ESSQSnH5ukPzKagLKj9hCIMvH8e91YXltNuqrObV/kPG2CabPRTFXm/Q/P0Du+gwy12egmTq6R2F4DRLJFJhqPuUv7ln/ujuk3tYOtWyXYOGmKrl0RJqmsC2boCfAHZ/dxcjYJCcf76Ns5yranzpD3ImztqEEdEUw5MOb5yEQ8pC5KkTzYyfI9GZSccMGElqaMy2DzIxFCRb4mB1NYkdt8sozGewYY3YozvZ91YwOTRKdibP5HRuJdET4wWefVOFd1SLO3BcLBHpfkZHCDdXyatyy7N92CDe5GWpOsKJ0hZ206W3uVNv3NsnmO9ciYhFNzOLNN0nMplGOMNUTpeLmzRRvLSQaT5KIzDBybISxsxE0W8PINVl1YwlF+dl0/qif2dQsgSIfmkdH1yF2KsXG2g0MNA9w6sAF2g8dV6Xbq0TpypVDIijUAm4J11VJz9Ff3/c1lguYS3lPlJtixRaMoDv8tneVY/g0XvhZC3nVATImghhBH4YH8rdlMdQ1Qm5NHoaukVdYQGB3BnnxKKZmEvT5UX6dyNlZZqNRsip8pGMKSTkk0xZj1jiRH8zQ9Mg2LvZMzoe12C4g6lJ4L0jJr90MN5YTl3lVjxtV8+oz22GofYz7ah5g1YZinvjZk3QP9lGwLhfJc7BjgkzZ+At8jA5ECGZ7KSooYno0SiKWpiA3wOCZC8RmU2QNZDPcPkTk/CwleSV8+OE/4dmxX3L+9EU8OeZ8GIi4pYZaYlAso8dc4t45haaBEqhsvF4y1gYY6LjAu/fdQ3FJMe9/7x/Mr/bL//BVGUuOMZkcJNhtk5UXJJjnR9MVZmEWEz1TTKs4CTvJ+2s/xGw8yke+/yEF8M5PvFPe++DDnBk4RUviAPnh7FeQqzNXWMuSoNFYZpM599EMjZPPtamK3RvIyg8SKsngsR8/hsf08jdf+ILcsHuPfPIzn5F33XUPETXG1MA0eq5BzvosbIG0ZaNMjZJtRUyPzJJMpomkpiguKeHGW/bI/Q89JJ/+zCfpau/hxNQxMnMyKd6Sx/W768RK2q56dhvJb+1XzUq3V7oFsq5QGtiWg1f38uCX9qIFYbBrjHP9A1Ql6nlo3yPYloNNmq9841HOJvrY/Xs7yarMIjmTmmdwpUAzwDRN+p86w9GftfHI7Y+wd+defL4A7Z0d/OsTj7LurkLiEYfs9QHOPjPMD//6R6qsqVqcOWWuBPpa3tjF/vKFkjPXyRdXMPU3n1TltTVipwWf8mA6BmuvX83zv3qGI48dJDOYxcjYMP4MH5vKN+LL9zB6ZgTT4yUjO4ToCidpMzUewR/0kluRzYYzq/ne8cf4cfcP8BsBpq0JPGEv3lCQxPg0ftOc1yiOfYnnrixdfqPA9B3pVKXbKwVx0yRAdnEII0eRtC3SVppAQSZr1q8mlZglOTtNwfpCcoryKFmTwzP/+AJ6vrC2oRjdF0Dz6CjLIjoSpfeZQcLbNrJ2+2qMMyajw8PYmTGyVYiiilXYCPHYLMmkj4y1PpcjlFtnAfS3dqq3lGNEcBczd0Dh29Yza8WJJKM4hoOKK7beXkPxhvWEt1XR8O5tJOJJzLCP9Q3rmD1mc/zb/UxeHCMVnebskSGOf70P74yXwu2FJHOFYFGIm95zA9et38iq6g1satjEzOAMtsdieHiSwCoPO++7UXqb29UlFf7WZiXmO5PYSYfyuhopuT4fO26TSiXJ35TD9Kk4TrFiw841KE1jqneKrEAAn+Gh+p7NVL07TMeTPZx67jTxVXFUxMNtX9xNcTgXSTkk0jZn2iLYAbiu6To8AQ8XTgyRiscJ5PtJRFPYIZvCyuy5U1dzPZy3OCv1H+1Uas51PQGTUy+eQ+KC3+/D4zXJ3ZTBYPMF0mMWKGFsdBp/rhePaTA9FWV2Jk79QzVcf2c1I+0xbvz0dgo35jATiROdTqDFQPcoYpE4mqYYPjHCxd4hsta799eBbD+TvVGmL8bm67mlhNGbk67FrV5PvNCq+g8MEiz04/f4wQLDp1NUnsVAywDR8VlCIT+x6SRjFyJ4Aia6rTE1OkNgY4Dqt4exNGF6YhaPbqA5ivYfdeNVJqGAn7PHzjHYfZa8DVk4tqDrOqbXAA2e/fovrznbLjswfa2dypkLbG+mB103UbgNJitho0whryrEVPcU6ek0mDDSMoqKC5pfQ3k1dE0jkUjhDXgwPDqeTA/TvdMUl+ejAhqDrReIDI8TWO0jGUuC47Y7DWWge9wtbaytkL7DHeq3Bhi4XArMXIiRmIzjiAUKHGxSyTSGx6CoIofISITkWJJ19cUc/uYJRrvHkYhN30/OcOLxTgaPDGHYGp2P9zBydpy11xdztnmQaGKKzJIAknawHAtH3BYpukNsKLEsCu3/ANLKl+X2CljVAAAAAElFTkSuQmCC" alt="LoRaLlama">
            <h1>LoRaLlama Dashboard</h1>
        </div>
        <div class="header-stats">
            <div class="header-stat">
                <div class="status-dot" id="status-dot"></div>
                <span id="status-text">Connected</span>
            </div>
            <div class="header-stat">
                <span>📦</span>
                <span class="value" id="header-packets">0</span>
                <span>packets</span>
            </div>
            <div class="header-stat">
                <span>👥</span>
                <span class="value" id="header-nodes">0</span>
                <span>nodes</span>
            </div>
            <div class="header-stat">
                <span>💬</span>
                <span class="value" id="header-messages">0</span>
                <span>messages</span>
            </div>
            <div class="header-stat weather-stat" id="header-weather-stat" title="Loading weather...">
                <span id="header-weather-icon">--</span>
                <span class="value" id="header-weather-temp">--</span>
                <span class="weather-loc" id="header-weather-loc"></span>
            </div>
            <div class="header-stat update-info">
                <span>🕐 <span id="last-update">just now</span></span>
                <span class="header-divider">|</span>
                <span id="refresh-mode">Smart refresh</span>
                <span class="header-divider">|</span>
                <button class="timeline-toggle-btn" id="timeline-toggle">🕰️ Timeline</button>
            </div>
        </div>
    </div>

    <div class="connection-bar" id="connection-bar">
        ⚠️ Connection lost. Attempting to reconnect...
    </div>

    <div class="main-container">
        <div class="map-section">
            <div id="map"></div>
            <div class="map-overlay">
                <div class="map-stat map-range-bar">
                    <button class="range-btn map-range-btn active" data-range="all">ALL</button>
                    <button class="range-btn map-range-btn" data-range="7d">7D</button>
                    <button class="range-btn map-range-btn" data-range="24h">24H</button>
                    <button class="range-btn map-range-btn" data-range="1h">1H</button>
                    <button class="map-trail-btn" id="trail-toggle" onclick="toggleTrails()">Routes</button>
                    <label class="map-follow-label" id="follow-label">
                        <input type="checkbox" id="follow-checkbox" onchange="toggleFollowMode(this.checked)">
                        Follow
                    </label>
                </div>
            </div>
            <div class="map-hop-legend" id="map-legend">
                <div class="map-hop-legend-title-row">
                    <div class="map-hop-legend-title" id="legend-title">Last Heard</div>
                    <button class="legend-toggle" onclick="toggleMapColorMode()">Hops</button>
                </div>
                <div id="legend-recency-items">
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#00ff88"></span>&lt; 15 min</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#00d4ff"></span>&lt; 1 hr</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#5577ff"></span>&lt; 6 hr</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#8855dd"></span>&lt; 12 hr</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#cc44aa"></span>&lt; 24 hr</div>
                </div>
                <div id="legend-hops-items" style="display:none">
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#00ff88"></span>Direct</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#00e5a0"></span>1 hop</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#00cce0"></span>2 hops</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#00aaff"></span>3 hops</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#5577ff"></span>4+ hops</div>
                    <div class="map-hop-legend-item"><span class="map-hop-legend-dot" style="background:#888"></span>Unknown</div>
                </div>
            </div>
        </div>

        <div class="sidebar">
            <div class="tabs">
                <div class="tab active" data-tab="messages">💬 Messages<br><span class="tab-count" id="tab-messages-count">0</span></div>
                <div class="tab" data-tab="nodes">👥 Nodes<br><span class="tab-count" id="tab-nodes-count">0</span></div>
                <div class="tab" data-tab="stats">📊 Stats</div>
                <div class="tab" data-tab="network">🔗 Network</div>
            </div>

            <div class="tab-content active" id="tab-messages">
                <div class="tab-header-info">
                    <span>0</span> messages
                </div>
                <div class="msg-view-toggle">
                    <button class="msg-view-btn active" data-view="all">All</button>
                    <button class="msg-view-btn" data-view="dms">DMs</button>
                </div>
                <div id="msg-view-all" style="display:flex;">
                    <div class="msg-range-bar">
                        <button class="range-btn msg-range-btn active" data-range="all">ALL</button>
                        <button class="range-btn msg-range-btn" data-range="24h">24H</button>
                        <button class="range-btn msg-range-btn" data-range="6h">6H</button>
                        <button class="range-btn msg-range-btn" data-range="1h">1H</button>
                        <button class="range-btn msg-range-btn" data-range="30m">30M</button>
                    </div>
                    <div class="message-list" id="message-list">
                        <div class="empty-state">
                            <div class="empty-state-icon">💬</div>
                            <div>No messages yet</div>
                        </div>
                    </div>
                </div>
                <div id="msg-view-dms" style="display:none;">
                    <div class="dm-conversations" id="dm-conversations">
                        <div class="empty-state">
                            <div class="empty-state-icon">📩</div>
                            <div>No DM conversations</div>
                        </div>
                    </div>
                    <div class="dm-thread" id="dm-thread" style="display:none;">
                        <div class="dm-thread-header" id="dm-thread-header">
                            <button class="dm-thread-back" onclick="closeDMThread()">←</button>
                            <span class="dm-thread-name" id="dm-thread-name"></span>
                        </div>
                        <div class="dm-thread-messages" id="dm-thread-messages"></div>
                    </div>
                </div>
                <div class="chat-input-container">
                    <div class="chat-input-wrapper">
                        <div class="quick-btns">
                            <button class="quick-btn" onclick="sendActualWeather()" title="Send weather">☀️</button>
                            <button class="quick-btn" onclick="sendQuickTime()" title="Send time">🕐</button>
                        </div>
                        <select class="channel-select" id="channel-select" title="Channel">
                            <option value="0">CH 0</option>
                        </select>
                        <input type="text" class="chat-input" id="chat-input"
                               placeholder="Type a message to broadcast..." maxlength="200">
                        <button class="chat-send-btn" id="chat-send-btn" onclick="sendMessage()">Send</button>
                    </div>
                    <div class="chat-status">
                        <span class="char-counter" id="char-counter">0 / 200 bytes</span>
                        <span class="send-status" id="send-status"></span>
                    </div>
                </div>
            </div>

            <div class="tab-content" id="tab-nodes">
                <div class="tab-header-info">
                    <span>0</span> nodes
                    (<span id="tab-active-count">0</span> active, <span id="tab-gps-count">0</span> GPS)
                </div>
                <div class="node-controls">
                    <input type="text" class="node-search" id="node-search" placeholder="Search nodes...">
                    <select class="node-sort" id="node-sort">
                        <option value="last_seen">Last Seen</option>
                        <option value="name">Name</option>
                        <option value="hops">Hops</option>
                    </select>
                </div>
                <div class="node-list" id="node-list">
                    <div class="empty-state">
                        <div class="empty-state-icon">👥</div>
                        <div>No nodes discovered</div>
                    </div>
                </div>
            </div>

            <div class="tab-content" id="tab-stats">
                <div class="stats-panel">
                    <!-- Filter Bar -->
                    <div class="stats-filter-bar">
                        <span class="filter-label">Range:</span>
                        <button class="filter-btn" data-range="1h">1H</button>
                        <button class="filter-btn" data-range="6h">6H</button>
                        <button class="filter-btn" data-range="24h">24H</button>
                        <button class="filter-btn" data-range="7d">7D</button>
                        <button class="filter-btn active" data-range="all">ALL</button>
                    </div>

                    <!-- Key Metrics (4 across) -->
                    <div class="stats-grid-4">
                        <div class="stat-mini">
                            <div class="stat-mini-value" id="stat-rx">0</div>
                            <div class="stat-mini-label">RX Msgs</div>
                        </div>
                        <div class="stat-mini">
                            <div class="stat-mini-value" id="stat-tx">0</div>
                            <div class="stat-mini-label">TX Msgs</div>
                        </div>
                        <div class="stat-mini">
                            <div class="stat-mini-value" id="stat-nodes">0</div>
                            <div class="stat-mini-label">Nodes</div>
                        </div>
                        <div class="stat-mini">
                            <div class="stat-mini-value" id="stat-active">0</div>
                            <div class="stat-mini-label">Active</div>
                        </div>
                    </div>

                    <!-- Secondary Metrics -->
                    <div class="stats-grid-4 secondary">
                        <div class="stat-micro">
                            <div class="stat-micro-value" id="stat-packets">0</div>
                            <div class="stat-micro-label">Packets</div>
                        </div>
                        <div class="stat-micro">
                            <div class="stat-micro-value" id="stat-telemetry">0</div>
                            <div class="stat-micro-label">Telemetry</div>
                        </div>
                        <div class="stat-micro">
                            <div class="stat-micro-value" id="stat-positions">0</div>
                            <div class="stat-micro-label">Positions</div>
                        </div>
                        <div class="stat-micro">
                            <div class="stat-micro-value" id="stat-dbsize">0</div>
                            <div class="stat-micro-label">DB (MB)</div>
                        </div>
                    </div>

                    <!-- Extended Metrics -->
                    <div class="stats-grid-4 secondary">
                        <div class="stat-micro">
                            <div class="stat-micro-value" id="stat-waypoints">0</div>
                            <div class="stat-micro-label">Waypoints</div>
                        </div>
                        <div class="stat-micro">
                            <div class="stat-micro-value" id="stat-traceroutes">0</div>
                            <div class="stat-micro-label">Routes</div>
                        </div>
                        <div class="stat-micro">
                            <div class="stat-micro-value" id="stat-rangetests">0</div>
                            <div class="stat-micro-label">Range Tests</div>
                        </div>
                        <div class="stat-micro">
                            <div class="stat-micro-value" id="stat-alerts">0</div>
                            <div class="stat-micro-label">Alerts</div>
                        </div>
                    </div>

                    <!-- Network Health (collapsible) -->
                    <div class="collapsible-section">
                        <div class="collapsible-header">
                            <span class="collapse-arrow">▼</span> Network Health
                        </div>
                        <div class="collapsible-content">
                            <div class="health-label">Hop Distribution</div>
                            <div class="hop-bar" id="hop-bar">
                                <div class="hop-seg hop-0" style="width:12.5%">0</div>
                                <div class="hop-seg hop-1" style="width:12.5%">1</div>
                                <div class="hop-seg hop-2" style="width:12.5%">2</div>
                                <div class="hop-seg hop-3" style="width:12.5%">3</div>
                                <div class="hop-seg hop-4" style="width:12.5%">4</div>
                                <div class="hop-seg hop-5" style="width:12.5%">5</div>
                                <div class="hop-seg hop-6" style="width:12.5%">6</div>
                                <div class="hop-seg hop-7" style="width:12.5%">7</div>
                            </div>
                            <div class="hop-legend" id="hop-legend"></div>
                            <div class="health-metrics-row">
                                <div class="health-metric">
                                    <span class="health-metric-label">Avg SNR</span>
                                    <span class="health-metric-value" id="stat-avg-snr">--</span>
                                    <span class="health-metric-unit">dB</span>
                                </div>
                                <div class="health-metric">
                                    <span class="health-metric-label">Ch Util</span>
                                    <span class="health-metric-value" id="stat-ch-util">--</span>
                                    <span class="health-metric-unit">%</span>
                                </div>
                                <div class="health-metric">
                                    <span class="health-metric-label">Air TX</span>
                                    <span class="health-metric-value" id="stat-air-tx">--</span>
                                    <span class="health-metric-unit">%</span>
                                </div>
                            </div>
                        </div>
                    </div>

                    <!-- Top Nodes (collapsible) -->
                    <div class="collapsible-section">
                        <div class="collapsible-header">
                            <span class="collapse-arrow">▼</span> Top Nodes
                        </div>
                        <div class="collapsible-content" id="top-nodes-content">
                            <div style="color:#555;font-size:0.8em;padding:4px">Loading...</div>
                        </div>
                    </div>

                    <!-- Packet Types (horizontal bars) -->
                    <div class="packet-breakdown">
                        <h3>Packet Types</h3>
                        <div id="packet-types">
                            <div style="color:#555;font-size:0.8em;padding:4px">Loading...</div>
                        </div>
                    </div>

                    <!-- Activity Chart (improved with axes) -->
                    <div class="chart-section">
                        <h3>Activity <span class="chart-range-label" id="chart-range-label">24H</span></h3>
                        <div class="chart-wrapper">
                            <div class="chart-y-axis" id="chart-y-axis"><span>0</span></div>
                            <div class="chart-body">
                                <div class="chart-container" id="activity-chart"></div>
                                <div class="chart-x-axis" id="chart-x-axis"></div>
                            </div>
                        </div>
                    </div>

                </div>
            </div>

            <div class="tab-content" id="tab-network">
                <div class="network-panel" id="network-panel">
                    <!-- Mesh Topology -->
                    <div class="network-section">
                        <div class="network-section-header" onclick="toggleNetSection(this)">
                            <span><span class="section-arrow">▼</span> Mesh Topology</span>
                            <span id="topology-count" style="color:#00d4ff;font-size:0.9em"></span>
                        </div>
                        <div class="network-section-body">
                            <canvas id="topology-canvas"></canvas>
                        </div>
                    </div>

                    <!-- Recent Traceroutes -->
                    <div class="network-section">
                        <div class="network-section-header" onclick="toggleNetSection(this)">
                            <span><span class="section-arrow">▼</span> Recent Traceroutes</span>
                            <span style="display:flex;gap:6px;align-items:center;">
                                <span id="trace-all-status" style="font-size:0.85em;color:#00d4ff;display:none"></span>
                                <button class="trace-all-btn" id="trace-all-btn" onclick="event.stopPropagation(); startTraceAll()">Trace All</button>
                                <button class="trace-all-btn cancel" id="trace-cancel-btn" onclick="event.stopPropagation(); cancelTraceAll()" style="display:none">Cancel</button>
                            </span>
                        </div>
                        <div class="network-section-body" id="traceroutes-list">
                            <div style="color:#555;font-size:0.8em">Loading...</div>
                        </div>
                    </div>

                    <!-- Store & Forward -->
                    <div class="network-section">
                        <div class="network-section-header" onclick="toggleNetSection(this)">
                            <span><span class="section-arrow">▼</span> Store & Forward</span>
                        </div>
                        <div class="network-section-body" id="sf-stats-list">
                            <div style="color:#555;font-size:0.8em">Loading...</div>
                        </div>
                    </div>

                    <!-- Sensor Alerts -->
                    <div class="network-section">
                        <div class="network-section-header" onclick="toggleNetSection(this)">
                            <span><span class="section-arrow">▼</span> Sensor Alerts</span>
                        </div>
                        <div class="network-section-body" id="alerts-list">
                            <div style="color:#555;font-size:0.8em">Loading...</div>
                        </div>
                    </div>

                    <!-- Paxcounter -->
                    <div class="network-section">
                        <div class="network-section-header" onclick="toggleNetSection(this)">
                            <span><span class="section-arrow">▼</span> Paxcounter</span>
                        </div>
                        <div class="network-section-body" id="pax-list">
                            <div style="color:#555;font-size:0.8em">Loading...</div>
                        </div>
                    </div>
                </div>
            </div>

        </div>
    </div>

    <!-- Timeline Scrubber -->
    <div class="timeline-container">
        <div class="timeline-info">
            <span id="timeline-date">🟢 Live</span>
            <button id="sync-now-btn" class="sync-now-btn hidden" onclick="syncToNow()">
                ⚡ Sync to Now
            </button>
        </div>
        <input type="range" id="timeline-slider" class="timeline-slider"
               min="0" max="1000" value="1000" step="1">
        <div class="timeline-labels">
            <span id="timeline-start">7 days ago</span>
            <span id="timeline-end">Now</span>
        </div>
    </div>

    <!-- DM Modal -->
    <div id="dm-modal" class="dm-modal hidden">
        <div class="dm-modal-content">
            <div class="dm-header">
                <span>💬 DM to: <strong id="dm-target-name">Node</strong></span>
                <button class="dm-close-btn" onclick="closeDM()">✕</button>
            </div>
            <input type="text" id="dm-input" class="chat-input" placeholder="Type message..." maxlength="200"
                   onkeyup="updateDMByteCounter()" onkeydown="if(event.key==='Enter')sendDM()">
            <div class="dm-footer">
                <span class="byte-counter" id="dm-byte-counter">200 bytes left</span>
                <button class="dm-btn" onclick="sendDM()">Send DM</button>
            </div>
        </div>
    </div>

    <script>
        // Initialize map
        const map = L.map('map', {
            center: [30.2672, -97.7431],  // Austin, TX (change to your location)
            zoom: 10,
            zoomControl: true
        });

        // Dark map tiles
        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            attribution: '&copy; OpenStreetMap, &copy; CARTO',
            subdomains: 'abcd',
            maxZoom: 19
        }).addTo(map);

        // Custom marker icon - static colored circle (no permanent animation)
        function createMarkerIcon(color = '#00d4ff', label = '') {
            return L.divIcon({
                className: 'custom-marker',
                html: `<div style="
                    width: 24px; height: 24px;
                    background: ${color};
                    border: 3px solid #fff;
                    border-radius: 50%;
                    box-shadow: 0 2px 10px rgba(0,0,0,0.5);
                "></div>${label ? `<div class="marker-label">${label}</div>` : ''}`,
                iconSize: [24, 24],
                iconAnchor: [12, 12]
            });
        }

        // Node markers and cluster group
        let nodeMarkers = {};
        let clusterGroup = L.markerClusterGroup({
            maxClusterRadius: 40,
            spiderfyOnMaxZoom: true,
            showCoverageOnHover: false,
            zoomToBoundsOnClick: true,
            disableClusteringAtZoom: 15
        });
        map.addLayer(clusterGroup);

        // Cached nodes for search/sort
        let cachedNodes = [];
        let nodeSearchTerm = '';
        let nodeSortBy = 'last_seen';
        let nodeSearchTimeout = null;

        // Track recently-messaged nodes and last_heard values for animation detection
        let recentMessageNodes = new Set();
        let lastHeardCache = {};
        let rippleTimeouts = {};
        let followMode = false;
        let followRestoreTimeout = null;
        let followSavedView = null;
        let seenMessageIds = new Set();
        let initialMessageLoad = true;

        // Apply ripple ring DOM elements to a marker element
        function applyRippleRings(el) {
            el.querySelectorAll('.ripple-ring').forEach(r => r.remove());
            ['ripple-ring-1','ripple-ring-2','ripple-ring-3'].forEach(cls => {
                const ring = document.createElement('div');
                ring.className = 'ripple-ring ' + cls;
                el.appendChild(ring);
            });
        }

        // Animate a node marker — 'message' for text msgs, 'data' for other packets
        function animateNode(nodeId, type) {
            const marker = nodeMarkers[nodeId];
            if (!marker) return;

            if (type === 'message') {
                // Clear previous cleanup timeout
                if (rippleTimeouts[nodeId]) clearTimeout(rippleTimeouts[nodeId]);
                recentMessageNodes.add(nodeId);

                // Follow mode: zoom to active nodes (may uncluster them)
                if (followMode) {
                    // Save the current view before first follow-zoom
                    if (!followSavedView) {
                        followSavedView = { center: map.getCenter(), zoom: map.getZoom() };
                    }
                    // Reset the restore timer on each new message
                    if (followRestoreTimeout) clearTimeout(followRestoreTimeout);
                    followRestoreTimeout = setTimeout(() => {
                        if (followSavedView && followMode) {
                            map.setView(followSavedView.center, followSavedView.zoom, {animate: true});
                            followSavedView = null;
                        }
                    }, 30000);

                    const latlngs = [];
                    recentMessageNodes.forEach(nid => {
                        const m = nodeMarkers[nid];
                        if (m) latlngs.push(m.getLatLng());
                    });
                    if (latlngs.length === 1) {
                        map.setView(latlngs[0], Math.max(map.getZoom(), 15), {animate: true});
                    } else if (latlngs.length > 1) {
                        map.fitBounds(L.latLngBounds(latlngs), {padding: [60, 60], maxZoom: 17, animate: true});
                    }
                }

                // Try to apply visual now, or retry after a short delay (for follow-mode zoom unclustering)
                const applyVisual = () => {
                    const el = marker.getElement();
                    if (!el) return false;
                    el.classList.remove('node-data-glow');
                    el.classList.add('node-msg-throb');
                    applyRippleRings(el);
                    return true;
                };

                if (!applyVisual()) {
                    // Marker is clustered — retry after zoom/pan settles
                    setTimeout(() => applyVisual(), 600);
                }

                // Cleanup after 10s
                rippleTimeouts[nodeId] = setTimeout(() => {
                    const el = marker.getElement();
                    if (el) {
                        el.classList.remove('node-msg-throb');
                        el.querySelectorAll('.ripple-ring').forEach(r => r.remove());
                    }
                    recentMessageNodes.delete(nodeId);
                    delete rippleTimeouts[nodeId];
                }, 10000);
            } else {
                const el = marker.getElement();
                if (!el) return;
                // Don't override an active message throb with a data glow
                if (el.classList.contains('node-msg-throb')) return;
                el.classList.add('node-data-glow');
                setTimeout(() => {
                    el.classList.remove('node-data-glow');
                }, 3000);
            }
        }
        let lastKnownUpdate = 0;
        let lastUpdateTime = Date.now();
        let currentStatsRange = 'all';
        let currentMsgRange = 'all';
        let currentMapRange = 'all';
        let mapColorMode = 'recency';  // 'recency' or 'hops'
        let cachedMapNodes = [];

        // Socket connection
        const socket = io();

        socket.on('connect', () => {
            document.getElementById('status-dot').classList.remove('offline');
            document.getElementById('status-text').textContent = 'Connected';
            document.getElementById('connection-bar').classList.remove('show');
            refreshData();
        });

        socket.on('disconnect', () => {
            document.getElementById('status-dot').classList.add('offline');
            document.getElementById('status-text').textContent = 'Disconnected';
            document.getElementById('connection-bar').classList.add('show');
        });

        socket.on('stats_update', updateStats);
        socket.on('new_message', (msg) => {
            prependMessage(msg);
            // Throb the sending node on the map (10s animation for text messages)
            if (msg.from_id) animateNode(msg.from_id, 'message');
            refreshData();
        });

        // Tab switching
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
                document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
                tab.classList.add('active');
                document.getElementById('tab-' + tab.dataset.tab).classList.add('active');
            });
        });

        // Stats filter bar
        document.querySelectorAll('.filter-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentStatsRange = btn.dataset.range;
                refreshStatsPanel();
            });
        });

        // Collapsible sections
        document.querySelectorAll('.collapsible-header').forEach(h => {
            h.addEventListener('click', () => {
                h.classList.toggle('collapsed');
                h.nextElementSibling.classList.toggle('hidden');
            });
        });

        // Timeline toggle button — in historical mode, keep visible until sync
        document.getElementById('timeline-toggle').addEventListener('click', () => {
            if (document.body.classList.contains('historical-mode') && document.body.classList.contains('timeline-visible')) {
                return; // Don't allow hiding while in historical mode
            }
            document.body.classList.toggle('timeline-visible');
            setTimeout(() => { if (typeof map !== 'undefined') map.invalidateSize(); }, 350);
        });

        // Message range buttons
        document.querySelectorAll('.msg-range-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.msg-range-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentMsgRange = btn.dataset.range;
                fetch('/api/messages?limit=50&range=' + currentMsgRange)
                    .then(r => r.json())
                    .then(updateMessages);
            });
        });

        // Message view toggle (All / DMs)
        document.querySelectorAll('.msg-view-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.msg-view-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                const view = btn.dataset.view;
                document.getElementById('msg-view-all').style.display = view === 'all' ? 'flex' : 'none';
                document.getElementById('msg-view-dms').style.display = view === 'dms' ? 'flex' : 'none';
                if (view === 'dms') loadDMConversations();
            });
        });

        // Map range buttons
        document.querySelectorAll('.map-range-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                document.querySelectorAll('.map-range-btn').forEach(b => b.classList.remove('active'));
                btn.classList.add('active');
                currentMapRange = btn.dataset.range;
                fetch('/api/nodes?range=' + currentMapRange)
                    .then(r => r.json())
                    .then(nodes => { updateNodes(nodes); updateMap(nodes, true); });
            });
        });

        // Chat input handling
        const chatInput = document.getElementById('chat-input');
        const charCounter = document.getElementById('char-counter');
        const sendStatus = document.getElementById('send-status');

        chatInput.addEventListener('input', updateCharCounter);
        chatInput.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                sendMessage();
            }
        });

        function updateCharCounter() {
            const text = chatInput.value;
            const bytes = new TextEncoder().encode(text).length;
            charCounter.textContent = `${bytes} / 200 bytes`;

            charCounter.classList.remove('warning', 'danger');
            if (bytes > 180) {
                charCounter.classList.add('danger');
            } else if (bytes > 150) {
                charCounter.classList.add('warning');
            }
        }

        function getSelectedChannel() {
            return parseInt(document.getElementById('channel-select').value) || 0;
        }

        async function sendMessage() {
            const text = chatInput.value.trim();
            if (!text) return;

            const bytes = new TextEncoder().encode(text).length;
            if (bytes > 200) {
                sendStatus.textContent = 'Message too long!';
                sendStatus.classList.add('error');
                return;
            }

            const btn = document.getElementById('chat-send-btn');
            btn.disabled = true;
            sendStatus.textContent = 'Sending...';
            sendStatus.classList.remove('error');

            try {
                const resp = await fetch('/api/send', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ message: text, channel: getSelectedChannel() })
                });

                const data = await resp.json();

                if (data.success) {
                    sendStatus.textContent = 'Queued for sending!';
                    chatInput.value = '';
                    updateCharCounter();
                    setTimeout(() => { sendStatus.textContent = ''; }, 3000);
                } else {
                    throw new Error(data.error || 'Failed to send');
                }
            } catch (e) {
                sendStatus.textContent = 'Error: ' + e.message;
                sendStatus.classList.add('error');
            } finally {
                btn.disabled = false;
            }
        }

        // Quick-send buttons
        async function sendActualWeather() {
            try {
                sendStatus.textContent = 'Fetching weather...';
                const resp = await fetch('/api/weather');
                const data = await resp.json();
                if (data.error) throw new Error(data.error);

                const msg = 'Weather: ' + data.weather;
                await fetch('/api/send', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ message: msg, destination: '^all', channel: getSelectedChannel() })
                });
                sendStatus.textContent = 'Weather sent!';
                setTimeout(() => { sendStatus.textContent = ''; }, 3000);
            } catch (e) {
                sendStatus.textContent = 'Error: ' + e.message;
            }
        }


        async function sendQuickTime() {
            try {
                const now = new Date();
                const msg = now.toLocaleString([], {month:'short', day:'numeric', hour:'2-digit', minute:'2-digit'});
                await fetch('/api/send', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ message: msg, destination: '^all', channel: getSelectedChannel() })
                });
                sendStatus.textContent = 'Time sent!';
                setTimeout(() => { sendStatus.textContent = ''; }, 3000);
            } catch (e) {
                sendStatus.textContent = 'Error: ' + e.message;
            }
        }

        // Smart refresh - only fetch data when DB has changed
        async function checkForUpdates() {
            try {
                const resp = await fetch(`/api/check-updates?since=${lastKnownUpdate}`);
                const data = await resp.json();

                if (data.has_updates) {
                    lastKnownUpdate = data.last_update;
                    await refreshData();
                }
            } catch (e) {
                console.error('Update check failed:', e);
            }
        }

        // Data fetching - messages + nodes in parallel, stats panel refreshes separately
        async function refreshData() {
            try {
                const [messages, nodes] = await Promise.all([
                    fetch('/api/messages?limit=50&range=' + currentMsgRange).then(r => r.json()),
                    fetch('/api/nodes?range=' + currentMapRange).then(r => r.json()),
                ]);

                updateMessages(messages);
                updateNodes(nodes);
                updateMap(nodes);
                updateLastUpdate();

                // Stats panel handles its own fetching with range filter
                refreshStatsPanel();
            } catch (e) {
                console.error('Refresh failed:', e);
            }
        }

        // updateStats - called by socket events and historical mode
        function updateStats(data) {
            // Always update header counters with all-time totals (header-nodes set by updateNodes from actual list)
            document.getElementById('header-packets').textContent = data.total_packets || 0;
            document.getElementById('header-messages').textContent = data.all_time_messages || data.total_messages || 0;
            document.getElementById('tab-messages-count').textContent = data.all_time_messages || data.total_messages || 0;
            document.getElementById('tab-active-count').textContent = data.active_nodes_24h || data.active_nodes || 0;

            if (isLiveMode) {
                // In live mode, trigger full stats panel refresh from API
                refreshStatsPanel();
            } else {
                // In historical mode, directly update stat card values from the data we have
                document.getElementById('stat-rx').textContent = data.total_messages || 0;
                document.getElementById('stat-tx').textContent = data.sent_messages || 0;
                document.getElementById('stat-nodes').textContent = data.total_nodes || 0;
                document.getElementById('stat-active').textContent = data.active_nodes_24h || data.active_nodes || 0;
                document.getElementById('stat-packets').textContent = data.total_packets || 0;
                document.getElementById('stat-telemetry').textContent = data.telemetry_records || 0;
                document.getElementById('stat-positions').textContent = data.position_records || 0;
                // Update packet types if available
                if (data.packet_types) {
                    let html = '';
                    const sorted = Object.entries(data.packet_types).sort((a, b) => b[1] - a[1]);
                    const maxPkt = sorted.length > 0 ? sorted[0][1] : 1;
                    for (const [type, count] of sorted.slice(0, 6)) {
                        const shortType = type.replace('_APP', '').replace('MESSAGE', 'MSG');
                        const barWidth = Math.max((count / maxPkt) * 100, 2);
                        html += `<div class="packet-row">
                            <span class="packet-type" title="${type}">${shortType}</span>
                            <div class="packet-bar-track"><div class="packet-bar-fill" style="width:${barWidth}%"></div></div>
                            <span class="packet-count">${count}</span>
                        </div>`;
                    }
                    document.getElementById('packet-types').innerHTML = html || '';
                }
            }
        }

        // Fetch enhanced stats from new endpoint
        async function refreshStatsPanel() {
            try {
                const resp = await fetch('/api/stats-enhanced?range=' + currentStatsRange);
                const data = await resp.json();
                if (data.error) { console.error('[STATS] Error:', data.error); return; }
                updateEnhancedStats(data);
            } catch (e) {
                console.error('[STATS] Refresh failed:', e);
            }
        }

        // Update all stats panel elements
        function updateEnhancedStats(data) {
            // Header stats - always show all-time totals (header-nodes set by updateNodes() from actual list length)
            document.getElementById('header-packets').textContent = data.total_packets || 0;
            document.getElementById('header-messages').textContent = data.all_time_messages || data.total_messages || 0;
            document.getElementById('tab-messages-count').textContent = data.all_time_messages || data.total_messages || 0;
            document.getElementById('tab-active-count').textContent = data.active_nodes || 0;

            // Key metrics
            document.getElementById('stat-rx').textContent = data.total_messages || 0;
            document.getElementById('stat-tx').textContent = data.sent_messages || 0;
            document.getElementById('stat-nodes').textContent = data.total_nodes || 0;
            document.getElementById('stat-active').textContent = data.active_nodes || 0;

            // Secondary metrics
            document.getElementById('stat-packets').textContent = data.total_packets || 0;
            document.getElementById('stat-telemetry').textContent = data.telemetry_records || 0;
            document.getElementById('stat-positions').textContent = data.position_records || 0;
            document.getElementById('stat-dbsize').textContent = data.database_size_mb || 0;

            // Bottom micro stats
            // Network health - hop distribution bar (8 segments)
            const hops = data.hop_distribution || {};
            const hopTotal = hops.total || 1;
            const hopBar = document.getElementById('hop-bar');
            if (hopBar) {
                const pct = (v) => Math.max(((v || 0) / hopTotal) * 100, 0).toFixed(1);
                const hopKeys = [
                    { key: 'direct', label: 'Direct', cls: 'hop-0' },
                    { key: 'hop1', label: '1 Hop', cls: 'hop-1' },
                    { key: 'hop2', label: '2 Hops', cls: 'hop-2' },
                    { key: 'hop3', label: '3 Hops', cls: 'hop-3' },
                    { key: 'hop4', label: '4 Hops', cls: 'hop-4' },
                    { key: 'hop5', label: '5 Hops', cls: 'hop-5' },
                    { key: 'hop6', label: '6 Hops', cls: 'hop-6' },
                    { key: 'hop7', label: '7 Hops', cls: 'hop-7' }
                ];
                hopBar.innerHTML = hopKeys.map(h => {
                    const v = hops[h.key] || 0;
                    return `<div class="hop-seg ${h.cls}" style="width:${pct(v)}%" title="${h.label}: ${v} nodes">${v > 0 ? v : ''}</div>`;
                }).join('');
            }

            // Hop legend text below bar
            const hopLegend = document.getElementById('hop-legend');
            if (hopLegend) {
                hopLegend.innerHTML = `
                    <span>Direct: ${hops.direct || 0}</span>
                    <span>1h: ${hops.hop1 || 0}</span>
                    <span>2h: ${hops.hop2 || 0}</span>
                    <span>3h: ${hops.hop3 || 0}</span>
                    <span>4+: ${(hops.hop4||0)+(hops.hop5||0)+(hops.hop6||0)+(hops.hop7||0)}</span>
                `;
            }

            // Health metrics
            document.getElementById('stat-avg-snr').textContent = data.avg_snr !== null && data.avg_snr !== undefined ? data.avg_snr : '--';
            document.getElementById('stat-ch-util').textContent = data.avg_channel_util !== null && data.avg_channel_util !== undefined ? data.avg_channel_util : '--';
            document.getElementById('stat-air-tx').textContent = data.avg_air_tx !== null && data.avg_air_tx !== undefined ? data.avg_air_tx : '--';

            // Top nodes
            const topContainer = document.getElementById('top-nodes-content');
            if (data.top_nodes && data.top_nodes.length > 0) {
                let html = '';
                for (const node of data.top_nodes) {
                    const dotColor = getHopColor(node.hops);
                    const battText = node.battery !== null && node.battery !== undefined ? node.battery + '%' : '--';
                    html += `<div class="top-node-row">
                        <div class="top-node-dot" style="background:${dotColor}"></div>
                        <span class="top-node-name">${escapeHtml(node.name)}</span>
                        <span class="top-node-count">${node.msg_count} msgs</span>
                        <span class="top-node-batt">🔋${battText}</span>
                    </div>`;
                }
                topContainer.innerHTML = html;
            } else {
                topContainer.innerHTML = '<div style="color:#555;font-size:0.75em;padding:4px">No message data</div>';
            }

            // Packet types (horizontal bars)
            if (data.packet_types) {
                let html = '';
                const sorted = Object.entries(data.packet_types).sort((a, b) => b[1] - a[1]);
                const maxPkt = sorted.length > 0 ? sorted[0][1] : 1;
                for (const [type, count] of sorted.slice(0, 6)) {
                    const shortType = type.replace('_APP', '').replace('MESSAGE', 'MSG');
                    const barWidth = Math.max((count / maxPkt) * 100, 2);
                    html += `<div class="packet-row">
                        <span class="packet-type" title="${type}">${shortType}</span>
                        <div class="packet-bar-track"><div class="packet-bar-fill" style="width:${barWidth}%"></div></div>
                        <span class="packet-count">${count}</span>
                    </div>`;
                }
                document.getElementById('packet-types').innerHTML = html ||
                    '<div style="color:#555;font-size:0.75em;padding:4px">No packet data</div>';
            }

            // Activity chart
            updateEnhancedChart(data.activity, data.activity_bucket);
            document.getElementById('chart-range-label').textContent = currentStatsRange.toUpperCase();
        }

        function updateChannelSelect(messages) {
            const select = document.getElementById('channel-select');
            const currentVal = select.value;
            const seen = new Set();
            if (messages) {
                for (const msg of messages) {
                    const ch = msg.channel;
                    if (ch !== undefined && ch !== null && ch > 0) seen.add(ch);
                }
            }
            // Remove all options except CH 0
            while (select.options.length > 1) select.remove(1);
            // Add seen channels sorted
            for (const ch of [...seen].sort((a, b) => a - b)) {
                const opt = document.createElement('option');
                opt.value = ch;
                opt.textContent = 'CH ' + ch;
                select.add(opt);
            }
            // Restore selection if still valid
            if ([...select.options].some(o => o.value === currentVal)) {
                select.value = currentVal;
            } else {
                select.value = '0';
            }
        }

        function updateMessages(messages) {
            const container = document.getElementById('message-list');
            if (!messages || messages.length === 0) {
                container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">💬</div><div>No messages yet</div></div>';
                return;
            }

            let html = '';
            for (const msg of messages) {
                // Support both 'direction' (from live API) and 'is_sent' (from historical API)
                const isSent = msg.direction === 'sent' || msg.is_sent === true;
                const isDM = isSent ? (msg.to_id && msg.to_id !== '^all') : (msg.to_id && msg.to_id !== '^all' && msg.to_id !== '!ffffffff');
                const time = formatTime(msg.timestamp);
                const fromLabel = isSent
                    ? 'Assistant → ' + (msg.to_id === '^all' ? 'Broadcast' : msg.to_id || 'Unknown')
                    : (msg.from_name || msg.from_id || 'Unknown');

                html += `<div class="message-item ${isSent ? 'sent' : ''}${isDM ? ' dm' : ''}">
                    <div class="message-header">
                        <span class="message-from">${escapeHtml(fromLabel)}</span>
                        ${isDM ? '<span class="dm-badge">DM</span>' : ''}
                        <span class="message-time">${time}</span>
                    </div>
                    <div class="message-text">${escapeHtml(msg.text || '')}</div>
                    ${!isSent && msg.snr ? `<div class="message-meta">
                        <span>📶 ${msg.snr}dB</span>
                        <span>📻 ${msg.rssi}dBm</span>
                    </div>` : ''}
                </div>`;
            }
            container.innerHTML = html;
            updateChannelSelect(messages);

            // Detect new messages and trigger ripple animations
            const currentIds = new Set();
            for (const msg of messages) {
                const msgKey = (msg.from_id || '') + '|' + msg.timestamp;
                currentIds.add(msgKey);
                if (!initialMessageLoad && !seenMessageIds.has(msgKey) && msg.from_id) {
                    animateNode(msg.from_id, 'message');
                }
            }
            seenMessageIds = currentIds;
            initialMessageLoad = false;
        }

        function prependMessage(msg) {
            const container = document.getElementById('message-list');
            const time = formatTime(msg.timestamp);

            const div = document.createElement('div');
            div.className = 'message-item';
            div.innerHTML = `
                <div class="message-header">
                    <span class="message-from">${escapeHtml(msg.from_name || 'Unknown')}</span>
                    <span class="message-time">${time}</span>
                </div>
                <div class="message-text">${escapeHtml(msg.text || '')}</div>
            `;

            if (container.querySelector('.empty-state')) {
                container.innerHTML = '';
            }
            container.insertBefore(div, container.firstChild);

            // Keep max 50
            while (container.children.length > 50) {
                container.removeChild(container.lastChild);
            }
        }

        function updateNodes(nodes, fromCache) {
            // If fresh data (not from cache), store it and update header with total
            if (!fromCache && nodes) {
                cachedNodes = nodes;
                document.getElementById('header-nodes').textContent = nodes.length;
            }

            // Apply search filter
            let filtered = (fromCache ? nodes : cachedNodes).slice();
            if (nodeSearchTerm) {
                const term = nodeSearchTerm.toLowerCase();
                filtered = filtered.filter(n =>
                    (n.long_name || '').toLowerCase().includes(term) ||
                    (n.short_name || '').toLowerCase().includes(term) ||
                    (n.node_id || '').toLowerCase().includes(term)
                );
            }

            // Apply sort
            if (nodeSortBy === 'name') {
                filtered.sort((a, b) => (a.long_name || '').localeCompare(b.long_name || ''));
            } else if (nodeSortBy === 'hops') {
                filtered.sort((a, b) => {
                    const ha = a.hops_used ?? a.hops_away ?? 999;
                    const hb = b.hops_used ?? b.hops_away ?? 999;
                    return ha - hb;
                });
            } else {
                // last_seen (default) - most recent first
                filtered.sort((a, b) => (b.last_heard || 0) - (a.last_heard || 0));
            }

            const container = document.getElementById('node-list');
            let gpsCount = 0;

            if (!filtered || filtered.length === 0) {
                container.innerHTML = nodeSearchTerm
                    ? '<div class="empty-state"><div class="empty-state-icon">🔍</div><div>No nodes match search</div></div>'
                    : '<div class="empty-state"><div class="empty-state-icon">👥</div><div>No nodes discovered</div></div>';
                document.getElementById('tab-gps-count').textContent = '0';
                if (!fromCache) document.getElementById('tab-nodes-count').textContent = '0';
                return;
            }

            let html = '';
            for (const node of filtered) {
                if (node.latitude && node.longitude) gpsCount++;

                const shortName = node.short_name || '??';
                const hops = node.hops_used ?? node.hops_away ?? null;
                const hopClass = getHopClass(hops);
                const lastSeen = timeSince(node.last_heard);

                html += `<div class="node-item" data-node-id="${node.node_id}" onclick="focusNode('${node.node_id}', ${node.latitude || 0}, ${node.longitude || 0})">
                    <div class="node-avatar">${escapeHtml(shortName)}</div>
                    <div class="node-info">
                        <div class="node-name">${escapeHtml(node.long_name || 'Unknown')}</div>
                        <div class="node-id">${node.node_id}</div>
                    </div>
                    <div class="node-stats">
                        ${hops !== null ? `<div class="node-signal ${hopClass}">${getHopLabel(hops)}</div>` : ''}
                        <div class="node-lastseen">${lastSeen}</div>
                    </div>
                    ${hops !== null && hops > 0 ? `<button class="node-trace-btn" title="Request traceroute" onclick="event.stopPropagation(); requestTraceroute('${node.node_id}')">TR</button>` : ''}
                </div>`;
            }
            container.innerHTML = html;
            document.getElementById('tab-gps-count').textContent = gpsCount;
            document.getElementById('tab-nodes-count').textContent = filtered.length;
        }

        function updateMap(nodes, clearExisting = false) {
            if (!nodes) return;
            cachedMapNodes = nodes;

            // In historical mode or when requested, clear all existing markers first
            if (clearExisting || !isLiveMode) {
                clusterGroup.clearLayers();
                nodeMarkers = {};
            }

            const bounds = [];

            for (const node of nodes) {
                if (!node.latitude || !node.longitude) continue;
                if (node.latitude === 0 && node.longitude === 0) continue;

                // Get node status (color based on recency)
                const status = getNodeStatus(node);

                // Skip offline/stale nodes on the map
                if (status.stale) continue;

                bounds.push([node.latitude, node.longitude]);

                // Update or create marker
                if (nodeMarkers[node.node_id]) {
                    nodeMarkers[node.node_id].setLatLng([node.latitude, node.longitude]);
                    nodeMarkers[node.node_id].setIcon(createMarkerIcon(status.color, escapeHtml(node.short_name || '')));
                    // Re-apply ripple animation if setIcon wiped the DOM
                    if (recentMessageNodes.has(node.node_id)) {
                        const el = nodeMarkers[node.node_id].getElement();
                        if (el) {
                            el.classList.add('node-msg-throb');
                            applyRippleRings(el);
                        }
                    }
                } else {
                    const marker = L.marker([node.latitude, node.longitude], {
                        icon: createMarkerIcon(status.color, escapeHtml(node.short_name || ''))
                    });
                    clusterGroup.addLayer(marker);
                    nodeMarkers[node.node_id] = marker;
                }

                // Detect non-message data activity (last_heard changed but no recent text message)
                if (node.last_heard && lastHeardCache[node.node_id] !== undefined) {
                    if (node.last_heard !== lastHeardCache[node.node_id] && !recentMessageNodes.has(node.node_id)) {
                        animateNode(node.node_id, 'data');
                    }
                }
                lastHeardCache[node.node_id] = node.last_heard;

                // Update popup content (always refresh to show latest data)
                const safeName = escapeHtml(node.long_name || node.short_name || 'Unknown');
                const hops = node.hops_used ?? node.hops_away ?? null;
                const hopColorDot = `<span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:${getHopColor(hops)};margin-right:4px;vertical-align:middle;"></span>`;
                const voltText = node.voltage ? ` (${node.voltage}V)` : '';
                const popupContent = `<div class="node-popup">
                    <h4>${safeName}</h4>
                    <div class="node-popup-id">${node.node_id}</div>
                    ${status.reason ? `<div class="node-popup-row" style="color: ${status.color}">
                        <span>⚠️ ${status.reason}</span>
                    </div>` : ''}
                    <div class="node-popup-section">
                    ${hops !== null ? `<div class="node-popup-row">
                        <span class="node-popup-label">Hops</span>
                        <span class="node-popup-value">${hopColorDot}${getHopLabel(hops)}</span>
                    </div>` : ''}
                    ${node.snr !== null && node.snr !== undefined ? `<div class="node-popup-row">
                        <span class="node-popup-label">SNR</span>
                        <span class="node-popup-value">${node.snr} dB</span>
                    </div>` : ''}
                    ${node.battery_level ? `<div class="node-popup-row">
                        <span class="node-popup-label">Battery</span>
                        <span class="node-popup-value">${node.battery_level}%${voltText}</span>
                    </div>` : ''}
                    ${node.hw_model ? `<div class="node-popup-row">
                        <span class="node-popup-label">Hardware</span>
                        <span class="node-popup-value">${node.hw_model}</span>
                    </div>` : ''}
                    ${node.role ? `<div class="node-popup-row">
                        <span class="node-popup-label">Role</span>
                        <span class="node-popup-value">${node.role}</span>
                    </div>` : ''}
                    ${node.channel_utilization ? `<div class="node-popup-row">
                        <span class="node-popup-label">Ch Util</span>
                        <span class="node-popup-value">${node.channel_utilization}%</span>
                    </div>` : ''}
                    ${node.air_util_tx ? `<div class="node-popup-row">
                        <span class="node-popup-label">Air TX</span>
                        <span class="node-popup-value">${node.air_util_tx}%</span>
                    </div>` : ''}
                    </div>
                    <div class="sparkline-container" id="spark-${node.node_id}"></div>
                    <div class="node-popup-traceroutes" id="tr-${node.node_id}"></div>
                    <div class="node-popup-actions">
                        <button class="btn-dm" onclick="openDM('${node.node_id}', '${safeName.replace(/'/g, "\\\\'")}')">DM</button>
                        ${hops !== null && hops > 0 ? `<button class="btn-trace" onclick="requestTraceroute('${node.node_id}')">Trace</button>` : ''}
                        <button class="btn-detail" onclick="showNodeDetail('${node.node_id}')">Detail</button>
                        <button class="btn-center" onclick="map.setView([${node.latitude},${node.longitude}],15)">Center</button>
                    </div>
                </div>`;

                nodeMarkers[node.node_id].bindPopup(popupContent);

                // Load sparkline and traceroutes when popup opens
                nodeMarkers[node.node_id].off('popupopen').on('popupopen', () => {
                    loadPopupSparkline(node.node_id);
                    loadPopupTraceroutes(node.node_id);
                });
            }

            // Fit bounds if we have nodes (only on initial load)
            if (bounds.length > 0 && !window.mapInitialized) {
                map.fitBounds(bounds, { padding: [50, 50], maxZoom: 12 });
                window.mapInitialized = true;
            }
        }

        function focusNode(nodeId, lat, lon) {
            if (lat && lon && lat !== 0 && lon !== 0) {
                map.setView([lat, lon], 14);
                if (nodeMarkers[nodeId]) {
                    nodeMarkers[nodeId].openPopup();
                }
            }
        }

        // Switch to Nodes tab and scroll to a specific node
        function showNodeDetail(nodeId) {
            // Switch to nodes tab
            document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
            const nodesTab = document.querySelector('.tab[data-tab="nodes"]');
            if (nodesTab) nodesTab.classList.add('active');
            document.getElementById('tab-nodes').classList.add('active');
            // Close popup
            map.closePopup();
            // Scroll to node in list
            setTimeout(() => {
                const nodeEl = document.querySelector(`.node-item[data-node-id="${nodeId}"]`);
                if (nodeEl) {
                    nodeEl.scrollIntoView({ behavior: 'smooth', block: 'center' });
                    nodeEl.style.background = 'rgba(0,212,255,0.15)';
                    setTimeout(() => { nodeEl.style.background = ''; }, 2000);
                }
            }, 100);
        }

        // Load sparkline into popup on demand
        async function loadPopupSparkline(nodeId) {
            const container = document.getElementById('spark-' + nodeId);
            if (!container) return;
            try {
                const resp = await fetch('/api/signal-trends?node_id=' + encodeURIComponent(nodeId) + '&hours=24');
                const data = await resp.json();
                const svg = buildSparklineSVG(data);
                if (svg) {
                    container.innerHTML = '<div class="sparkline-label">SNR (24h)</div>' + svg;
                }
            } catch (e) {
                console.error('[SPARK] Failed:', e);
            }
        }

        // Load traceroutes for a node into popup on demand
        async function loadPopupTraceroutes(nodeId) {
            const container = document.getElementById('tr-' + nodeId);
            if (!container) return;
            try {
                const resp = await fetch('/api/traceroutes?node_id=' + encodeURIComponent(nodeId) + '&limit=5');
                const data = await resp.json();
                if (!data || data.length === 0) return;
                let html = '<div style="font-size:0.7em;color:#888;text-transform:uppercase;margin-bottom:4px;">Recent Traceroutes</div>';
                for (const tr of data) {
                    const route = Array.isArray(tr.route) ? tr.route.join(' → ') : (tr.route || '?');
                    html += `<div class="popup-tr-item">${escapeHtml(route)}</div>
                             <div class="popup-tr-time">${formatTime(tr.timestamp)}</div>`;
                }
                container.innerHTML = html;
                container.classList.add('visible');
            } catch (e) {
                console.error('[TR] Failed:', e);
            }
        }

        // Legacy updateChart - still used by historical mode
        function updateChart(data) {
            if (!data || data.length === 0) return;
            // Convert old format {hour, count} to new format {label, count}
            const converted = data.map(d => ({label: d.hour || d.label, count: d.count}));
            updateEnhancedChart(converted, 'hour');
        }

        // Enhanced chart with axes, labels, tooltips, and current-hour highlight
        function updateEnhancedChart(data, bucketType) {
            const container = document.getElementById('activity-chart');
            const xAxis = document.getElementById('chart-x-axis');
            const yAxis = document.getElementById('chart-y-axis');

            if (!data || data.length === 0) {
                container.innerHTML = '<div style="color:#555;margin:auto;font-size:0.75em;">No activity data</div>';
                if (xAxis) xAxis.innerHTML = '';
                if (yAxis) yAxis.innerHTML = '<span>0</span>';
                return;
            }

            const maxVal = Math.max(...data.map(d => d.count), 1);
            const currentHour = new Date().getHours().toString().padStart(2, '0');

            // Y-axis labels
            if (yAxis) {
                yAxis.innerHTML = `<span>${maxVal}</span><span>${Math.round(maxVal / 2)}</span><span>0</span>`;
            }

            // Bars with tooltips
            let barsHtml = '';
            for (const item of data) {
                const height = Math.max((item.count / maxVal) * 100, 2);
                const isCurrent = (bucketType === 'hour' && item.label === currentHour);
                const tooltipLabel = bucketType === 'hour' ? item.label + ':00' : item.label;
                barsHtml += `<div class="chart-bar ${isCurrent ? 'current-hour' : ''}"
                    style="height:${height}%"
                    data-tooltip="${tooltipLabel}: ${item.count}"
                    title="${tooltipLabel} - ${item.count} msgs"></div>`;
            }
            container.innerHTML = barsHtml;

            // X-axis labels (adaptive spacing)
            if (xAxis) {
                let xHtml = '';
                const labelInterval = data.length <= 7 ? 1 :
                                      data.length <= 12 ? 2 :
                                      data.length <= 24 ? 4 : 6;
                for (let i = 0; i < data.length; i++) {
                    if (i % labelInterval === 0) {
                        const label = bucketType === 'hour' ? data[i].label + ':00' : data[i].label;
                        xHtml += `<span>${label}</span>`;
                    }
                }
                xAxis.innerHTML = xHtml;
            }
        }

        function updateLastUpdate() {
            lastUpdateTime = Date.now();
            document.getElementById('last-update').textContent = 'just now';
        }

        function getHopClass(hops) {
            if (hops === null || hops === undefined) return '';
            if (hops === 0) return 'signal-excellent';   // Direct
            if (hops === 1) return 'signal-good';         // 1 hop
            if (hops === 2) return 'signal-okay';         // 2 hops
            if (hops === 3) return 'signal-fair';         // 3 hops
            return 'signal-weak';                          // 4+ hops
        }

        function getHopColor(hops) {
            if (hops === null || hops === undefined) return '#00d4ff';
            const colors = [
                '#00ff88', '#00e5a0', '#00cce0', '#00aaff',
                '#5577ff', '#8855dd', '#cc44aa', '#ff3366'
            ];
            return colors[Math.min(hops, 7)];
        }

        function getHopLabel(hops) {
            if (hops === null || hops === undefined) return '? hops';
            if (hops === 0) return 'Direct';
            return hops + (hops === 1 ? ' hop' : ' hops');
        }

        // Recency-based color: green → cyan → blue → purple → pink
        function getRecencyColor(node) {
            if (!node.last_heard) return '#666666';
            let lastHeard;
            if (typeof node.last_heard === 'number') {
                lastHeard = new Date(node.last_heard * 1000);
            } else {
                lastHeard = new Date(node.last_heard);
                if (isNaN(lastHeard.getTime())) lastHeard = new Date(node.last_heard + 'Z');
            }
            const mins = (Date.now() - lastHeard.getTime()) / 60000;
            if (mins < 15) return '#00ff88';    // bright green — just heard
            if (mins < 60) return '#00d4ff';    // cyan — recent
            if (mins < 360) return '#5577ff';   // blue — hours ago
            if (mins < 720) return '#8855dd';   // purple — half day
            return '#cc44aa';                   // pink — stale (12-24h)
        }

        function getNodeStatus(node) {
            // 1. Offline 24h+ = hidden (filtered in updateMap), but fallback grey
            if (node.last_heard) {
                let lastHeard;
                if (typeof node.last_heard === 'number') {
                    lastHeard = new Date(node.last_heard * 1000);
                } else {
                    lastHeard = new Date(node.last_heard);
                    if (isNaN(lastHeard.getTime())) lastHeard = new Date(node.last_heard + 'Z');
                }
                const hoursSince = (Date.now() - lastHeard.getTime()) / 3600000;
                if (hoursSince > 24) {
                    return { color: '#444444', reason: 'Offline 24h+', stale: true };
                }
            } else {
                return { color: '#444444', reason: 'Never heard', stale: true };
            }

            // 2. Low battery warning (still uses recency color, just adds reason)
            let reason = null;
            if (node.battery_level !== null && node.battery_level !== undefined && node.battery_level < 20) {
                reason = 'Low battery: ' + node.battery_level + '%';
            }

            // 3. Color based on current mode
            const color = mapColorMode === 'hops'
                ? getHopColor(node.hops_used ?? node.hops_away ?? null)
                : getRecencyColor(node);
            return { color: color, reason: reason, stale: false };
        }

        function toggleMapColorMode() {
            const btn = document.querySelector('.legend-toggle');
            const recencyItems = document.getElementById('legend-recency-items');
            const hopsItems = document.getElementById('legend-hops-items');
            const title = document.getElementById('legend-title');
            if (mapColorMode === 'recency') {
                mapColorMode = 'hops';
                title.textContent = 'Hop Distance';
                btn.textContent = 'Last Heard';
                recencyItems.style.display = 'none';
                hopsItems.style.display = '';
            } else {
                mapColorMode = 'recency';
                title.textContent = 'Last Heard';
                btn.textContent = 'Hops';
                recencyItems.style.display = '';
                hopsItems.style.display = 'none';
            }
            // Re-render map markers with new colors
            if (cachedMapNodes.length) updateMap(cachedMapNodes, true);
        }

        // Fixed timestamp parsing - handles Unix timestamps and ISO strings
        function timeSince(timestamp) {
            if (!timestamp) return 'Never';

            let date;

            // Check if it's a Unix timestamp (number or numeric string)
            if (typeof timestamp === 'number' || /^\\d+$/.test(timestamp)) {
                const ts = typeof timestamp === 'number' ? timestamp : parseInt(timestamp);
                // Unix timestamps from Meshtastic are in seconds
                date = new Date(ts * 1000);
            } else {
                // Try parsing as ISO string
                date = new Date(timestamp);
                // If invalid, try adding 'Z' for UTC
                if (isNaN(date.getTime())) {
                    date = new Date(timestamp + 'Z');
                }
            }

            if (isNaN(date.getTime())) return 'Unknown';

            const seconds = Math.floor((Date.now() - date.getTime()) / 1000);

            if (seconds < 0) return 'Just now';
            if (seconds < 60) return 'Now';
            if (seconds < 3600) return Math.floor(seconds / 60) + 'm ago';
            if (seconds < 86400) return Math.floor(seconds / 3600) + 'h ago';
            return Math.floor(seconds / 86400) + 'd ago';
        }

        function formatTime(timestamp) {
            if (!timestamp) return '';
            const date = new Date(timestamp);
            if (isNaN(date.getTime())) return '';
            return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
        }

        function escapeHtml(text) {
            if (!text) return '';
            const div = document.createElement('div');
            div.textContent = text;
            return div.innerHTML;
        }

        // ==================== TIMELINE SCRUBBER ====================
        let isLiveMode = true;
        let timelineMin = Date.now() - (7 * 24 * 60 * 60 * 1000);  // Default: 7 days ago
        let timelineMax = Date.now();

        async function initTimeline() {
            try {
                const resp = await fetch('/api/time-range');
                const data = await resp.json();

                if (data.earliest) {
                    timelineMin = new Date(data.earliest).getTime();
                }
                timelineMax = Date.now();

                // Update labels
                const startDate = new Date(timelineMin);
                document.getElementById('timeline-start').textContent = formatDateShort(startDate);
            } catch (e) {
                console.error('Failed to init timeline:', e);
            }
        }

        function formatDateShort(date) {
            const now = new Date();
            const diffDays = Math.floor((now - date) / (24 * 60 * 60 * 1000));
            if (diffDays === 0) return 'Today';
            if (diffDays === 1) return 'Yesterday';
            if (diffDays < 7) return diffDays + ' days ago';
            return date.toLocaleDateString();
        }

        function formatDateTime(date) {
            return date.toLocaleString([], {
                month: 'short', day: 'numeric',
                hour: '2-digit', minute: '2-digit'
            });
        }

        // Build local ISO string (matches how Python stores timestamps with datetime.now().isoformat())
        function toLocalISO(date) {
            const pad = n => String(n).padStart(2, '0');
            return date.getFullYear() + '-' + pad(date.getMonth()+1) + '-' + pad(date.getDate())
                 + 'T' + pad(date.getHours()) + ':' + pad(date.getMinutes()) + ':' + pad(date.getSeconds());
        }

        // Timeline slider event - debounced so it waits until user stops dragging
        let timelineDebounceTimer = null;
        document.getElementById('timeline-slider').addEventListener('input', (e) => {
            const pct = e.target.value / 1000;
            const targetTime = timelineMin + (timelineMax - timelineMin) * pct;
            const targetDate = new Date(targetTime);

            if (pct >= 0.995) {
                // Close to end = return to live mode
                if (timelineDebounceTimer) clearTimeout(timelineDebounceTimer);
                syncToNow();
                return;
            }

            // Update UI immediately (responsive feel)
            isLiveMode = false;
            document.body.classList.add('historical-mode');
            document.getElementById('sync-now-btn').classList.remove('hidden');
            document.getElementById('timeline-date').textContent = '📅 ' + formatDateTime(targetDate);
            document.getElementById('timeline-date').classList.add('historical');
            document.getElementById('refresh-mode').textContent = 'Loading...';
            // Disable all range/filter buttons in historical mode
            document.querySelectorAll('.filter-btn').forEach(b => b.disabled = true);
            document.querySelectorAll('.range-btn').forEach(b => b.disabled = true);

            // Debounce the actual data fetch - wait 300ms after user stops dragging
            if (timelineDebounceTimer) clearTimeout(timelineDebounceTimer);
            timelineDebounceTimer = setTimeout(async () => {
                document.getElementById('refresh-mode').textContent = 'Historical view';
                await loadHistoricalData(toLocalISO(targetDate));
            }, 300);
        });

        async function loadHistoricalData(timestamp) {
            console.log('[TIMELINE] Loading historical data for:', timestamp);
            try {
                const resp = await fetch('/api/historical?at=' + encodeURIComponent(timestamp));
                const data = await resp.json();
                console.log('[TIMELINE] Received:', data.messages?.length || 0, 'messages,', data.nodes?.length || 0, 'nodes');

                if (data.error) {
                    console.error('[TIMELINE] Error:', data.error);
                    return;
                }

                // Update ALL dashboard sections - same as refreshData() does
                // 1. Messages list
                updateMessages(data.messages || []);

                // 2. Node list sidebar
                updateNodes(data.nodes || []);

                // 3. Map markers (clear old ones for historical)
                updateMap(data.nodes || [], true);

                // 4. Full stats (header, tabs, sidebar stats, packet types)
                if (data.stats) {
                    updateStats(data.stats);
                }

                // 5. Activity chart (if provided)
                if (data.activity) {
                    updateChart(data.activity);
                }

                // 6. Update the last-update indicator
                document.getElementById('last-update').textContent = 'historical';

            } catch (e) {
                console.error('[TIMELINE] Failed to load:', e);
            }
        }

        function syncToNow() {
            isLiveMode = true;
            document.body.classList.remove('historical-mode');
            document.body.classList.remove('timeline-visible');
            document.getElementById('sync-now-btn').classList.add('hidden');
            document.getElementById('timeline-date').textContent = '🟢 Live';
            document.getElementById('timeline-date').classList.remove('historical');
            document.getElementById('timeline-slider').value = 1000;
            document.getElementById('refresh-mode').textContent = 'Smart refresh';
            setTimeout(() => { if (typeof map !== 'undefined') map.invalidateSize(); }, 350);

            // Re-enable all filter/range buttons
            document.querySelectorAll('.filter-btn').forEach(b => b.disabled = false);
            document.querySelectorAll('.range-btn').forEach(b => b.disabled = false);

            refreshData();  // Load current live data
        }

        // ==================== DM (DIRECT MESSAGE) ====================
        let dmTargetId = null;

        function openDM(nodeId, nodeName) {
            dmTargetId = nodeId;
            document.getElementById('dm-target-name').textContent = nodeName;
            document.getElementById('dm-modal').classList.remove('hidden');
            document.getElementById('dm-input').value = '';
            document.getElementById('dm-input').focus();
            updateDMByteCounter();
        }

        // ==================== TRACE ALL ====================
        let traceAllRunning = false;
        let traceAllCancelled = false;

        async function startTraceAll() {
            if (traceAllRunning) return;

            // Filter to nodes with positions, skip direct connections (0 hops)
            const targets = cachedMapNodes.filter(n =>
                n.latitude && n.longitude &&
                n.latitude !== 0 && n.longitude !== 0 &&
                (n.hops_used ?? n.hops_away ?? 1) > 0
            );

            if (targets.length === 0) {
                showToast('No nodes with GPS positions to trace', true);
                return;
            }

            traceAllRunning = true;
            traceAllCancelled = false;
            document.getElementById('trace-all-btn').style.display = 'none';
            document.getElementById('trace-cancel-btn').style.display = '';
            const statusEl = document.getElementById('trace-all-status');
            statusEl.style.display = '';

            let completed = 0;
            let failed = 0;
            for (const node of targets) {
                if (traceAllCancelled) break;

                statusEl.textContent = `${completed + 1}/${targets.length}`;

                let ok = false;
                try {
                    const resp = await fetch('/api/request-traceroute', {
                        method: 'POST',
                        headers: {'Content-Type': 'application/json'},
                        body: JSON.stringify({node_id: node.node_id})
                    });
                    const data = await resp.json();
                    ok = !!data.success;
                    if (!ok) {
                        console.warn('[TRACE-ALL] Failed for', node.node_id, data.error);
                        failed++;
                    }
                } catch (e) {
                    console.error('[TRACE-ALL] Error for', node.node_id, e);
                    failed++;
                }

                completed++;

                // Wait between successful requests to avoid flooding the mesh
                // Skip wait on failure — move on immediately
                if (ok && !traceAllCancelled && completed < targets.length) {
                    // 30s delay — LoRa traceroute round-trips can take 30-60s on multi-hop
                    await new Promise(r => setTimeout(r, 30000));
                }
            }

            traceAllRunning = false;
            document.getElementById('trace-all-btn').style.display = '';
            document.getElementById('trace-cancel-btn').style.display = 'none';
            statusEl.style.display = 'none';

            if (traceAllCancelled) {
                showToast(`Trace All cancelled after ${completed}/${targets.length} nodes`);
            } else {
                const failStr = failed > 0 ? `, ${failed} failed` : '';
                showToast(`Trace All complete: ${completed - failed} traced${failStr}`);
            }

            // Refresh network tab and route lines
            if (networkTabActive) refreshNetworkTab();
            if (trailsEnabled) loadTrails();
        }

        function cancelTraceAll() {
            traceAllCancelled = true;
            document.getElementById('trace-all-status').textContent = 'cancelling...';
        }

        function closeDM() {
            document.getElementById('dm-modal').classList.add('hidden');
            dmTargetId = null;
        }

        async function requestTraceroute(nodeId) {
            try {
                const resp = await fetch('/api/request-traceroute', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({node_id: nodeId})
                });
                const data = await resp.json();
                if (data.success) {
                    showToast('Traceroute requested to ' + nodeId);
                } else {
                    showToast('Failed: ' + (data.error || 'Unknown error'), true);
                }
            } catch (e) {
                showToast('Error: ' + e.message, true);
            }
        }

        async function loadDMConversations() {
            try {
                const resp = await fetch('/api/dm-conversations');
                const convos = await resp.json();
                const container = document.getElementById('dm-conversations');

                if (!convos || convos.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">📩</div><div>No DM conversations</div></div>';
                    return;
                }

                let html = '';
                for (const c of convos) {
                    const shortName = (c.name || '??').substring(0, 4);
                    const total = (c.received_count || 0) + (c.sent_count || 0);
                    const time = formatTime(c.last_timestamp);
                    html += `<div class="dm-conv-item" onclick="openDMThread('${c.node_id}', '${escapeHtml(c.name).replace(/'/g, "\\'")}')">
                        <div class="dm-conv-avatar">${escapeHtml(shortName)}</div>
                        <div class="dm-conv-info">
                            <div class="dm-conv-name">${escapeHtml(c.name)}</div>
                            <div class="dm-conv-preview">${escapeHtml(c.last_text || '')}</div>
                        </div>
                        <div class="dm-conv-meta">
                            <div class="dm-conv-time">${time}</div>
                            <div class="dm-conv-count">${total}</div>
                        </div>
                    </div>`;
                }
                container.innerHTML = html;
            } catch (e) {
                console.error('Failed to load DM conversations:', e);
            }
        }

        async function openDMThread(nodeId, nodeName) {
            document.getElementById('dm-conversations').style.display = 'none';
            document.getElementById('dm-thread').style.display = 'flex';
            document.getElementById('dm-thread-name').textContent = nodeName + ' (' + nodeId + ')';

            try {
                const resp = await fetch('/api/dm-thread?node_id=' + encodeURIComponent(nodeId));
                const messages = await resp.json();
                const container = document.getElementById('dm-thread-messages');

                if (!messages || messages.length === 0) {
                    container.innerHTML = '<div class="empty-state"><div class="empty-state-icon">💬</div><div>No messages</div></div>';
                    return;
                }

                let html = '';
                for (const msg of messages) {
                    const isSent = msg.direction === 'sent';
                    const time = formatTime(msg.timestamp);
                    const from = isSent ? 'Assistant' : (msg.from_name || msg.from_id || 'Unknown');
                    html += `<div class="message-item ${isSent ? 'sent' : ''}">
                        <div class="message-header">
                            <span class="message-from">${escapeHtml(from)}</span>
                            <span class="message-time">${time}</span>
                        </div>
                        <div class="message-text">${escapeHtml(msg.text || '')}</div>
                    </div>`;
                }
                container.innerHTML = html;
            } catch (e) {
                console.error('Failed to load DM thread:', e);
            }
        }

        function closeDMThread() {
            document.getElementById('dm-thread').style.display = 'none';
            document.getElementById('dm-conversations').style.display = '';
        }

        function showToast(msg, isError) {
            let toast = document.getElementById('toast-msg');
            if (!toast) {
                toast = document.createElement('div');
                toast.id = 'toast-msg';
                toast.style.cssText = 'position:fixed;bottom:24px;left:50%;transform:translateX(-50%);padding:10px 24px;border-radius:8px;font-size:0.9em;z-index:9999;transition:opacity 0.3s;pointer-events:none;';
                document.body.appendChild(toast);
            }
            toast.textContent = msg;
            toast.style.background = isError ? 'rgba(255,71,87,0.95)' : 'rgba(0,212,255,0.95)';
            toast.style.color = '#fff';
            toast.style.opacity = '1';
            clearTimeout(toast._timer);
            toast._timer = setTimeout(() => { toast.style.opacity = '0'; }, 3000);
        }

        function updateDMByteCounter() {
            const text = document.getElementById('dm-input').value;
            const bytes = new TextEncoder().encode(text).length;
            const counter = document.getElementById('dm-byte-counter');
            counter.textContent = (200 - bytes) + ' bytes left';

            counter.classList.remove('warning', 'danger');
            if (bytes > 180) {
                counter.classList.add('danger');
            } else if (bytes > 150) {
                counter.classList.add('warning');
            }
        }

        async function sendDM() {
            const text = document.getElementById('dm-input').value.trim();
            if (!text || !dmTargetId) return;

            const bytes = new TextEncoder().encode(text).length;
            if (bytes > 200) {
                alert('Message too long! Max 200 bytes.');
                return;
            }

            try {
                const resp = await fetch('/api/send', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        message: text,
                        destination: dmTargetId,
                        channel: getSelectedChannel()
                    })
                });

                const data = await resp.json();

                if (data.success) {
                    closeDM();
                    // Brief notification
                    const targetName = document.getElementById('dm-target-name').textContent;
                    console.log('DM queued to ' + targetName);
                } else {
                    alert('Failed to send: ' + (data.error || 'Unknown error'));
                }
            } catch (e) {
                alert('Error sending DM: ' + e.message);
            }
        }

        // Close DM modal on escape key
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                closeDM();
            }
        });

        // Close DM modal when clicking outside
        document.getElementById('dm-modal').addEventListener('click', (e) => {
            if (e.target.id === 'dm-modal') {
                closeDM();
            }
        });

        // ==================== NODE SEARCH & SORT ====================

        document.getElementById('node-search').addEventListener('input', (e) => {
            clearTimeout(nodeSearchTimeout);
            nodeSearchTimeout = setTimeout(() => {
                nodeSearchTerm = e.target.value.trim();
                updateNodes(cachedNodes, true);
            }, 200);
        });

        document.getElementById('node-sort').addEventListener('change', (e) => {
            nodeSortBy = e.target.value;
            updateNodes(cachedNodes, true);
        });

        // ==================== REFRESH & INTERVALS ====================

        // Smart refresh - check for updates every 5 seconds (only in live mode)
        setInterval(() => {
            if (isLiveMode) {
                checkForUpdates();
            }
        }, 5000);

        // Update "last update" display
        setInterval(() => {
            if (isLiveMode) {
                const seconds = Math.floor((Date.now() - lastUpdateTime) / 1000);
                document.getElementById('last-update').textContent = seconds < 5 ? 'just now' : seconds + 's ago';
            }
        }, 1000);

        // ==================== WEATHER WIDGET ====================
        const weatherCodeEmoji = {
            0: '☀️', 1: '🌤️', 2: '⛅', 3: '☁️',
            45: '🌫️', 48: '🌫️',
            51: '🌦️', 53: '🌦️', 55: '🌦️',
            61: '🌧️', 63: '🌧️', 65: '🌧️',
            71: '❄️', 73: '❄️', 75: '❄️',
            80: '🌦️', 81: '🌧️', 82: '🌧️',
            95: '⛈️', 96: '⛈️', 99: '⛈️'
        };

        async function fetchWeather() {
            try {
                const resp = await fetch('/api/weather');
                const data = await resp.json();
                if (data.error) throw new Error(data.error);

                const emoji = weatherCodeEmoji[data.weather_code] || '🌡️';
                document.getElementById('header-weather-icon').textContent = emoji;
                document.getElementById('header-weather-temp').textContent = Math.round(data.temp) + '°F';
                document.getElementById('header-weather-loc').textContent = data.location || '';

                const tooltip = `${data.description} | Feels like ${Math.round(data.feels_like)}°F | Humidity ${data.humidity}% | Wind ${Math.round(data.wind_speed)}mph ${data.wind_dir}`;
                document.getElementById('header-weather-stat').title = tooltip;
            } catch (e) {
                document.getElementById('header-weather-icon').textContent = '--';
                document.getElementById('header-weather-temp').textContent = '--';
                document.getElementById('header-weather-loc').textContent = '';
                document.getElementById('header-weather-stat').title = 'Weather unavailable';
            }
        }

        fetchWeather();
        setInterval(fetchWeather, 900000); // Refresh every 15 minutes

        // ==================== NETWORK TAB ====================
        let networkTabActive = false;

        function toggleNetSection(header) {
            header.classList.toggle('collapsed');
            const body = header.nextElementSibling;
            body.classList.toggle('hidden');
        }

        // Refresh network tab when activated
        document.querySelectorAll('.tab').forEach(tab => {
            tab.addEventListener('click', () => {
                if (tab.dataset.tab === 'network') {
                    networkTabActive = true;
                    refreshNetworkTab();
                } else {
                    networkTabActive = false;
                }
            });
        });

        async function refreshNetworkTab() {
            try {
                const [topology, traceroutes, sfStats, alerts, paxData] = await Promise.all([
                    fetch('/api/topology').then(r => r.json()),
                    fetch('/api/traceroutes?limit=10').then(r => r.json()),
                    fetch('/api/store-forward-stats').then(r => r.json()),
                    fetch('/api/detection-alerts?limit=10').then(r => r.json()),
                    fetch('/api/paxcounter?limit=20').then(r => r.json())
                ]);

                renderTopology(topology);
                renderTraceroutes(traceroutes);
                renderSFStats(sfStats);
                renderAlerts(alerts);
                renderPaxcounter(paxData);
            } catch (e) {
                console.error('[NETWORK] Refresh failed:', e);
            }
        }

        function renderTopology(data) {
            const canvas = document.getElementById('topology-canvas');
            if (!canvas) return;
            const ctx = canvas.getContext('2d');

            canvas.width = canvas.offsetWidth;
            canvas.height = Math.max(canvas.offsetHeight, 250);

            const nodes = data.nodes || [];
            const edges = data.edges || [];

            document.getElementById('topology-count').textContent = nodes.length + ' nodes, ' + edges.length + ' links';

            if (nodes.length === 0 || edges.length === 0) {
                ctx.fillStyle = '#555';
                ctx.font = '12px sans-serif';
                ctx.textAlign = 'center';
                ctx.fillText(edges.length === 0 ? 'No link data — run traceroutes to build topology' : 'No topology data',
                    canvas.width/2, canvas.height/2);
                return;
            }

            // Simple force-directed layout with iterative relaxation
            const nodeMap = {};
            const pad = 30;
            const layoutNodes = nodes.map((n, i) => {
                const angle = (2 * Math.PI * i) / nodes.length;
                const r = Math.min(canvas.width - pad*2, canvas.height - pad*2) * 0.35;
                const pos = {
                    id: n.node_id,
                    name: n.short_name || n.long_name || n.node_id,
                    x: canvas.width/2 + r * Math.cos(angle) + (Math.random()-0.5)*20,
                    y: canvas.height/2 + r * Math.sin(angle) + (Math.random()-0.5)*20,
                    vx: 0, vy: 0
                };
                nodeMap[n.node_id] = pos;
                return pos;
            });

            // Run force simulation (simple spring model, 60 iterations)
            for (let iter = 0; iter < 60; iter++) {
                // Repulsion between all nodes
                for (let i = 0; i < layoutNodes.length; i++) {
                    for (let j = i+1; j < layoutNodes.length; j++) {
                        const a = layoutNodes[i], b = layoutNodes[j];
                        let dx = b.x - a.x, dy = b.y - a.y;
                        let dist = Math.sqrt(dx*dx + dy*dy) || 1;
                        let force = 800 / (dist * dist);
                        a.vx -= dx/dist * force; a.vy -= dy/dist * force;
                        b.vx += dx/dist * force; b.vy += dy/dist * force;
                    }
                }
                // Attraction along edges
                for (const edge of edges) {
                    const a = nodeMap[edge.node_id], b = nodeMap[edge.neighbor_id];
                    if (!a || !b) continue;
                    let dx = b.x - a.x, dy = b.y - a.y;
                    let dist = Math.sqrt(dx*dx + dy*dy) || 1;
                    let force = (dist - 60) * 0.05;
                    a.vx += dx/dist * force; a.vy += dy/dist * force;
                    b.vx -= dx/dist * force; b.vy -= dy/dist * force;
                }
                // Apply velocity with damping and clamp to canvas
                for (const n of layoutNodes) {
                    n.x += n.vx * 0.3; n.y += n.vy * 0.3;
                    n.vx *= 0.8; n.vy *= 0.8;
                    n.x = Math.max(pad, Math.min(canvas.width - pad, n.x));
                    n.y = Math.max(pad, Math.min(canvas.height - pad, n.y));
                }
            }

            // Draw edges
            for (const edge of edges) {
                const from = nodeMap[edge.node_id], to = nodeMap[edge.neighbor_id];
                if (!from || !to) continue;
                const snr = edge.snr;
                let color = 'rgba(0,212,255,0.4)';
                if (snr !== null && snr !== undefined) {
                    if (snr > 5) color = 'rgba(0,255,136,0.6)';
                    else if (snr > 0) color = 'rgba(255,165,2,0.6)';
                    else color = 'rgba(255,71,87,0.6)';
                }
                ctx.beginPath();
                ctx.moveTo(from.x, from.y);
                ctx.lineTo(to.x, to.y);
                ctx.strokeStyle = color;
                ctx.lineWidth = 1.5;
                ctx.stroke();
            }

            // Draw nodes
            for (const node of layoutNodes) {
                ctx.beginPath();
                ctx.arc(node.x, node.y, 5, 0, Math.PI * 2);
                ctx.fillStyle = '#00d4ff';
                ctx.fill();
                ctx.strokeStyle = 'rgba(255,255,255,0.7)';
                ctx.lineWidth = 1;
                ctx.stroke();

                ctx.fillStyle = '#aaa';
                ctx.font = '8px sans-serif';
                ctx.textAlign = 'center';
                ctx.fillText(node.name, node.x, node.y + 14);
            }
        }

        function renderTraceroutes(data) {
            const container = document.getElementById('traceroutes-list');
            if (!data || !Array.isArray(data) || data.length === 0) {
                container.innerHTML = '<div style="color:#555;font-size:0.8em">No traceroute data — request traceroutes from the map</div>';
                return;
            }

            // Build name lookup from cached nodes
            const nameLookup = {};
            for (const n of cachedNodes) {
                nameLookup[n.node_id] = n.short_name || n.long_name || n.node_id;
            }
            function nodeName(id) {
                if (!id) return '?';
                // Handle numeric hop IDs
                if (typeof id === 'number') id = '!' + id.toString(16).padStart(8, '0');
                return nameLookup[id] || (id.length > 6 ? id.slice(-4) : id);
            }

            // Deduplicate: keep only the most recent per from→to pair
            const seen = new Set();
            const unique = [];
            for (const tr of data) {
                const key = (tr.from_id || '') + '>' + (tr.to_id || '');
                if (!seen.has(key)) {
                    seen.add(key);
                    unique.push(tr);
                }
            }

            let html = '';
            for (const tr of unique.slice(0, 10)) {
                const parts = [nodeName(tr.from_id)];
                const route = tr.route || [];
                if (Array.isArray(route)) {
                    for (const hop of route) parts.push(nodeName(hop));
                }
                parts.push(nodeName(tr.to_id));
                const routeStr = parts.join(' → ');

                const snrStr = tr.snr_towards && Array.isArray(tr.snr_towards)
                    ? tr.snr_towards.map(s => (s/4).toFixed(1) + 'dB').join(', ')
                    : '';

                html += `<div class="traceroute-item">
                    <div class="traceroute-route">${escapeHtml(routeStr)}</div>
                    <div><span class="traceroute-time">${formatTime(tr.timestamp)}</span>
                    ${snrStr ? `<span class="traceroute-snr"> | ${snrStr}</span>` : ''}</div>
                </div>`;
            }
            container.innerHTML = html;
        }

        function renderSFStats(data) {
            const container = document.getElementById('sf-stats-list');
            if (!data || data.length === 0) {
                container.innerHTML = '<div style="color:#555;font-size:0.8em">No store & forward data</div>';
                return;
            }

            let html = '';
            for (const sf of data) {
                html += `<div class="sf-item">
                    <span class="sf-node">${escapeHtml(sf.from_id || '?')}</span>
                    <span class="sf-stat">${sf.messages_saved || 0}/${sf.messages_max || '?'} msgs</span>
                    <span class="sf-stat">${sf.requests || 0} reqs</span>
                </div>`;
            }
            container.innerHTML = html;
        }

        function renderAlerts(data) {
            const container = document.getElementById('alerts-list');
            if (!data || data.length === 0) {
                container.innerHTML = '<div style="color:#555;font-size:0.8em">No sensor alerts</div>';
                return;
            }

            let html = '';
            for (const alert of data.slice(0, 8)) {
                html += `<div class="alert-item">
                    <div class="alert-text">${escapeHtml(alert.alert_text || '')}</div>
                    <div>
                        <span class="alert-from">${escapeHtml(alert.sensor_name || alert.from_id || '?')}</span>
                        <span class="alert-time"> | ${formatTime(alert.timestamp)}</span>
                    </div>
                </div>`;
            }
            container.innerHTML = html;
        }

        function renderPaxcounter(data) {
            const container = document.getElementById('pax-list');
            if (!data || data.length === 0) {
                container.innerHTML = '<div style="color:#555;font-size:0.8em">No paxcounter data</div>';
                return;
            }

            let html = '';
            for (const pax of data.slice(0, 10)) {
                html += `<div class="pax-item">
                    <span>${escapeHtml(pax.node_id || '?')}</span>
                    <span class="pax-counts">WiFi: ${pax.wifi_count || 0} | BLE: ${pax.ble_count || 0}</span>
                    <span style="color:#555;font-size:0.85em">${formatTime(pax.timestamp)}</span>
                </div>`;
            }
            container.innerHTML = html;
        }

        // ==================== WAYPOINT MARKERS ====================
        let waypointMarkers = [];

        function createWaypointIcon() {
            return L.divIcon({
                className: 'waypoint-marker',
                html: `<div style="
                    width: 20px; height: 20px;
                    background: #ffa502;
                    border: 2px solid #fff;
                    border-radius: 50% 50% 50% 0;
                    transform: rotate(-45deg);
                    box-shadow: 0 2px 8px rgba(0,0,0,0.5);
                "></div>`,
                iconSize: [20, 20],
                iconAnchor: [10, 20]
            });
        }

        async function loadWaypoints() {
            try {
                const resp = await fetch('/api/waypoints?active_only=true');
                const waypoints = await resp.json();

                // Clear old markers
                waypointMarkers.forEach(m => map.removeLayer(m));
                waypointMarkers = [];

                for (const wp of waypoints) {
                    if (!wp.latitude || !wp.longitude) continue;

                    const marker = L.marker([wp.latitude, wp.longitude], {
                        icon: createWaypointIcon()
                    }).addTo(map);

                    const expireStr = wp.expire ? new Date(wp.expire * 1000).toLocaleString() : 'Never';
                    marker.bindPopup(`<div class="node-popup">
                        <h4 style="color:#ffa502">${escapeHtml(wp.name || 'Waypoint')}</h4>
                        ${wp.description ? `<div class="node-popup-row"><span class="node-popup-value">${escapeHtml(wp.description)}</span></div>` : ''}
                        <div class="node-popup-row">
                            <span class="node-popup-label">Creator</span>
                            <span class="node-popup-value">${escapeHtml(wp.node_id || '?')}</span>
                        </div>
                        <div class="node-popup-row">
                            <span class="node-popup-label">Expires</span>
                            <span class="node-popup-value">${expireStr}</span>
                        </div>
                    </div>`);

                    waypointMarkers.push(marker);
                }
            } catch (e) {
                console.error('[WPT] Failed to load waypoints:', e);
            }
        }

        // ==================== TRACEROUTE ROUTE LINES ====================
        let trailsEnabled = false;
        let trailPolylines = [];
        const trailColors = ['#00d4ff', '#00ff88', '#ffa502', '#ff4757', '#a55eea', '#2ed573', '#ff6348', '#1e90ff'];

        function toggleTrails() {
            trailsEnabled = !trailsEnabled;
            const btn = document.getElementById('trail-toggle');
            btn.classList.toggle('active', trailsEnabled);

            if (trailsEnabled) {
                loadTrails();
            } else {
                clearTrails();
            }
        }

        function toggleFollowMode(checked) {
            followMode = checked;
            document.getElementById('follow-label').classList.toggle('active', checked);
            if (!checked) {
                // Restore original view immediately when unchecked
                if (followSavedView) {
                    map.setView(followSavedView.center, followSavedView.zoom, {animate: true});
                    followSavedView = null;
                }
                if (followRestoreTimeout) {
                    clearTimeout(followRestoreTimeout);
                    followRestoreTimeout = null;
                }
            }
        }

        function clearTrails() {
            trailPolylines.forEach(p => map.removeLayer(p));
            trailPolylines = [];
        }

        // Convert numeric hop ID to !hex node_id format
        function hopToNodeId(hop) {
            if (typeof hop === 'number') return '!' + hop.toString(16).padStart(8, '0');
            if (typeof hop === 'string' && !hop.startsWith('!')) return '!' + hop;
            return hop;
        }

        async function loadTrails() {
            clearTrails();
            try {
                const resp = await fetch('/api/traceroutes?limit=50');
                const traceroutes = await resp.json();
                if (!traceroutes || !Array.isArray(traceroutes)) return;

                // Build position lookup from cached map nodes
                const posLookup = {};
                for (const n of cachedMapNodes) {
                    if (n.latitude && n.longitude && n.latitude !== 0 && n.longitude !== 0) {
                        posLookup[n.node_id] = { lat: n.latitude, lng: n.longitude, name: n.short_name || n.long_name || n.node_id };
                    }
                }

                // Deduplicate: keep only the most recent traceroute per from→to pair
                const seen = new Set();
                const uniqueTraces = [];
                for (const tr of traceroutes) {
                    const key = (tr.from_id || '') + '→' + (tr.to_id || '');
                    if (!seen.has(key)) {
                        seen.add(key);
                        uniqueTraces.push(tr);
                    }
                }

                let colorIdx = 0;
                for (const tr of uniqueTraces) {
                    // Build ordered chain: from_id → route hops → to_id
                    const chain = [];
                    if (tr.from_id) chain.push(tr.from_id);
                    if (Array.isArray(tr.route)) {
                        for (const hop of tr.route) chain.push(hopToNodeId(hop));
                    }
                    if (tr.to_id) chain.push(tr.to_id);

                    // Resolve positions for each node in the chain
                    const points = [];
                    for (const nodeId of chain) {
                        const pos = posLookup[nodeId];
                        if (pos) points.push({ ...pos, nodeId });
                    }

                    if (points.length < 2) continue;

                    const color = trailColors[colorIdx % trailColors.length];
                    colorIdx++;

                    // Age-based opacity (newer traceroutes are brighter)
                    const trAge = (Date.now() - new Date(tr.timestamp).getTime()) / 3600000; // hours
                    const opacity = Math.max(0.3, Math.min(0.9, 1 - trAge / 24));

                    // Draw line segments between consecutive nodes in the route
                    for (let i = 1; i < points.length; i++) {
                        const seg = L.polyline(
                            [[points[i-1].lat, points[i-1].lng], [points[i].lat, points[i].lng]],
                            { color: color, weight: 3, opacity: opacity,
                              lineCap: 'round', lineJoin: 'round', dashArray: '8 4' }
                        ).addTo(map);
                        seg.bindTooltip(
                            `${points[i-1].name} → ${points[i].name}`,
                            { sticky: true, opacity: 0.85 }
                        );
                        trailPolylines.push(seg);
                    }

                    // Small circle at each hop to show relay nodes
                    for (let i = 1; i < points.length - 1; i++) {
                        const dot = L.circleMarker([points[i].lat, points[i].lng], {
                            radius: 4, color: color, fillColor: color, fillOpacity: 0.8, weight: 1
                        }).addTo(map);
                        dot.bindTooltip(points[i].name + ' (relay)', { permanent: false });
                        trailPolylines.push(dot);
                    }
                }
            } catch (e) {
                console.error('[ROUTES] Failed to load:', e);
            }
        }

        // ==================== SIGNAL SPARKLINE IN POPUPS ====================
        function buildSparklineSVG(data) {
            if (!data || data.length === 0) return '';

            const w = 150, h = 30;
            const values = data.map(d => d.avg_snr).filter(v => v !== null);
            if (values.length === 0) return '';

            const min = Math.min(...values);
            const max = Math.max(...values);
            const range = max - min || 1;

            let points = '';
            for (let i = 0; i < values.length; i++) {
                const x = (i / (values.length - 1 || 1)) * w;
                const y = h - ((values[i] - min) / range) * (h - 4) - 2;
                points += `${x},${y} `;
            }

            return `<div class="sparkline-container">
                <div class="sparkline-label">24h SNR trend</div>
                <svg width="${w}" height="${h}" viewBox="0 0 ${w} ${h}">
                    <polyline points="${points}" fill="none" stroke="#00d4ff" stroke-width="1.5"/>
                </svg>
            </div>`;
        }

        // Enhanced popup with sparkline - override updateMap node popup
        const originalUpdateMap = updateMap;
        updateMap = function(nodes, clearExisting) {
            originalUpdateMap(nodes, clearExisting);

            // Add sparkline fetch on popup open for each marker
            for (const nodeId in nodeMarkers) {
                const marker = nodeMarkers[nodeId];
                marker.off('popupopen');
                marker.on('popupopen', async function() {
                    try {
                        const resp = await fetch('/api/signal-trends?node_id=' + encodeURIComponent(nodeId) + '&hours=24');
                        const trends = await resp.json();
                        if (trends && trends.length > 1) {
                            const sparkline = buildSparklineSVG(trends);
                            if (sparkline) {
                                const popup = marker.getPopup();
                                const content = popup.getContent();
                                if (!content.includes('sparkline-container')) {
                                    popup.setContent(content.replace('</div></div></div>', '</div>' + sparkline + '</div></div>'));
                                }
                            }
                        }
                    } catch (e) {
                        // Silently fail - sparkline is optional
                    }
                });
            }
        };

        // ==================== ENHANCED STATS ====================
        const originalUpdateEnhancedStats = updateEnhancedStats;
        updateEnhancedStats = function(data) {
            originalUpdateEnhancedStats(data);

            // Update extended metric cards
            const el = (id) => document.getElementById(id);
            if (el('stat-waypoints')) el('stat-waypoints').textContent = data.waypoint_records || 0;
            if (el('stat-traceroutes')) el('stat-traceroutes').textContent = data.traceroute_records || 0;
            if (el('stat-rangetests')) el('stat-rangetests').textContent = data.range_test_records || 0;
            if (el('stat-alerts')) el('stat-alerts').textContent = data.detection_alerts || 0;
        };

        // ==================== ENHANCED REFRESH ====================
        // Patch refreshData to also load waypoints and refresh network tab if active
        const originalRefreshData = refreshData;
        refreshData = async function() {
            await originalRefreshData();
            loadWaypoints();
            if (trailsEnabled) loadTrails();
            if (networkTabActive) refreshNetworkTab();
        };

        // Initial load
        initTimeline();
        refreshData();
    </script>
</body>
</html>
"""


@app.route('/')
def index():
    """Serve the dashboard page."""
    return render_template_string(DASHBOARD_HTML)


@app.route('/api/stats')
def api_stats():
    """Get overall statistics."""
    global db
    if not db:
        db = MeshDatabase()

    stats = db.get_stats()

    # Get packet type counts
    try:
        conn = db._get_conn()
        cursor = conn.execute('''
            SELECT packet_type, COUNT(*) as count
            FROM raw_packets
            GROUP BY packet_type
            ORDER BY count DESC
        ''')
        packet_types = {row[0]: row[1] for row in cursor.fetchall()}
        stats['packet_types'] = packet_types
    except Exception:
        stats['packet_types'] = {}

    return jsonify(stats)


@app.route('/api/messages')
def api_messages():
    """Get recent messages."""
    global db
    if not db:
        db = MeshDatabase()

    limit = request.args.get('limit', 50, type=int)
    range_param = request.args.get('range', 'all')
    range_map = {'30m': '-30 minutes', '1h': '-1 hours', '6h': '-6 hours', '24h': '-24 hours'}
    time_filter = range_map.get(range_param)

    messages = []
    try:
        conn = db._get_conn()

        # Get received messages (exclude assistant entries — those are in sent_messages)
        if time_filter:
            cursor = conn.execute('''
                SELECT timestamp, from_id, from_name, to_id, text, snr, rssi, channel
                FROM messages
                WHERE from_id != 'assistant'
                  AND timestamp > datetime('now', 'localtime', ?)
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (time_filter, limit))
        else:
            cursor = conn.execute('''
                SELECT timestamp, from_id, from_name, to_id, text, snr, rssi, channel
                FROM messages
                WHERE from_id != 'assistant'
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (limit,))

        for row in cursor.fetchall():
            messages.append({
                'timestamp': row[0],
                'from_id': row[1],
                'from_name': row[2],
                'to_id': row[3],
                'text': row[4],
                'snr': row[5],
                'rssi': row[6],
                'channel': row[7],
                'direction': 'received'
            })

        # Get sent messages
        if time_filter:
            cursor = conn.execute('''
                SELECT timestamp, to_id, text, channel
                FROM sent_messages
                WHERE timestamp > datetime('now', 'localtime', ?)
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (time_filter, limit))
        else:
            cursor = conn.execute('''
                SELECT timestamp, to_id, text, channel
                FROM sent_messages
                ORDER BY timestamp DESC
                LIMIT ?
            ''', (limit,))

        for row in cursor.fetchall():
            messages.append({
                'timestamp': row[0],
                'to_id': row[1],
                'text': row[2],
                'channel': row[3],
                'direction': 'sent'
            })

        # Sort by timestamp
        messages.sort(key=lambda x: x['timestamp'], reverse=True)
        messages = messages[:limit]

    except Exception as e:
        print(f"Error getting messages: {e}")

    return jsonify(messages)


@app.route('/api/dm-conversations')
def api_dm_conversations():
    """Get DM conversations grouped by partner node."""
    global db
    if not db:
        db = MeshDatabase()

    conversations = []
    try:
        conn = db._get_conn()

        # Get our node ID (the bot) from sent_messages — find the most common to_id that's not ^all
        # Actually, DMs are: received messages where to_id is NOT ^all/broadcast,
        # and sent messages where to_id is NOT ^all
        # Group by conversation partner

        # Received DMs (messages sent directly to us, excluding our own assistant entries)
        cursor = conn.execute('''
            SELECT from_id, from_name, MAX(timestamp) as last_ts, COUNT(*) as count,
                   (SELECT text FROM messages m2 WHERE m2.from_id = m.from_id
                    AND m2.to_id != '^all' AND m2.to_id != '!ffffffff'
                    AND m2.from_id != 'assistant'
                    ORDER BY m2.timestamp DESC LIMIT 1) as last_text
            FROM messages m
            WHERE to_id != '^all' AND to_id != '!ffffffff' AND to_id IS NOT NULL
              AND from_id != 'assistant'
            GROUP BY from_id
            ORDER BY last_ts DESC
        ''')
        partners = {}
        for row in cursor.fetchall():
            partner_id = row[0]
            partners[partner_id] = {
                'node_id': partner_id,
                'name': row[1] or partner_id,
                'last_timestamp': row[2],
                'received_count': row[3],
                'sent_count': 0,
                'last_text': row[4] or ''
            }

        # Sent DMs (our responses)
        cursor = conn.execute('''
            SELECT to_id, MAX(timestamp) as last_ts, COUNT(*) as count,
                   (SELECT text FROM sent_messages s2 WHERE s2.to_id = s.to_id
                    AND s2.to_id != '^all'
                    ORDER BY s2.timestamp DESC LIMIT 1) as last_text
            FROM sent_messages s
            WHERE to_id != '^all' AND to_id IS NOT NULL
            GROUP BY to_id
            ORDER BY last_ts DESC
        ''')
        for row in cursor.fetchall():
            partner_id = row[0]
            if partner_id in partners:
                partners[partner_id]['sent_count'] = row[2]
                if row[1] > partners[partner_id]['last_timestamp']:
                    partners[partner_id]['last_timestamp'] = row[1]
                    partners[partner_id]['last_text'] = 'You: ' + (row[3] or '')
            else:
                # Look up name from nodes table
                node = db.get_node(partner_id)
                name = node.get('long_name', partner_id) if node else partner_id
                partners[partner_id] = {
                    'node_id': partner_id,
                    'name': name,
                    'last_timestamp': row[1],
                    'received_count': 0,
                    'sent_count': row[2],
                    'last_text': 'You: ' + (row[3] or '')
                }

        conversations = sorted(partners.values(), key=lambda x: x['last_timestamp'], reverse=True)

    except Exception as e:
        print(f"Error getting DM conversations: {e}")

    return jsonify(conversations)


@app.route('/api/dm-thread')
def api_dm_thread():
    """Get DM thread with a specific node."""
    global db
    if not db:
        db = MeshDatabase()

    node_id = request.args.get('node_id')
    if not node_id:
        return jsonify({'error': 'node_id required'}), 400

    messages = []
    try:
        conn = db._get_conn()

        # Received from this node (DMs only)
        cursor = conn.execute('''
            SELECT timestamp, from_id, from_name, to_id, text, snr, rssi
            FROM messages
            WHERE from_id = ? AND to_id != '^all' AND to_id != '!ffffffff'
            ORDER BY timestamp DESC LIMIT 100
        ''', (node_id,))
        for row in cursor.fetchall():
            messages.append({
                'timestamp': row[0], 'from_id': row[1], 'from_name': row[2],
                'to_id': row[3], 'text': row[4], 'snr': row[5], 'rssi': row[6],
                'direction': 'received'
            })

        # Sent to this node
        cursor = conn.execute('''
            SELECT timestamp, to_id, text
            FROM sent_messages
            WHERE to_id = ?
            ORDER BY timestamp DESC LIMIT 100
        ''', (node_id,))
        for row in cursor.fetchall():
            messages.append({
                'timestamp': row[0], 'to_id': row[1], 'text': row[2],
                'direction': 'sent'
            })

        messages.sort(key=lambda x: x['timestamp'], reverse=True)
        messages = messages[:100]

    except Exception as e:
        print(f"Error getting DM thread: {e}")

    return jsonify(messages)


@app.route('/api/nodes')
def api_nodes():
    """Get all known nodes."""
    global db
    if not db:
        db = MeshDatabase()

    range_param = request.args.get('range', 'all')
    hours_map = {'1h': 1, '24h': 24, '7d': 168}
    filter_hours = hours_map.get(range_param)

    nodes = []
    try:
        conn = db._get_conn()
        if filter_hours:
            cutoff = int(time.time()) - (filter_hours * 3600)
            cursor = conn.execute('''
                SELECT n.node_id, n.long_name, n.short_name, n.hw_model, n.last_heard, n.snr,
                       n.battery_level, n.latitude, n.longitude, n.hops_away,
                       n.role, n.uptime_seconds, n.channel_utilization, n.air_util_tx,
                       n.voltage, n.times_heard, n.via_mqtt, n.first_seen,
                       (SELECT m.hop_start - m.hop_limit
                        FROM messages m
                        WHERE m.from_id = n.node_id AND m.hop_start > 0
                        ORDER BY m.timestamp DESC LIMIT 1) as hops_used
                FROM nodes n
                WHERE n.last_heard > ?
                ORDER BY n.last_heard DESC
            ''', (cutoff,))
        else:
            cursor = conn.execute('''
                SELECT n.node_id, n.long_name, n.short_name, n.hw_model, n.last_heard, n.snr,
                       n.battery_level, n.latitude, n.longitude, n.hops_away,
                       n.role, n.uptime_seconds, n.channel_utilization, n.air_util_tx,
                       n.voltage, n.times_heard, n.via_mqtt, n.first_seen,
                       (SELECT m.hop_start - m.hop_limit
                        FROM messages m
                        WHERE m.from_id = n.node_id AND m.hop_start > 0
                        ORDER BY m.timestamp DESC LIMIT 1) as hops_used
                FROM nodes n
                ORDER BY n.last_heard DESC
            ''')

        for row in cursor.fetchall():
            nodes.append({
                'node_id': row[0],
                'long_name': row[1],
                'short_name': row[2],
                'hw_model': row[3],
                'last_heard': row[4],
                'snr': row[5],
                'battery_level': row[6],
                'latitude': row[7],
                'longitude': row[8],
                'hops_away': row[9],
                'role': row[10],
                'uptime_seconds': row[11],
                'channel_utilization': row[12],
                'air_util_tx': row[13],
                'voltage': row[14],
                'times_heard': row[15],
                'via_mqtt': row[16],
                'first_seen': row[17],
                'hops_used': row[18]
            })
    except Exception as e:
        print(f"Error getting nodes: {e}")

    return jsonify(nodes)


@app.route('/api/activity')
def api_activity():
    """Get hourly activity data for chart."""
    global db
    if not db:
        db = MeshDatabase()

    activity = []
    try:
        conn = db._get_conn()

        # Get message counts per hour for last 24 hours
        cursor = conn.execute('''
            SELECT strftime('%H', timestamp) as hour, COUNT(*) as count
            FROM messages
            WHERE timestamp > datetime('now', 'localtime', '-24 hours')
            GROUP BY hour
            ORDER BY hour
        ''')

        # Initialize all hours
        hour_counts = {f'{i:02d}': 0 for i in range(24)}
        for row in cursor.fetchall():
            hour_counts[row[0]] = row[1]

        activity = [{'hour': h, 'count': c} for h, c in sorted(hour_counts.items())]

    except Exception as e:
        print(f"Error getting activity: {e}")

    return jsonify(activity)


@app.route('/api/check-updates')
def api_check_updates():
    """Check if database has been updated since given timestamp."""
    global db
    if not db:
        db = MeshDatabase()

    since = request.args.get('since', 0, type=float)
    last_update = db.get_last_modified()

    return jsonify({
        'has_updates': last_update > since,
        'last_update': last_update
    })


@app.route('/api/send', methods=['POST'])
def api_send():
    """Queue a message to be sent via the bridge."""
    global db
    if not db:
        db = MeshDatabase()

    try:
        data = request.get_json()
        message = data.get('message', '').strip()
        destination = data.get('destination', '^all')
        channel = data.get('channel', 0)

        if not message:
            return jsonify({'success': False, 'error': 'Empty message'})

        # Check byte length
        if len(message.encode('utf-8')) > 200:
            return jsonify({'success': False, 'error': 'Message too long (max 200 bytes)'})

        # Determine message type: DM (PKC-encrypted) if destination is a specific node
        is_dm = destination and destination != '^all' and destination != '!ffffffff'
        msg_type = 'dm' if is_dm else 'text'

        # Add to outbox
        msg_id = db.add_to_outbox(message, destination, channel, msg_type=msg_type)

        return jsonify({
            'success': True,
            'message_id': msg_id,
            'status': 'queued',
            'type': msg_type
        })

    except Exception as e:
        print(f"Error queueing message: {e}")
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/time-range')
def api_time_range():
    """Get the available time range for historical data."""
    global db
    if not db:
        db = MeshDatabase()

    try:
        time_range = db.get_time_range(days=7)
        return jsonify(time_range)
    except Exception as e:
        print(f"Error getting time range: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/weather')
def api_weather():
    """Fetch current weather from Open-Meteo API."""
    import requests

    try:
        lat, lon = 30.2672, -97.7431  # Austin, TX (change to your location)
        location = "Austin, TX"
        url = (f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
               f"&current=temperature_2m,relative_humidity_2m,apparent_temperature,weather_code,wind_speed_10m,wind_direction_10m"
               f"&temperature_unit=fahrenheit&wind_speed_unit=mph")
        resp = requests.get(url, timeout=10)
        data = resp.json()
        current = data.get('current', {})
        temp = current.get('temperature_2m', '?')
        feels_like = current.get('apparent_temperature', '?')
        humidity = current.get('relative_humidity_2m', '?')
        wind_speed = current.get('wind_speed_10m', '?')
        wind_deg = current.get('wind_direction_10m', 0)
        weather_code = current.get('weather_code', 0)

        # Wind direction to cardinal
        directions = ['N', 'NNE', 'NE', 'ENE', 'E', 'ESE', 'SE', 'SSE',
                      'S', 'SSW', 'SW', 'WSW', 'W', 'WNW', 'NW', 'NNW']
        wind_dir = directions[int((wind_deg + 11.25) / 22.5) % 16] if isinstance(wind_deg, (int, float)) else '?'

        # Weather code to description
        weather_desc = {
            0: 'Clear', 1: 'Mostly Clear', 2: 'Partly Cloudy', 3: 'Overcast',
            45: 'Fog', 48: 'Fog', 51: 'Light Drizzle', 53: 'Drizzle', 55: 'Heavy Drizzle',
            61: 'Light Rain', 63: 'Rain', 65: 'Heavy Rain', 71: 'Light Snow', 73: 'Snow',
            75: 'Heavy Snow', 80: 'Showers', 81: 'Showers', 82: 'Heavy Showers',
            95: 'Thunderstorm', 96: 'Thunderstorm', 99: 'Severe Thunderstorm'
        }.get(weather_code, 'Unknown')

        return jsonify({
            'weather': f"{temp}F {weather_desc}",
            'temp': temp,
            'feels_like': feels_like,
            'humidity': humidity,
            'wind_speed': wind_speed,
            'wind_dir': wind_dir,
            'description': weather_desc,
            'weather_code': weather_code,
            'location': location,
            'raw': current
        })
    except Exception as e:
        print(f"Error fetching weather: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/stats-enhanced')
def api_stats_enhanced():
    """Get enhanced statistics with time range filtering. Single endpoint for entire stats panel."""
    global db
    if not db:
        db = MeshDatabase()

    range_param = request.args.get('range', '24h')

    # Map range to SQL datetime modifier
    range_map = {
        '1h': '-1 hours',
        '6h': '-6 hours',
        '24h': '-24 hours',
        '7d': '-7 days',
    }
    time_filter = range_map.get(range_param)  # None for 'all'

    try:
        conn = db._get_conn()
        result = {}

        def count_filtered(table, time_col='timestamp'):
            if time_filter:
                row = conn.execute(f'SELECT COUNT(*) FROM {table} WHERE {time_col} > datetime("now", "localtime", ?)', (time_filter,)).fetchone()
            else:
                row = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()
            return row[0] if row else 0

        # Core counts (filtered by range) - only message counts are filtered
        result['total_messages'] = count_filtered('messages')
        result['sent_messages'] = count_filtered('sent_messages')

        # Infrastructure stats - filtered by range like messages
        result['total_packets'] = count_filtered('raw_packets')
        result['telemetry_records'] = count_filtered('telemetry')
        result['position_records'] = count_filtered('positions')
        try:
            result['routing_records'] = count_filtered('routing')
        except:
            result['routing_records'] = 0
        try:
            result['filtered_messages'] = count_filtered('filtered_content')
        except:
            result['filtered_messages'] = 0

        # All-time totals for header bar (always unfiltered)
        result['all_time_messages'] = conn.execute('SELECT COUNT(*) FROM messages').fetchone()[0]
        result['all_time_sent'] = conn.execute('SELECT COUNT(*) FROM sent_messages').fetchone()[0]

        # Total nodes (always all-time)
        row = conn.execute('SELECT COUNT(*) FROM nodes').fetchone()
        result['total_nodes'] = row[0] if row else 0

        # Active nodes within range
        if range_param == 'all':
            result['active_nodes'] = result['total_nodes']
        else:
            hours_map = {'1h': 1, '6h': 6, '24h': 24, '7d': 168}
            cutoff = int(time.time()) - (hours_map.get(range_param, 24) * 3600)
            row = conn.execute('SELECT COUNT(*) FROM nodes WHERE last_heard > ?', (cutoff,)).fetchone()
            result['active_nodes'] = row[0] if row else 0

        # Neighbor count
        try:
            row = conn.execute('SELECT COUNT(*) FROM neighbors').fetchone()
            result['neighbor_records'] = row[0] if row else 0
        except:
            result['neighbor_records'] = 0

        # New table counts
        for table, key in [('waypoints', 'waypoint_records'), ('traceroutes', 'traceroute_records'),
                           ('store_forward', 'store_forward_records'), ('range_tests', 'range_test_records'),
                           ('detection_sensor', 'detection_alerts'), ('paxcounter', 'paxcounter_records')]:
            try:
                row = conn.execute(f'SELECT COUNT(*) FROM {table}').fetchone()
                result[key] = row[0] if row else 0
            except:
                result[key] = 0

        # DB size
        from pathlib import Path
        db_size = Path(db.db_path).stat().st_size if Path(db.db_path).exists() else 0
        result['database_size_mb'] = round(db_size / (1024 * 1024), 2)

        # Packet types breakdown (top 6, filtered)
        if time_filter:
            cursor = conn.execute('''
                SELECT packet_type, COUNT(*) as count
                FROM raw_packets WHERE timestamp > datetime('now', 'localtime', ?)
                GROUP BY packet_type ORDER BY count DESC LIMIT 6
            ''', (time_filter,))
        else:
            cursor = conn.execute('''
                SELECT packet_type, COUNT(*) as count
                FROM raw_packets GROUP BY packet_type ORDER BY count DESC LIMIT 6
            ''')
        result['packet_types'] = {row[0]: row[1] for row in cursor.fetchall()}

        # Hop distribution - count ALL nodes by hops_away (0-7) from nodes table
        hop_sql = '''
            SELECT
                SUM(CASE WHEN hops_away IS NULL OR hops_away = 0 THEN 1 ELSE 0 END),
                SUM(CASE WHEN hops_away = 1 THEN 1 ELSE 0 END),
                SUM(CASE WHEN hops_away = 2 THEN 1 ELSE 0 END),
                SUM(CASE WHEN hops_away = 3 THEN 1 ELSE 0 END),
                SUM(CASE WHEN hops_away = 4 THEN 1 ELSE 0 END),
                SUM(CASE WHEN hops_away = 5 THEN 1 ELSE 0 END),
                SUM(CASE WHEN hops_away = 6 THEN 1 ELSE 0 END),
                SUM(CASE WHEN hops_away >= 7 THEN 1 ELSE 0 END),
                COUNT(*)
            FROM nodes
        '''
        if range_param == 'all':
            cursor = conn.execute(hop_sql)
        else:
            hours_map_hop = {'1h': 1, '6h': 6, '24h': 24, '7d': 168}
            hop_cutoff = int(time.time()) - (hours_map_hop.get(range_param, 24) * 3600)
            cursor = conn.execute(hop_sql + ' WHERE last_heard > ?', (hop_cutoff,))
        hop_row = cursor.fetchone()
        result['hop_distribution'] = {
            'direct': hop_row[0] or 0,
            'hop1': hop_row[1] or 0,
            'hop2': hop_row[2] or 0,
            'hop3': hop_row[3] or 0,
            'hop4': hop_row[4] or 0,
            'hop5': hop_row[5] or 0,
            'hop6': hop_row[6] or 0,
            'hop7': hop_row[7] or 0,
            'total': hop_row[8] or 0
        }

        # Average SNR
        if time_filter:
            row = conn.execute('SELECT AVG(snr) FROM messages WHERE snr IS NOT NULL AND timestamp > datetime("now", "localtime", ?)', (time_filter,)).fetchone()
        else:
            row = conn.execute('SELECT AVG(snr) FROM messages WHERE snr IS NOT NULL').fetchone()
        result['avg_snr'] = round(row[0], 1) if row and row[0] else None

        # Channel utilization & Air TX (latest from nodes)
        row = conn.execute('''
            SELECT AVG(channel_utilization), AVG(air_util_tx)
            FROM nodes WHERE channel_utilization IS NOT NULL AND channel_utilization > 0
        ''').fetchone()
        result['avg_channel_util'] = round(row[0], 1) if row and row[0] else None
        result['avg_air_tx'] = round(row[1], 1) if row and row[1] else None

        # Top 5 most active nodes in range
        if time_filter:
            cursor = conn.execute('''
                SELECT m.from_id, m.from_name, COUNT(*) as msg_count,
                       n.battery_level, n.hops_away
                FROM messages m
                LEFT JOIN nodes n ON m.from_id = n.node_id
                WHERE m.timestamp > datetime('now', 'localtime', ?)
                GROUP BY m.from_id
                ORDER BY msg_count DESC LIMIT 5
            ''', (time_filter,))
        else:
            cursor = conn.execute('''
                SELECT m.from_id, m.from_name, COUNT(*) as msg_count,
                       n.battery_level, n.hops_away
                FROM messages m
                LEFT JOIN nodes n ON m.from_id = n.node_id
                GROUP BY m.from_id
                ORDER BY msg_count DESC LIMIT 5
            ''')
        result['top_nodes'] = [{
            'node_id': row[0],
            'name': row[1] or row[0],
            'msg_count': row[2],
            'battery': row[3],
            'hops': row[4]
        } for row in cursor.fetchall()]

        # Activity chart with adaptive buckets (pre-filled to avoid sparse gaps)
        now = datetime.now()
        if range_param == '1h':
            cursor = conn.execute('''
                SELECT strftime('%H', timestamp) || ':' ||
                       printf('%02d', (CAST(strftime('%M', timestamp) AS INTEGER) / 5) * 5) as bucket,
                       COUNT(*) as count
                FROM messages WHERE timestamp > datetime('now', 'localtime', '-1 hours')
                GROUP BY bucket ORDER BY bucket
            ''')
            # Pre-fill all 12 five-minute buckets for the last hour
            buckets = {}
            t = now - timedelta(hours=1)
            t = t.replace(minute=(t.minute // 5) * 5, second=0, microsecond=0)
            while t <= now:
                label = f'{t.hour:02d}:{(t.minute // 5) * 5:02d}'
                buckets[label] = 0
                t += timedelta(minutes=5)
            for row in cursor.fetchall():
                if row[0] in buckets:
                    buckets[row[0]] = row[1]
            result['activity'] = [{'label': k, 'count': v} for k, v in sorted(buckets.items())]
            result['activity_bucket'] = '5min'
        elif range_param == '6h':
            cursor = conn.execute('''
                SELECT strftime('%H', timestamp) || ':' ||
                       CASE WHEN CAST(strftime('%M', timestamp) AS INTEGER) < 30 THEN '00' ELSE '30' END as bucket,
                       COUNT(*) as count
                FROM messages WHERE timestamp > datetime('now', 'localtime', '-6 hours')
                GROUP BY bucket ORDER BY bucket
            ''')
            # Pre-fill all 30-minute buckets for the last 6 hours
            buckets = {}
            t = now - timedelta(hours=6)
            t = t.replace(minute=0 if t.minute < 30 else 30, second=0, microsecond=0)
            while t <= now:
                label = f'{t.hour:02d}:{"00" if t.minute < 30 else "30"}'
                buckets[label] = 0
                t += timedelta(minutes=30)
            for row in cursor.fetchall():
                if row[0] in buckets:
                    buckets[row[0]] = row[1]
            result['activity'] = [{'label': k, 'count': v} for k, v in sorted(buckets.items())]
            result['activity_bucket'] = '30min'
        elif range_param == '7d':
            cursor = conn.execute('''
                SELECT strftime('%m/%d', timestamp) as bucket, COUNT(*) as count
                FROM messages WHERE timestamp > datetime('now', 'localtime', '-7 days')
                GROUP BY bucket ORDER BY bucket
            ''')
            # Pre-fill all 7 daily buckets
            buckets = {}
            for i in range(7, -1, -1):
                d = now - timedelta(days=i)
                label = d.strftime('%m/%d')
                buckets[label] = 0
            for row in cursor.fetchall():
                if row[0] in buckets:
                    buckets[row[0]] = row[1]
            result['activity'] = [{'label': k, 'count': v} for k, v in sorted(buckets.items())]
            result['activity_bucket'] = 'day'
        elif range_param == 'all':
            # All time: daily buckets
            cursor = conn.execute('''
                SELECT strftime('%m/%d', timestamp) as bucket, COUNT(*) as count
                FROM messages
                GROUP BY bucket ORDER BY bucket
            ''')
            result['activity'] = [{'label': row[0], 'count': row[1]} for row in cursor.fetchall()]
            result['activity_bucket'] = 'day'
        else:
            # 24h: hourly, pre-filled
            cursor = conn.execute('''
                SELECT strftime('%H', timestamp) as hour, COUNT(*) as count
                FROM messages WHERE timestamp > datetime('now', 'localtime', '-24 hours')
                GROUP BY hour ORDER BY hour
            ''')
            hour_counts = {f'{i:02d}': 0 for i in range(24)}
            for row in cursor.fetchall():
                hour_counts[row[0]] = row[1]
            result['activity'] = [{'label': h, 'count': c} for h, c in sorted(hour_counts.items())]
            result['activity_bucket'] = 'hour'

        return jsonify(result)

    except Exception as e:
        import traceback
        print(f"Error getting enhanced stats: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/historical')
def api_historical():
    """Get all data at a specific point in time."""
    global db
    if not db:
        db = MeshDatabase()

    try:
        timestamp = request.args.get('at')
        if not timestamp:
            return jsonify({'error': 'Missing "at" parameter'}), 400

        # Get historical data
        messages = db.get_messages_before(timestamp, limit=50)
        nodes = db.get_nodes_at_time(timestamp)
        stats = db.get_stats_at_time(timestamp)

        # Get activity chart data (hourly counts for 24h before timestamp)
        activity = []
        try:
            conn = db._get_conn()
            cursor = conn.execute('''
                SELECT strftime('%H', timestamp) as hour, COUNT(*) as count
                FROM messages
                WHERE timestamp > datetime(?, '-24 hours') AND timestamp <= ?
                GROUP BY hour
                ORDER BY hour
            ''', (timestamp, timestamp))

            hour_counts = {f'{i:02d}': 0 for i in range(24)}
            for row in cursor.fetchall():
                hour_counts[row[0]] = row[1]
            activity = [{'hour': h, 'count': c} for h, c in sorted(hour_counts.items())]
        except Exception as e:
            print(f"Error getting historical activity: {e}")

        return jsonify({
            'timestamp': timestamp,
            'messages': messages,
            'nodes': nodes,
            'stats': stats,
            'activity': activity
        })

    except Exception as e:
        print(f"Error getting historical data: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/telemetry-history')
def api_telemetry_history():
    """Get telemetry time series for a node."""
    global db
    if not db:
        db = MeshDatabase()

    node_id = request.args.get('node_id')
    tel_type = request.args.get('type', 'device')
    limit = request.args.get('limit', 100, type=int)

    try:
        conn = db._get_conn()
        if node_id:
            cursor = conn.execute('''
                SELECT timestamp, telemetry_type, battery_level, voltage,
                       channel_utilization, air_util_tx, uptime_seconds,
                       temperature, relative_humidity, barometric_pressure
                FROM telemetry
                WHERE node_id = ? AND (? = 'all' OR telemetry_type = ?)
                ORDER BY timestamp DESC LIMIT ?
            ''', (node_id, tel_type, tel_type, limit))
        else:
            cursor = conn.execute('''
                SELECT node_id, timestamp, telemetry_type, battery_level, voltage,
                       channel_utilization, air_util_tx, temperature, relative_humidity
                FROM telemetry
                WHERE ? = 'all' OR telemetry_type = ?
                ORDER BY timestamp DESC LIMIT ?
            ''', (tel_type, tel_type, limit))

        results = [dict(row) for row in cursor.fetchall()]
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/position-trail')
def api_position_trail():
    """Get position history for map trails."""
    global db
    if not db:
        db = MeshDatabase()

    node_id = request.args.get('node_id')
    limit = request.args.get('limit', 100, type=int)

    try:
        conn = db._get_conn()
        if node_id:
            cursor = conn.execute('''
                SELECT node_id, timestamp, latitude, longitude, altitude, speed
                FROM positions
                WHERE node_id = ? AND latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY timestamp DESC LIMIT ?
            ''', (node_id, limit))
        else:
            cursor = conn.execute('''
                SELECT node_id, timestamp, latitude, longitude, altitude, speed
                FROM positions
                WHERE latitude IS NOT NULL AND longitude IS NOT NULL
                ORDER BY timestamp DESC LIMIT ?
            ''', (limit,))

        results = [dict(row) for row in cursor.fetchall()]
        return jsonify(results)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/topology')
def api_topology():
    """Get network topology graph."""
    global db
    if not db:
        db = MeshDatabase()

    try:
        topology = db.get_network_topology()
        return jsonify(topology)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/waypoints')
def api_waypoints():
    """Get active waypoints."""
    global db
    if not db:
        db = MeshDatabase()

    active_only = request.args.get('active_only', 'true').lower() == 'true'

    try:
        waypoints = db.get_waypoints(active_only=active_only)
        return jsonify(waypoints)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/traceroutes')
def api_traceroutes():
    """Get recent traceroutes, optionally filtered by node_id."""
    global db
    if not db:
        db = MeshDatabase()

    limit = request.args.get('limit', 20, type=int)
    node_id = request.args.get('node_id')

    try:
        traceroutes = db.get_traceroutes(limit=limit)
        if node_id:
            filtered = []
            for tr in traceroutes:
                match = (tr.get('from_id') == node_id or
                         tr.get('to_id') == node_id)
                if not match and tr.get('route'):
                    route = tr['route']
                    if isinstance(route, list):
                        match = node_id in route
                    elif isinstance(route, str):
                        match = node_id in route
                if match:
                    filtered.append(tr)
            traceroutes = filtered[:limit]
        return jsonify(traceroutes)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/request-traceroute', methods=['POST'])
def api_request_traceroute():
    """Queue a traceroute request to be sent by the bridge."""
    global db
    if not db:
        db = MeshDatabase()

    data = request.get_json()
    node_id = data.get('node_id') if data else None
    if not node_id:
        return jsonify({'error': 'node_id required'}), 400

    try:
        msg_id = db.add_traceroute_request(node_id)
        return jsonify({'success': True, 'request_id': msg_id})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/signal-trends')
def api_signal_trends():
    """Get SNR/RSSI trends for a node."""
    global db
    if not db:
        db = MeshDatabase()

    node_id = request.args.get('node_id')
    hours = request.args.get('hours', 24, type=int)

    if not node_id:
        return jsonify({'error': 'node_id required'}), 400

    try:
        trends = db.get_signal_trends(node_id, hours=hours)
        return jsonify(trends)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/paxcounter')
def api_paxcounter():
    """Get paxcounter history."""
    global db
    if not db:
        db = MeshDatabase()

    node_id = request.args.get('node_id')
    limit = request.args.get('limit', 100, type=int)

    try:
        data = db.get_paxcounter_history(node_id=node_id, limit=limit)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/range-tests')
def api_range_tests():
    """Get range test results."""
    global db
    if not db:
        db = MeshDatabase()

    limit = request.args.get('limit', 50, type=int)

    try:
        data = db.get_range_tests(limit=limit)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/detection-alerts')
def api_detection_alerts():
    """Get detection sensor alerts."""
    global db
    if not db:
        db = MeshDatabase()

    limit = request.args.get('limit', 50, type=int)

    try:
        data = db.get_detection_alerts(limit=limit)
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/store-forward-stats')
def api_store_forward_stats():
    """Get latest store & forward stats per node."""
    global db
    if not db:
        db = MeshDatabase()

    try:
        data = db.get_store_forward_stats()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/node-detail')
def api_node_detail():
    """Get comprehensive single-node view."""
    global db
    if not db:
        db = MeshDatabase()

    node_id = request.args.get('node_id')
    if not node_id:
        return jsonify({'error': 'node_id required'}), 400

    try:
        node = db.get_node(node_id)
        if not node:
            return jsonify({'error': 'Node not found'}), 404

        telemetry = db.get_telemetry_history(node_id, limit=20)
        positions = db.get_position_history(node_id, limit=20)
        signal = db.get_signal_trends(node_id, hours=24)
        facts = db.get_user_facts(node_id)

        return jsonify({
            'node': node,
            'telemetry': telemetry,
            'positions': positions,
            'signal_trends': signal,
            'facts': facts
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def run_dashboard(host='0.0.0.0', port=5000, debug=False):
    """Run the dashboard server."""
    print(f"\n{'='*60}")
    print("  🦙 LoRaLlama Dashboard")
    print(f"{'='*60}")
    print(f"\n  🌐 Local:   http://localhost:{port}")
    print(f"  🌐 Network: http://<your-ip>:{port}")
    print("\n  Press Ctrl+C to stop\n")

    socketio.run(app, host=host, port=port, debug=debug, allow_unsafe_werkzeug=True)


if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='LoRaLlama Dashboard')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=5000, help='Port to listen on')
    parser.add_argument('--debug', action='store_true', help='Enable debug mode')

    args = parser.parse_args()
    run_dashboard(host=args.host, port=args.port, debug=args.debug)
