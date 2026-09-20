#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PolarDB-X Cache Peer Monitor
=============================
A lightweight monitoring dashboard for cache_peer status.
Connects to PolarDB-X (MySQL-compatible) and periodically collects
cache peer metrics, serving a real-time monitoring UI.
"""

import json
import time
import threading
import configparser
import os
import sys
from datetime import datetime, timedelta
from collections import deque

from flask import Flask, jsonify, send_from_directory, request
from flask_cors import CORS
import pymysql

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "config.ini")

def load_config():
    """Load configuration from config.ini."""
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE, encoding="utf-8")
    return {
        "db_host": cfg.get("database", "host", fallback="127.0.0.1"),
        "db_port": cfg.getint("database", "port", fallback=3306),
        "db_user": cfg.get("database", "user", fallback="polardbx_root"),
        "db_password": cfg.get("database", "password", fallback=""),
        "collect_interval": cfg.getint("monitor", "collect_interval", fallback=10),
        "history_capacity": cfg.getint("monitor", "history_capacity", fallback=360),
        "server_port": cfg.getint("monitor", "server_port", fallback=8199),
    }

CONFIG = load_config()

# ---------------------------------------------------------------------------
# In-memory history store (ring buffer per peer)
# ---------------------------------------------------------------------------
# Structure: { peer_name: deque([ { "ts": ..., "status_json": {...}, "raw": {...} } ]) }
history_store = {}
history_lock = threading.Lock()
MAX_HISTORY = CONFIG["history_capacity"]

# Latest snapshot for quick access
latest_snapshot = {"peers": [], "collected_at": None}
snapshot_lock = threading.Lock()

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------
def get_connection():
    """Create a new MySQL connection."""
    return pymysql.connect(
        host=CONFIG["db_host"],
        port=CONFIG["db_port"],
        user=CONFIG["db_user"],
        password=CONFIG["db_password"],
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=5,
        read_timeout=10,
    )


def fetch_peers():
    """Fetch current cache_peer rows."""
    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM metadb.cache_peer ORDER BY id")
            rows = cursor.fetchall()
        return rows
    finally:
        if conn:
            conn.close()


def serialize_row(row):
    """Convert a DB row to JSON-safe dict."""
    result = {}
    for k, v in row.items():
        if isinstance(v, datetime):
            result[k] = v.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(v, bytes):
            result[k] = v.decode("utf-8", errors="replace")
        else:
            result[k] = v
    return result

# ---------------------------------------------------------------------------
# Collector thread
# ---------------------------------------------------------------------------
def collect_loop():
    """Periodically collect cache_peer data."""
    interval = CONFIG["collect_interval"]
    print(f"[Collector] Starting collection every {interval}s, "
          f"history capacity={MAX_HISTORY} per peer")

    while True:
        try:
            rows = fetch_peers()
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            peers = []

            for row in rows:
                peer = serialize_row(row)
                peer_name = peer.get("peer_name", "unknown")

                # Parse status_json if present
                status = None
                raw_json = peer.get("status_json")
                if raw_json and isinstance(raw_json, str) and raw_json.strip():
                    try:
                        status = json.loads(raw_json)
                    except json.JSONDecodeError:
                        status = None

                peers.append(peer)

                # Append to history
                snapshot_entry = {
                    "ts": now,
                    "status": status,
                    "host": peer.get("host"),
                    "role": peer.get("role"),
                    "leader": peer.get("leader"),
                }

                with history_lock:
                    if peer_name not in history_store:
                        history_store[peer_name] = deque(maxlen=MAX_HISTORY)
                    history_store[peer_name].append(snapshot_entry)

            with snapshot_lock:
                latest_snapshot["peers"] = peers
                latest_snapshot["collected_at"] = now

        except Exception as e:
            print(f"[Collector] Error: {e}")

        time.sleep(interval)

# ---------------------------------------------------------------------------
# Flask application
# ---------------------------------------------------------------------------
app = Flask(__name__, static_folder="static")
CORS(app)


@app.route("/")
def index():
    """Serve the dashboard HTML."""
    return send_from_directory(app.static_folder, "index.html")


@app.route("/static/<path:filename>")
def static_files(filename):
    """Serve static files."""
    return send_from_directory(app.static_folder, filename)


@app.route("/api/peers")
def api_peers():
    """Return the latest peer snapshot."""
    with snapshot_lock:
        return jsonify(latest_snapshot)


@app.route("/api/history")
def api_history():
    """
    Return historical data for charts.
    Query params:
      - peer: peer_name (optional, returns all if omitted)
      - minutes: how many minutes of history (default 60)
    """
    peer_filter = request.args.get("peer")
    minutes = int(request.args.get("minutes", 60))
    cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")

    result = {}
    with history_lock:
        targets = {peer_filter: history_store.get(peer_filter, deque())} \
            if peer_filter else dict(history_store)

        for name, dq in targets.items():
            entries = [e for e in dq if e["ts"] >= cutoff]
            result[name] = entries

    return jsonify({"history": result, "minutes": minutes, "collect_interval": CONFIG["collect_interval"]})


@app.route("/api/history/metrics")
def api_history_metrics():
    """
    Return time-series for specific metrics across all peers.
    Query params:
      - section: JSON section name (arena/bp/localCache/rpc/cnCache/config)
      - keys: comma-separated metric keys
      - minutes: history window (default 60)
    """
    section = request.args.get("section", "")
    keys = [k.strip() for k in request.args.get("keys", "").split(",") if k.strip()]
    minutes = int(request.args.get("minutes", 60))
    cutoff = (datetime.now() - timedelta(minutes=minutes)).strftime("%Y-%m-%d %H:%M:%S")

    result = {}
    with history_lock:
        for peer_name, dq in history_store.items():
            series = {"ts": [], **{k: [] for k in keys}}
            for entry in dq:
                if entry["ts"] < cutoff:
                    continue
                series["ts"].append(entry["ts"])
                status = entry.get("status") or {}
                sec_data = status.get(section, {})
                for k in keys:
                    series[k].append(sec_data.get(k))
            result[peer_name] = series

    return jsonify({"metrics": result, "section": section, "keys": keys})


@app.route("/api/config")
def api_config():
    """Return current monitor configuration."""
    return jsonify({
        "collect_interval": CONFIG["collect_interval"],
        "history_capacity": CONFIG["history_capacity"],
        "db_host": CONFIG["db_host"],
        "db_port": CONFIG["db_port"],
    })


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    port = CONFIG["server_port"]
    print("=" * 60)
    print("  PolarDB-X Cache Peer Monitor")
    print(f"  DB: {CONFIG['db_host']}:{CONFIG['db_port']}")
    print(f"  Collect interval: {CONFIG['collect_interval']}s")
    print(f"  Dashboard: http://localhost:{port}")
    print("=" * 60)

    # Start collector in background
    collector = threading.Thread(target=collect_loop, daemon=True)
    collector.start()

    # Run Flask
    app.run(host="0.0.0.0", port=port, debug=False, threaded=True)


if __name__ == "__main__":
    main()
