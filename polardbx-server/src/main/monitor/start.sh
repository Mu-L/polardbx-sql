#!/bin/bash
# ============================================================
# PolarDB-X Cache Peer Monitor - Startup Script
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# Check Python3
if ! command -v python3 &> /dev/null; then
    echo "[ERROR] python3 not found. Please install Python 3.8+."
    exit 1
fi

# Install dependencies if needed
if ! python3 -c "import flask, pymysql, flask_cors" 2>/dev/null; then
    echo "[INFO] Installing dependencies..."
    pip3 install -r requirements.txt
fi

echo "[INFO] Starting PolarDB-X Cache Peer Monitor..."
echo "[INFO] Edit config.ini to change database connection and monitor settings."
echo ""

python3 app.py "$@"
