#!/bin/sh
set -euo pipefail

DB_DIR="/data"
SYMLINK="$DB_DIR/nflread.duckdb"

# ── First-run: initial ingest into slot A and create symlink ──────────────────
if [ ! -L "$SYMLINK" ]; then
    echo "[init] First run detected — ingesting initial DB into nflread_a.duckdb..."
    NFL_MCP_DB_PATH="$DB_DIR/nflread_a.duckdb" nfl-mcp ingest
    ln -s nflread_a.duckdb "$SYMLINK"
    echo "[init] Done. Active DB: nflread_a.duckdb"
fi

# ── Write crontab ─────────────────────────────────────────────────────────────
cat > /tmp/crontab << 'EOF'
# Thursday 6AM UTC — corrected weekly data + all datasets
0 6 * * 4     /update_db.sh >> /var/log/nflmcp/nfl_update.log 2>&1

# Sun/Mon/Wed 3AM UTC — overnight game-day updates
0 3 * * 0,1,3 /update_db.sh >> /var/log/nflmcp/nfl_update.log 2>&1
EOF

# ── Start supercronic in background ──────────────────────────────────────────
supercronic /tmp/crontab &
echo "[cron] Scheduler started."

# ── Start MCP server as PID 1 ────────────────────────────────────────────────
echo "[server] Starting NFL MCP server..."
exec nfl-mcp serve --host 0.0.0.0 --port 8000
