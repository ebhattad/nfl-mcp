#!/bin/sh
set -euo pipefail

DB_DIR="/data"
SYMLINK="$DB_DIR/nflread.duckdb"

# ── First-run: serve immediately, ingest in the background ────────────────────
# A blocking full ingest can exceed the Container Apps startup-probe window,
# which kills and restarts the replica before the server ever binds its port —
# producing an endless re-ingest loop on ephemeral storage. Instead, create an
# empty active DB so the server can start right away (the startup probe passes),
# then run the full ingest into the inactive slot in the background and swap the
# symlink atomically when it completes. Data becomes queryable once ingest ends.
if [ ! -L "$SYMLINK" ]; then
    echo "[init] First run detected — creating empty active DB so the server can start immediately..."
    python -c "import duckdb; duckdb.connect('$DB_DIR/nflread_a.duckdb').close()"
    ln -s nflread_a.duckdb "$SYMLINK"
    echo "[init] Starting full initial ingest in the background (this takes a while; data becomes queryable when it finishes)..."
    /update_db.sh >> /var/log/nflmcp/nfl_update.log 2>&1 &
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
