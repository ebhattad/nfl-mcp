#!/bin/sh
set -euo pipefail

DB_DIR="/data"
SYMLINK="$DB_DIR/nflread.duckdb"

# ── Resolve current active slot ───────────────────────────────────────────────
if [ ! -L "$SYMLINK" ]; then
    echo "[$(date -u)] ERROR: $SYMLINK is not a symlink. Has entrypoint.sh run first-run init?"
    exit 1
fi

CURRENT=$(readlink "$SYMLINK")

if [ "$CURRENT" = "nflread_a.duckdb" ]; then
    TARGET="$DB_DIR/nflread_b.duckdb"
else
    TARGET="$DB_DIR/nflread_a.duckdb"
fi

echo "[$(date -u)] Active DB : $CURRENT"
echo "[$(date -u)] Target DB : $TARGET"

# ── Copy active DB to target slot ─────────────────────────────────────────────
# This brings along _ingest_metadata so ingest skips already-loaded data
echo "[$(date -u)] Copying active DB to target slot..."
cp "$DB_DIR/$CURRENT" "$TARGET"

# ── Run incremental ingest into target slot ───────────────────────────────────
echo "[$(date -u)] Running incremental ingest..."
NFL_MCP_DB_PATH="$TARGET" nfl-mcp ingest

# ── Atomic symlink swap ───────────────────────────────────────────────────────
echo "[$(date -u)] Swapping symlink to $(basename "$TARGET")..."
ln -sfn "$(basename "$TARGET")" "$SYMLINK"

echo "[$(date -u)] Update complete. Active DB is now $(basename "$TARGET")."
