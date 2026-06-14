FROM python:3.12-slim

# Install nfl-mcp from local source so the image always reflects the committed
# code (including the DuckDB memory bounds used during the baked ingest).
WORKDIR /src
COPY . /src
RUN pip install --no-cache-dir uv && uv pip install --system --no-cache .

ENV NFL_MCP_DB_PATH=/data/nflread.duckdb

# Bake the full DuckDB database into the image at build time. The container then
# serves read-only with NO runtime ingest — instant startup, no OOM, no crash
# loop, and no external storage. Refresh the data by rebuilding the image.
#
# Memory caps are passed inline (not as ENV) so the heavy build-time ingest stays
# within the CI runner's RAM (spilling to disk) without leaking the large limit
# into the lightweight runtime serve process configured further down.
#
# After ingest we validate the bake: no leftover WAL, and the core tables that
# the slow PBP path produces actually have rows — so a silently-incomplete
# ingest fails the build instead of shipping a broken image.
#
# INGEST_ARGS is empty by default, so production/release builds bake the full
# nflverse dataset (all seasons). CI passes a lightweight value (e.g. a single
# season) to validate the whole build pipeline quickly without the heavy bake.
ARG INGEST_ARGS=""
RUN mkdir -p /data \
 && NFL_MCP_DUCKDB_MEMORY_LIMIT=3GB \
    NFL_MCP_DUCKDB_THREADS=2 \
    POLARS_MAX_THREADS=2 \
    nfl-mcp ingest ${INGEST_ARGS} \
 && rm -rf /data/.duckdb_spill /root/.cache \
 && test ! -e /data/nflread.duckdb.wal \
 && python -c "import duckdb,sys; c=duckdb.connect('/data/nflread.duckdb',read_only=True); rows={t:c.execute('SELECT count(*) FROM '+t).fetchone()[0] for t in ('plays','schedules')}; print('baked rows:',rows); sys.exit(0 if all(v>0 for v in rows.values()) else 1)"

# Runtime resource caps for the small serve container (e.g. 1Gi). Heavy queries
# spill to /tmp instead of OOM-killing the process.
ENV NFL_MCP_DUCKDB_MEMORY_LIMIT=512MB \
    NFL_MCP_DUCKDB_THREADS=1 \
    NFL_MCP_DUCKDB_TEMP_DIR=/tmp/.duckdb_spill

EXPOSE 8000

ENTRYPOINT ["nfl-mcp", "serve", "--host", "0.0.0.0", "--port", "8000"]
