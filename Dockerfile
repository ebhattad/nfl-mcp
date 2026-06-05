FROM python:3.12-slim

ARG SUPERCRONIC_VERSION=0.2.33

RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/* && \
    curl -fsSL "https://github.com/aptible/supercronic/releases/download/v${SUPERCRONIC_VERSION}/supercronic-linux-amd64" \
    -o /usr/local/bin/supercronic && \
    chmod +x /usr/local/bin/supercronic

RUN pip install uv && uv pip install --system nfl-mcp

RUN mkdir -p /data /tmp /var/log/nflmcp && \
    chmod 777 /var/log/nflmcp

COPY entrypoint.sh /entrypoint.sh
COPY update_db.sh /update_db.sh
RUN chmod +x /entrypoint.sh /update_db.sh

ENV NFL_MCP_DB_PATH=/data/nflread.duckdb

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
