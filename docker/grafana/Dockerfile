FROM alpine:3.22 AS plugin-downloader

ARG CLICKHOUSE_PLUGIN_VERSION=4.20.0

RUN apk add --no-cache \
    ca-certificates \
    curl \
    python3 \
    unzip

RUN set -eux; \
    curl -fsSL \
      "https://api.github.com/repos/grafana/clickhouse-datasource/releases/tags/v${CLICKHOUSE_PLUGIN_VERSION}" \
      -o /tmp/release.json; \
    ASSET_URL="$(python3 -c 'import json; data=json.load(open("/tmp/release.json")); print(next(asset["browser_download_url"] for asset in data["assets"] if "linux_amd64" in asset["name"].lower().replace("-", "_") and asset["name"].lower().endswith(".zip")))')"; \
    echo "Downloading plugin from ${ASSET_URL}"; \
    curl -fsSL "${ASSET_URL}" -o /tmp/plugin.zip; \
    mkdir -p /plugin; \
    unzip -q /tmp/plugin.zip -d /plugin; \
    test -f /plugin/grafana-clickhouse-datasource/plugin.json


FROM grafana/grafana:13.2.1

USER root

COPY --from=plugin-downloader \
    --chown=472:0 \
    /plugin/grafana-clickhouse-datasource \
    /var/lib/grafana/plugins/grafana-clickhouse-datasource

USER 472
