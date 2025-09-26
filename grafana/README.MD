install loki

docker run -d --name=loki -p 3100:3100 grafana/loki:latest


run prometheus

docker run -d --name=prometheus -p 9090:9090 -v "C:\prometheus\prometheus.yml:/etc/prometheus/prometheus.yml" prom/prometheus:latest --web.enable-remote-write-receiver --config.file=/etc/prometheus/prometheus.yml


Run PostgreSQL Exporter

docker run -d --name=postgres-exporter -p 9187:9187 -e DATA_SOURCE_NAME="postgresql://postgres:aqwzsxedc@host.docker.internal:5432/postgres?sslmode=disable" quay.io/prometheuscommunity/postgres-exporter --collector.stat_user_tables

docker run -d --name=postgres-exporter -p 9187:9187 -v "C:\Program Files\GrafanaLabs\Alloy\queries.yaml:/etc/queries.yaml" -e DATA_SOURCE_NAME="postgresql://postgres:aqwzsxedc@host.docker.internal:5432/strava_poc?sslmode=disable" quay.io/prometheuscommunity/postgres-exporter --extend.query-path=/etc/queries.yaml


docker run -d --name=grafana -p 3000:3000 grafana/grafana-oss   

sc stop "Alloy" 
sc start "Alloy" 
sc query "Alloy" | find "STATE"


"C:\Program Files\GrafanaLabs\Alloy\alloy-windows-amd64.exe" run postgre.alloy




request prometeus :
curl "http://localhost:9090/api/v1/query?query=up{job=\"integrations/postgres_exporter\"}"