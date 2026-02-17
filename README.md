# Kafka Consumer Lag Demo: Traditional vs Burrow vs KLag

## Quick Start

1. Start everything:

```bash
docker compose up -d --build
```

2. Open Grafana dashboards:
- Traditional only: [http://localhost:3000/d/traditional-kafka-metrics/traditional-kafka-metrics](http://localhost:3000/d/traditional-kafka-metrics/traditional-kafka-metrics)
- Burrow only: [http://localhost:3000/d/burrow-evaluated-lag/burrow-evaluated-lag](http://localhost:3000/d/burrow-evaluated-lag/burrow-evaluated-lag)
- KLag only: [http://localhost:3000/d/klag-dlp-monitor/klag-data-loss-prevention](http://localhost:3000/d/klag-dlp-monitor/klag-data-loss-prevention)

3. Optional endpoints:
- Grafana home: [http://localhost:3000](http://localhost:3000)
- Burrow API: [http://localhost:8000/v3/kafka](http://localhost:8000/v3/kafka)
- KLag metrics: [http://localhost:8888/metrics](http://localhost:8888/metrics)
- Elasticsearch: [http://localhost:9200](http://localhost:9200)

Grafana credentials: `admin` / `admin` (anonymous editor mode is also enabled).

## What This Runs

- Kafka in KRaft mode (no ZooKeeper)
- Burrow
- KLag
- Kafka exporter (traditional lag metrics)
- Elasticsearch
- Grafana
- Elasticsearch metrics indexer (scrapes Kafka exporter, Burrow, and KLag)
- Traffic simulator

## Scenarios

The simulator continuously creates five topic/group patterns:

- `demo.healthy` + `demo-healthy-group`: high-throughput micro-batching consumer; lag spikes briefly, then catches up.
- `demo.slow` + `demo-slow-group`: always consuming, but underpowered, so lag trends up.
- `demo.stopped` + `demo-stopped-group`: consumes briefly then stops while producer continues.
- `demo.bursty` + `demo-bursty-group`: alternates between fast and slow phases.
- `demo.risk` + `demo-risk-group`: short-retention demo (`retention.ms=120000`) with pause/resume consumer cycles to trigger KLag retention-risk signals.

Topic layout:

- `demo.healthy`: 3 partitions
- `demo.slow`: 1 partition
- `demo.stopped`: 1 partition
- `demo.bursty`: 1 partition
- `demo.risk`: 1 partition (`retention.ms=120000`)

## Dashboard Focus

- **Traditional - Kafka Metrics**: raw lag, consume rates, produced vs consumed offsets, plus threshold-based status lights.
- **Burrow - Evaluated Lag**: Burrow status (`OK/WARN/ERR/STOP/...`), bad partitions, max lag, and completeness.
- **KLag - Data Loss Prevention**: lag retention percent, lag age (ms), group state, and derived retention risk code (`GREEN/YELLOW/RED`).

## Stop

```bash
docker compose down -v
```
