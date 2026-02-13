# Burrow vs Traditional Consumer Lag Demo

## Quick Start

1. Start the stack:

```bash
docker compose up -d --build
```

2. Open the dashboard directly:
- [http://localhost:3000/d/burrow-vs-traditional/](http://localhost:3000/d/burrow-vs-traditional/)

3. Optional endpoints:
- Grafana home: [http://localhost:3000](http://localhost:3000)
- Burrow API: [http://localhost:8000/v3/kafka](http://localhost:8000/v3/kafka)
- Elasticsearch: [http://localhost:9200](http://localhost:9200)

This stack demonstrates why Burrow gives better lag signal than raw lag charts, using Elasticsearch as the metrics backend.

It spins up:
- Kafka (KRaft mode, no Zookeeper)
- Burrow
- Elasticsearch + Grafana
- Kafka exporter (traditional lag metrics)
- Elasticsearch metrics indexer (scrapes Kafka exporter + Burrow API and writes normalized docs)
- Traffic simulator with four topic patterns

## Scenarios

The simulator continuously creates four topic/group behaviors:
- `demo.healthy` + `demo-healthy-group`: high-throughput micro-batching consumer with transient lag spikes that repeatedly returns to zero (keeps up over time).
- `demo.slow` + `demo-slow-group`: consumer continues committing, but at an intentionally underpowered rate.
- `demo.stopped` + `demo-stopped-group`: consumer commits for ~90 seconds and then stops while production continues.
- `demo.bursty` + `demo-bursty-group`: consumer alternates between fast and slow phases, so lag periodically grows and then catches up.

Topic partitioning is also intentional for visibility:
- `demo.healthy`: 3 partitions
- `demo.slow`: 1 partition
- `demo.stopped`: 1 partition
- `demo.bursty`: 1 partition

This gives three problematic visual cases (`slow`, `stopped`, `bursty`) and one healthy baseline that still shows real traffic.

## Start

```bash
docker compose up --build
```

## Open

- Grafana: [http://localhost:3000](http://localhost:3000)
- Elasticsearch: [http://localhost:9200](http://localhost:9200)
- Burrow API: [http://localhost:8000/v3/kafka](http://localhost:8000/v3/kafka)

Grafana credentials: `admin` / `admin` (anonymous view is also enabled).

## Dashboard

Grafana auto-loads the dashboard:
- **Traditional panels** (from Elasticsearch indexed docs):
  - `traditional_group_lag_sum`
  - `traditional_group_total_lag`
- **Traditional activity panels**:
  - `traditional_group_consume_rate` for consume throughput
  - `traditional_group_total_consumed_offset` for total consumed offsets
  - Produced vs consumed offsets for `demo.healthy` to prove active flow while lag stays near zero
- **Burrow panels**:
  - `burrow_group_status_code` (OK/WARN/ERR/STOP semantics)
  - `burrow_group_bad_partitions`
  - `burrow_group_max_lag`
  - `burrow_group_complete`

Status code mapping used in the dashboard:
- `0=OK`
- `1=WARN`
- `2=ERR`
- `3=STOP`
- `4=STALL`
- `5=NOTFOUND`
- `6=UNKNOWN`

## Why this demo is useful

Traditional lag charts always produce numbers. Burrow adds state-aware interpretation:
- `demo.slow` typically trends toward `WARN` while still making offset progress.
- `demo.stopped` transitions toward `STOP/ERR` once commits cease.
- `demo.bursty` oscillates between healthy and warning behavior as throughput phases change.
- `demo.healthy` remains `OK` with low bad-partition count, even when traditional lag spikes during batch windows.

## Stop and clean up

```bash
docker compose down -v
```
