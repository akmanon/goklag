# goklag

Production-grade Golang service for monitoring Apache Kafka consumer lag and exporting Prometheus metrics.

## Features

- Sarama Kafka client with SASL/GSSAPI (Kerberos) keytab auth
- Single shared Kafka client instance
- Bounded worker pools for metadata and broker offset batches
- Exponential backoff retries on metadata fetch failures
- Graceful handling of missing topics/partitions
- Prometheus gauge metrics:
  - `kafka_consumer_partition_lag{group,topic,partition}`
  - `kafka_consumer_topic_lag{group,topic}`
  - `kafka_consumer_group_total_lag{group}`
  - `kafka_partition_latest_offset{topic,partition}`
  - `kafka_partition_committed_offset{group,topic,partition}`
- HTTP endpoints:
  - `/metrics`
  - `/healthz`
- Graceful shutdown on SIGINT/SIGTERM

## Project Structure

```text
cmd/app/main.go
internal/config/config.go
internal/kafka/collector.go
internal/metrics/metrics.go
internal/server/server.go
config.yaml
prometheus-scrape.yml
```

## Build

```bash
go mod tidy
go build ./cmd/app
```

Using Makefile:

```bash
make test
make build-linux
```

Linux production binary output:

- `bin/goklag-linux-amd64`

## Run

```bash
go run ./cmd/app -config config.yaml
```

## Configuration

See `config.yaml` for the full sample.

- `server.lag_poll_interval_seconds` controls how often lag is collected.
- Default lag polling interval is `60` seconds (1 minute).

Required Kafka auth settings:

- `kafka.sasl.mechanism: GSSAPI`
- `kafka.sasl.kerberos.realm`
- `kafka.sasl.kerberos.service_name`
- `kafka.sasl.kerberos.username`
- `kafka.sasl.kerberos.keytab_path`

## Prometheus

Sample scrape config is available in `prometheus-scrape.yml`.

## Notes

- Lag is computed as `max(latest_offset - committed_offset, 0)`.
- If a committed offset is missing (`-1`), lag falls back to latest offset.
- Unknown/missing topics are logged and skipped without crashing the process.
