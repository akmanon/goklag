# DESIGN.md

## 1. Overview

`goklag` is a Kafka consumer lag monitoring daemon written in Go. It periodically:

1. Loads configured consumer groups and topics.
2. Resolves partitions for each topic.
3. Fetches latest (head) offsets per partition from broker leaders.
4. Fetches committed offsets per consumer group (Kafka mode) or from HDFS offset files (Spark mode).
5. Computes lag at partition/topic/group levels.
6. Publishes results as Prometheus gauges.
7. Serves `/metrics` and `/healthz` over HTTP.

The service uses a single shared Sarama client (connection reuse), bounded worker pools, context cancellation propagation, and graceful termination on SIGINT/SIGTERM.

## 2. Goals and Non-Goals

### 2.1 Goals

- Production-safe Kafka lag collection.
- Kerberos keytab authentication (SASL/GSSAPI).
- Efficient handling of many groups/partitions.
- Bounded concurrency with no goroutine leaks.
- Prometheus-compatible metrics with controlled label design.

### 2.2 Non-Goals

- Consumer message consumption.
- Offset commits or consumer group management.
- Automatic topic discovery outside configured topic lists.
- Persistent storage of historical lag snapshots.

## 3. High-Level Architecture

- `cmd/app/main.go`
  - Process bootstrap, config loading, lifecycle orchestration, polling loop.
- `internal/config`
  - Strongly typed config schema, defaults, validation.
- `internal/kafka`
  - Kafka client construction and lag collection logic.
- `internal/hdfsoffset`
  - HDFS CLI integration (`hdfs dfs -ls/-cat`) and committed offset parsing.
- `internal/metrics`
  - Prometheus gauge vectors and safe updates/cleanup.
- `internal/server`
  - HTTP endpoints and graceful server shutdown.

### 3.1 Main Data Flow

1. `main()` loads `config.yaml`.
2. `NewCollector()` builds Sarama client with GSSAPI keytab auth.
3. `metrics.NewStore()` registers gauge vectors.
4. `server.New()` starts `/metrics` and `/healthz` server.
5. Scheduler triggers periodic collection every `lag_poll_interval_seconds`.
6. `Collector.Collect()` (Kafka committed offsets) or `Collector.CollectHDFS()` (HDFS committed offsets) returns a `kafka.Snapshot`.
7. `Store.Update(snapshot)` updates/deletes metric series atomically.

## 4. Runtime Workflow (Low-Level)

### 4.1 Startup Sequence

1. Parse CLI flag `-config` (default `config.yaml`).
2. Initialize Zap production logger.
3. Load and validate config via Viper.
4. Create Kafka collector with parsed Kafka version and SASL settings.
5. Create Prometheus metrics store and HTTP server.
6. Start HTTP server in background goroutine.
7. Create root context tied to SIGINT/SIGTERM.
8. Execute one immediate collection before ticker loop.
9. Enter periodic collection loop (`lag_poll_interval_seconds`), selecting Kafka or HDFS committed-offset mode based on config.

### 4.2 Collection Sequence

For each collection tick:

1. Build per-cycle timeout context (`request_timeout_seconds`).
2. Resolve configured distinct topic set.
3. Fetch partitions per topic (parallel bounded workers + metadata retry/backoff).
4. Group topic-partitions by leader broker.
5. Fetch latest offsets in broker batches (`OffsetRequest`).
6. Resolve committed offsets:
   - Kafka mode: fetch committed offsets from group coordinator requests.
   - HDFS mode: list HDFS offset directories, select max numeric file, and parse committed offsets from JSON.
7. Compute lag per partition and aggregate topic/group totals.
8. Return `Snapshot`.
9. Push snapshot to Prometheus store.
10. Remove stale metric series no longer present in current snapshot.

### 4.3 Shutdown Sequence

1. Receive signal cancellation from root context.
2. Stop ticker.
3. Gracefully shutdown HTTP server (15s timeout).
4. Close shared Kafka client.
5. Exit process.

## 5. Configuration Model

`internal/config.Config`:

- `Kafka`
  - `Brokers []string`
  - `Version string`
  - `SASL`
    - `Mechanism` (must be `GSSAPI`)
    - `Kerberos`
      - `Realm`
      - `ServiceName`
      - `Username`
      - `KeytabPath`
  - `Consumers []ConsumerBinding`
    - `Group`
    - `Topics []string`
  - `HDFSOffset []HDFSOffsetBinding` (optional)
    - `Topic`
    - `Path` (HDFS directory containing numeric offset files)
- `Server`
  - `Port`
  - `ScrapeIntervalSeconds`
  - `LagPollIntervalSeconds` (default 60)
  - `WorkerPoolSize`
  - `RequestTimeoutSeconds`

### 5.1 Validation Strategy

`Config.Validate()` enforces required fields and positive intervals/sizes. `kafka.hdfs_offset` is optional, but if provided each binding requires non-empty `topic` and `path`, and topic keys must be unique.

### 5.2 Defaults

Set in `Load()` via Viper:

- `server.port = 9090`
- `server.scrape_interval_seconds = 15`
- `server.lag_poll_interval_seconds = 60`
- `server.request_timeout_seconds = 10`
- `server.worker_pool_size = clamp(runtime.NumCPU()*4, 8..64)`

## 6. Kafka Integration Details

## 6.1 Client Construction

`NewCollector()` configures:

- Kafka protocol version from `kafka.version`.
- Client ID `goklag-monitor`.
- Network timeouts (dial/read/write).
- Sarama metadata retry baseline.
- SASL enabled with handshake.
- SASL mechanism: GSSAPI.
- Kerberos auth type: keytab (`KRB5_KEYTAB_AUTH`).

A single `sarama.Client` is reused across all cycles.

## 6.2 Topic Metadata and Partition Resolution

`loadPartitions()` uses worker pool + job channel.

Each worker calls `getPartitionsWithRetry()`:

- Attempts up to `retryMax`.
- Calls `client.Partitions(topic)`.
- On failure: refresh metadata and exponential backoff.
- On repeated failure: fallback to cached partition list if available.
- Returns sorted partition IDs.

## 6.3 Latest Offset Fetching

`fetchLatestOffsets()`:

1. For each topic-partition, resolve leader broker.
2. Group partitions by broker ID.
3. Worker pool processes each broker batch.
4. Build one `OffsetRequest` per broker with `OffsetNewest` blocks.
5. Parse response blocks into `map[TopicPartition]int64`.

This design minimizes request count and improves scale at high partition counts.

## 6.4 Committed Offset Fetching

`fetchCommittedOffsets()` per consumer group:

1. Find coordinator via `client.Coordinator(group)`.
2. Build one `OffsetFetchRequest` containing all configured partitions.
3. Use request version determined by `offsetFetchVersion()` based on broker version.
4. Parse blocks; tolerate `ErrUnknownTopicOrPartition` without hard failure.

## 6.5 HDFS Committed Offset Fetching (Spark Mode)

When `kafka.hdfs_offset` is configured, committed offsets are read from HDFS instead of Kafka consumer group coordinators.

1. For each configured topic/path, execute `hdfs dfs -ls <path>`.
2. Parse entries, keep only numeric filenames, and pick the highest integer key.
3. Execute `hdfs dfs -cat <path>/<max_file>`.
4. Parse JSON of shape `{"topic": {"partition": "offset"}}`.
5. Map committed offsets by partition and compute lag using the existing lag formula.

## 6.6 Lag Computation

`computeLag(latest, committed)`:

- latest `< 0` => lag `0`
- committed `< 0` => lag `latest` (treat uncommitted as full lag)
- committed `>= latest` => lag `0`
- else => `latest - committed`

Totals:

- Topic lag = sum(partition lag)
- Group lag = sum(topic lag)

## 7. Prometheus Metrics Design

All metrics are gauge vectors:

- `kafka_consumer_partition_lag{group,topic,partition}`
- `kafka_consumer_topic_lag{group,topic}`
- `kafka_consumer_group_total_lag{group}`
- `kafka_partition_latest_offset{topic,partition}`
- `kafka_partition_committed_offset{group,topic,partition}`

### 7.1 Update Model

`Store.Update(snapshot)` is mutex-protected.

- Set all metrics present in current snapshot.
- Build current label-key sets.
- Compare with previous cycle label-key sets.
- Delete stale series to prevent unbounded series retention.

### 7.2 Label Cardinality Notes

The implementation uses labels mandated by requirements. Cardinality is bounded operationally by configured groups/topics/partitions rather than dynamic discovery.

## 8. HTTP Server Design

`internal/server` provides:

- `/metrics` via `promhttp.HandlerFor(registry, opts)`
- `/healthz` returning `200 ok`

Timeout hardening on `http.Server`:

- `ReadHeaderTimeout = 5s`
- `ReadTimeout = 10s`
- `WriteTimeout = 10s`
- `IdleTimeout = 60s`

## 9. Concurrency and Resource Management

- Bounded goroutines through worker pool sizing.
- Shared channels closed deterministically by producer goroutine.
- Worker `WaitGroup` ensures complete drain before return.
- Context checked in producer and work loops.
- Ticker stopped on shutdown.
- Kafka client closed once at process end.

## 10. Error Handling Model

- Config errors: fatal at startup.
- Metadata failures: warn + retry/backoff + cache fallback.
- Latest offset partial failures: logged, snapshot still returned.
- Group offset failures: warn and continue other groups.
- HDFS command/parse failures: warn and continue other topics.
- HTTP server fatal only on unexpected listen failure.

This prioritizes service continuity and observability over strict fail-stop behavior during transient broker issues.

## 11. Performance Characteristics

### 11.1 Efficient Patterns Used

- Single Sarama client reused globally.
- Broker-level batching for latest offsets.
- Per-group batch fetch for committed offsets.
- Optional HDFS CLI-based committed offset retrieval for Spark jobs with ephemeral consumer groups.
- Bounded workers for metadata and broker requests.
- Maps pre-sized for common snapshot dimensions.

### 11.2 Scalability Expectations

The architecture supports large topologies (100+ groups, 1000+ partitions) by reducing per-partition round trips and capping concurrent goroutines.

## 12. Security and Auth Notes

- No credentials hardcoded in source.
- Kerberos principal/keytab supplied via config.
- Service name and realm are configurable.
- Keytab file permissions should be restricted at OS level.

## 13. Function Reference (Low-Level)

## 13.1 `cmd/app/main.go`

- `main()`
  - Bootstraps logger/config/collector/server.
  - Starts polling and handles signal-based shutdown.
  - Side effects: network listeners, Kafka connections, logs.

- `collectAndPublish(rootCtx, collector, metricStore, timeout, logger)`
  - Creates per-cycle timeout context.
  - Calls collector, then metrics update.
  - Logs metric cardinalities for cycle visibility.

- `collectAndPublishHDFS(rootCtx, collector, reader, metricStore, timeout, logger)`
  - Creates per-cycle timeout context.
  - Calls HDFS-based collector path, then metrics update.
  - Logs metric cardinalities for cycle visibility.

## 13.2 `internal/config/config.go`

- `(ServerConfig) ScrapeInterval()`
  - Converts scrape seconds to `time.Duration`.

- `(ServerConfig) RequestTimeout()`
  - Converts request timeout seconds to `time.Duration`.

- `(ServerConfig) LagPollInterval()`
  - Converts lag poll seconds to `time.Duration`.

- `Load(path)`
  - Reads YAML config using Viper.
  - Applies defaults.
  - Unmarshals into typed struct and validates.

- `(*Config) Validate()`
  - Enforces mandatory fields and positive numeric values.

- `defaultWorkerPoolSize()`
  - Computes CPU-based default worker count with `[8,64]` clamp.

## 13.3 `internal/kafka` (`collector.go`, `hdfs_lag.go`)

- `NewCollector(cfg, workers, logger)`
  - Parses Kafka version.
  - Configures Sarama including GSSAPI keytab auth.
  - Creates shared client and collector state.

- `(*Collector) Close()`
  - Closes shared Sarama client.

- `(*Collector) Collect(ctx)`
  - End-to-end lag collection for all configured groups/topics.
  - Produces snapshot maps for all metric dimensions.

- `(*Collector) CollectHDFS(ctx, reader)`
  - End-to-end lag collection using Kafka latest offsets plus HDFS committed offsets.
  - Produces snapshot maps using synthetic group label `spark-hdfs`.

- `(*Collector) loadPartitions(ctx, topics)`
  - Parallel topic partition resolution with worker pool.

- `(*Collector) getPartitionsWithRetry(ctx, topic)`
  - Metadata retry with exponential backoff and cache fallback.

- `(*Collector) fetchLatestOffsets(ctx, partitionsByTopic)`
  - Batches offset head requests per leader broker.

- `(*Collector) fetchCommittedOffsets(ctx, group, topics, partitionsByTopic)`
  - Performs single batched fetch from group coordinator.

- `(*Collector) offsetFetchVersion()`
  - Maps broker version to compatible offset fetch API version.

- `configuredTopics(bindings)`
  - De-duplicates and sorts topics from consumer bindings.

- `computeLag(latest, committed)`
  - Lag rules for missing/advanced offsets.

- `min(a,b)`
  - Local helper for worker count decisions.

## 13.4 `internal/hdfsoffset/reader.go`

- `NewReader()`
  - Constructs HDFS CLI reader using `hdfs` binary.

- `(*Reader) ReadTopicOffsets(ctx, topic, basePath)`
  - Lists HDFS path, selects max numeric offset file, reads file contents, parses committed offsets by partition.

- `latestOffsetFilePath(lsOutput)`
  - Extracts the latest numeric filename from `hdfs dfs -ls` output.

- `parseTopicOffsets(content, topic)`
  - Parses topic-partition offsets from JSON payload.

## 13.5 `internal/metrics/metrics.go`

- `NewStore()`
  - Creates registry and gauge vectors.
  - Registers metrics collectors.

- `(*Store) Registry()`
  - Returns registry for HTTP handler wiring.

- `(*Store) Update(snapshot)`
  - Updates current values and deletes stale series.
  - Protected by mutex for thread safety.

- `join2/split2/join3/split3`
  - Internal label-key encoding helpers for stale-series diffing.

## 13.6 `internal/server/server.go`

- `New(port, registry, logger)`
  - Builds hardened HTTP server and route mux.

- `(*Server) Start()`
  - Starts HTTP listener asynchronously.
  - Fatal logs on unexpected server failure.

- `(*Server) Shutdown(ctx)`
  - Graceful HTTP shutdown with caller-provided context.

## 14. Test Coverage Summary

Current tests validate:

- Config defaults and lag polling validation.
- Lag math and configured topic normalization.
- HDFS offset filename selection and JSON parsing behavior.
- Metrics update + stale series cleanup.

Potential future tests:

- Mocked Sarama interaction for collector end-to-end flow.
- Failure-path behavior under coordinator/leader errors.
- HTTP endpoint tests for status and payload exposure.

## 15. Build and Artifact Workflow

Makefile targets:

- `make tidy`
- `make test`
- `make build`
- `make build-linux`
- `make build-linux-arm64`
- `make clean`

Linux production artifact:

- `bin/goklag-linux-amd64` built with:
  - `CGO_ENABLED=0`
  - `GOOS=linux`
  - `GOARCH=amd64`
  - `-trimpath`
  - stripped symbols (`-s -w`)

## 16. Operational Notes

- Tune `server.worker_pool_size` based on broker/network capacity.
- Ensure keytab readability by runtime user.
- Keep `lag_poll_interval_seconds` aligned with Prometheus scrape interval to avoid stale/over-sampled expectations.
- For very large deployments, distribute monitored groups across multiple instances to cap per-process cardinality and request load.
