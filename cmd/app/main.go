package main

import (
	"context"
	"errors"
	"flag"
	"os/signal"
	"syscall"
	"time"

	"goklag/internal/config"
	"goklag/internal/hdfsoffset"
	"goklag/internal/kafka"
	"goklag/internal/metrics"
	"goklag/internal/server"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	logCfg := zap.NewProductionConfig()
	logCfg.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	logger, err := logCfg.Build()
	if err != nil {
		panic(err)
	}
	defer func() { _ = logger.Sync() }()
	logger.Info("starting goklag",
		zap.String("version", version),
		zap.String("commit", commit),
		zap.String("build_date", buildDate),
	)

	cfg, err := config.Load(*configPath)
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	collector, err := kafka.NewCollector(cfg.Kafka, cfg.Server.WorkerPoolSize, logger)
	if err != nil {
		logger.Fatal("failed to build kafka collector", zap.Error(err))
	}
	defer func() {
		if cerr := collector.Close(); cerr != nil {
			logger.Warn("failed to close kafka client", zap.Error(cerr))
		}
	}()

	metricStore := metrics.NewStore()
	httpServer := server.New(cfg.Server.Port, metricStore.Registry(), logger)
	httpServer.Start()

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	runCollect := func(ctx context.Context) {
		collectAndPublish(ctx, collector, metricStore, cfg.Server.RequestTimeout(), logger)
	}
	if len(cfg.Kafka.HDFSOffset) > 0 {
		hdfsReader := hdfsoffset.NewReader()
		logger.Info("using hdfs committed offsets for lag collection",
			zap.Int("hdfs_topics", len(cfg.Kafka.HDFSOffset)),
		)
		runCollect = func(ctx context.Context) {
			collectAndPublishCombined(ctx, collector, hdfsReader, metricStore, cfg.Server.RequestTimeout(), logger)
		}
	}

	runCollect(rootCtx)

	ticker := time.NewTicker(cfg.Server.LagPollInterval())
	defer ticker.Stop()

	run := true
	for run {
		select {
		case <-rootCtx.Done():
			run = false
		case <-ticker.C:
			runCollect(rootCtx)
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Warn("http shutdown failed", zap.Error(err))
	}

	logger.Info("shutdown complete")
}

func collectAndPublish(
	rootCtx context.Context,
	collector *kafka.Collector,
	metricStore *metrics.Store,
	timeout time.Duration,
	logger *zap.Logger,
) {
	ctx, cancel := context.WithTimeout(rootCtx, timeout)
	defer cancel()

	snapshot, err := collector.Collect(ctx)
	if err != nil {
		logger.Warn("collection failed", zap.Error(err))
		return
	}

	metricStore.Update(snapshot)
	logger.Info("collection completed",
		zap.Int("partition_lag_metrics", len(snapshot.PartitionLag)),
		zap.Int("topic_lag_metrics", len(snapshot.TopicLag)),
		zap.Int("group_lag_metrics", len(snapshot.GroupLag)),
	)
}

func collectAndPublishHDFS(
	rootCtx context.Context,
	collector *kafka.Collector,
	reader kafka.HDFSOffsetReader,
	metricStore *metrics.Store,
	timeout time.Duration,
	logger *zap.Logger,
) {
	ctx, cancel := context.WithTimeout(rootCtx, timeout)
	defer cancel()

	snapshot, err := collector.CollectHDFS(ctx, reader)
	if err != nil {
		logger.Warn("hdfs collection failed", zap.Error(err))
		return
	}

	metricStore.Update(snapshot)
	logger.Info("hdfs collection completed",
		zap.Int("partition_lag_metrics", len(snapshot.PartitionLag)),
		zap.Int("topic_lag_metrics", len(snapshot.TopicLag)),
		zap.Int("group_lag_metrics", len(snapshot.GroupLag)),
	)
}

func collectAndPublishCombined(
	rootCtx context.Context,
	collector *kafka.Collector,
	reader kafka.HDFSOffsetReader,
	metricStore *metrics.Store,
	timeout time.Duration,
	logger *zap.Logger,
) {
	var (
		combined     kafka.Snapshot
		haveSnapshot bool
	)

	if snapshot, ok := collectKafkaSnapshot(rootCtx, collector, timeout, logger); ok {
		combined = snapshot
		haveSnapshot = true
	}

	if snapshot, ok := collectHDFSSnapshot(rootCtx, collector, reader, timeout, logger); ok {
		if haveSnapshot {
			combined = mergeSnapshots(combined, snapshot)
		} else {
			combined = snapshot
			haveSnapshot = true
		}
	}

	if !haveSnapshot {
		logger.Warn("both kafka and hdfs collections failed")
		return
	}

	metricStore.Update(combined)
	logger.Info("combined collection completed",
		zap.Int("partition_lag_metrics", len(combined.PartitionLag)),
		zap.Int("topic_lag_metrics", len(combined.TopicLag)),
		zap.Int("group_lag_metrics", len(combined.GroupLag)),
	)
}

func collectKafkaSnapshot(
	rootCtx context.Context,
	collector *kafka.Collector,
	timeout time.Duration,
	logger *zap.Logger,
) (kafka.Snapshot, bool) {
	ctx, cancel := context.WithTimeout(rootCtx, timeout)
	defer cancel()

	snapshot, err := collector.Collect(ctx)
	if err != nil {
		logger.Warn("kafka collection failed", zap.Error(err))
		return kafka.Snapshot{}, false
	}
	return snapshot, true
}

func collectHDFSSnapshot(
	rootCtx context.Context,
	collector *kafka.Collector,
	reader kafka.HDFSOffsetReader,
	timeout time.Duration,
	logger *zap.Logger,
) (kafka.Snapshot, bool) {
	ctx, cancel := context.WithTimeout(rootCtx, timeout)
	defer cancel()

	snapshot, err := collector.CollectHDFS(ctx, reader)
	if err != nil {
		logger.Warn("hdfs collection failed", zap.Error(err))
		return kafka.Snapshot{}, false
	}
	return snapshot, true
}

func mergeSnapshots(a, b kafka.Snapshot) kafka.Snapshot {
	out := kafka.Snapshot{
		PartitionLag:      make(map[kafka.GroupTopicPartition]float64, len(a.PartitionLag)+len(b.PartitionLag)),
		TopicLag:          make(map[kafka.GroupTopic]float64, len(a.TopicLag)+len(b.TopicLag)),
		GroupLag:          make(map[string]float64, len(a.GroupLag)+len(b.GroupLag)),
		LatestOffset:      make(map[kafka.TopicPartition]float64, len(a.LatestOffset)+len(b.LatestOffset)),
		CommittedOffset:   make(map[kafka.GroupTopicPartition]float64, len(a.CommittedOffset)+len(b.CommittedOffset)),
		CollectionSuccess: 1,
	}

	for k, v := range a.PartitionLag {
		out.PartitionLag[k] = v
	}
	for k, v := range b.PartitionLag {
		out.PartitionLag[k] = v
	}
	for k, v := range a.TopicLag {
		out.TopicLag[k] = v
	}
	for k, v := range b.TopicLag {
		out.TopicLag[k] = v
	}
	for k, v := range a.GroupLag {
		out.GroupLag[k] = v
	}
	for k, v := range b.GroupLag {
		out.GroupLag[k] = v
	}
	for k, v := range a.LatestOffset {
		out.LatestOffset[k] = v
	}
	for k, v := range b.LatestOffset {
		out.LatestOffset[k] = v
	}
	for k, v := range a.CommittedOffset {
		out.CommittedOffset[k] = v
	}
	for k, v := range b.CommittedOffset {
		out.CommittedOffset[k] = v
	}

	return out
}
