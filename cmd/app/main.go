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
			collectAndPublishHDFS(ctx, collector, hdfsReader, metricStore, cfg.Server.RequestTimeout(), logger)
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
