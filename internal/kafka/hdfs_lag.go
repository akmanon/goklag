package kafka

import (
	"context"

	"go.uber.org/zap"
)

const SparkHDFSGroup = "spark-hdfs"

type HDFSOffsetReader interface {
	ReadTopicOffsets(ctx context.Context, topic, basePath string) (map[int32]int64, error)
}

func (c *Collector) CollectHDFS(ctx context.Context, reader HDFSOffsetReader) (Snapshot, error) {
	snapshot := Snapshot{
		PartitionLag:    make(map[GroupTopicPartition]float64, 1024),
		TopicLag:        make(map[GroupTopic]float64, 256),
		GroupLag:        make(map[string]float64, 1),
		LatestOffset:    make(map[TopicPartition]float64, 1024),
		CommittedOffset: make(map[GroupTopicPartition]float64, 1024),
	}

	topics := make([]string, 0, len(c.cfg.HDFSOffset))
	paths := make(map[string]string, len(c.cfg.HDFSOffset))
	for _, binding := range c.cfg.HDFSOffset {
		topics = append(topics, binding.Topic)
		paths[binding.Topic] = binding.Path
	}

	partitionsByTopic := c.loadPartitions(ctx, topics)
	latestByPartition, latestErr := c.fetchLatestOffsets(ctx, partitionsByTopic)
	if latestErr != nil {
		c.logger.Warn("latest offset fetch had partial failures", zap.Error(latestErr))
	}

	for tp, latest := range latestByPartition {
		snapshot.LatestOffset[tp] = float64(latest)
	}

	groupTotal := 0.0
	for _, topic := range topics {
		select {
		case <-ctx.Done():
			return snapshot, ctx.Err()
		default:
		}

		committedByPartition, err := reader.ReadTopicOffsets(ctx, topic, paths[topic])
		if err != nil {
			c.logger.Warn("hdfs committed offset fetch failed",
				zap.String("group", SparkHDFSGroup),
				zap.String("topic", topic),
				zap.Error(err),
			)
		}

		partitions, ok := partitionsByTopic[topic]
		if !ok || len(partitions) == 0 {
			snapshot.TopicLag[GroupTopic{Group: SparkHDFSGroup, Topic: topic}] = 0
			continue
		}

		topicTotal := 0.0
		for _, partition := range partitions {
			tp := TopicPartition{Topic: topic, Partition: partition}
			gtp := GroupTopicPartition{Group: SparkHDFSGroup, Topic: topic, Partition: partition}

			latest, hasLatest := latestByPartition[tp]
			if !hasLatest {
				latest = -1
			}

			committed, hasCommitted := committedByPartition[partition]
			if !hasCommitted {
				committed = -1
			}

			lag := computeLag(latest, committed)
			topicTotal += float64(lag)

			snapshot.PartitionLag[gtp] = float64(lag)
			snapshot.CommittedOffset[gtp] = float64(committed)
		}

		snapshot.TopicLag[GroupTopic{Group: SparkHDFSGroup, Topic: topic}] = topicTotal
		groupTotal += topicTotal
	}

	snapshot.GroupLag[SparkHDFSGroup] = groupTotal

	if latestErr != nil {
		snapshot.CollectionSuccess = 0
		return snapshot, nil
	}

	snapshot.CollectionSuccess = 1
	return snapshot, nil
}
