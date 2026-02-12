package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"goklag/internal/config"

	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

type GroupTopicPartition struct {
	Group     string
	Topic     string
	Partition int32
}

type GroupTopic struct {
	Group string
	Topic string
}

type TopicPartition struct {
	Topic     string
	Partition int32
}

type Snapshot struct {
	PartitionLag      map[GroupTopicPartition]float64
	TopicLag          map[GroupTopic]float64
	GroupLag          map[string]float64
	LatestOffset      map[TopicPartition]float64
	CommittedOffset   map[GroupTopicPartition]float64
	CollectionSuccess float64
}

type Collector struct {
	client     sarama.Client
	cfg        config.KafkaConfig
	workers    int
	logger     *zap.Logger
	retryMax   int
	retryBase  time.Duration
	kafkaVer   sarama.KafkaVersion
	metaMu     sync.RWMutex
	topicCache map[string][]int32
}

func NewCollector(cfg config.KafkaConfig, workers int, logger *zap.Logger) (*Collector, error) {
	kafkaVer, err := sarama.ParseKafkaVersion(cfg.Version)
	if err != nil {
		return nil, fmt.Errorf("parse kafka version: %w", err)
	}

	scfg := sarama.NewConfig()
	scfg.Version = kafkaVer
	scfg.ClientID = "goklag-monitor"
	scfg.Net.DialTimeout = 8 * time.Second
	scfg.Net.ReadTimeout = 8 * time.Second
	scfg.Net.WriteTimeout = 8 * time.Second
	scfg.Metadata.Retry.Max = 3
	scfg.Metadata.Retry.Backoff = 500 * time.Millisecond
	scfg.Metadata.Full = false
	scfg.Net.SASL.Enable = true
	scfg.Net.SASL.Handshake = true

	if strings.ToUpper(cfg.SASL.Mechanism) != string(sarama.SASLTypeGSSAPI) {
		return nil, fmt.Errorf("unsupported SASL mechanism %q, expected %q", cfg.SASL.Mechanism, sarama.SASLTypeGSSAPI)
	}

	scfg.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
	scfg.Net.SASL.GSSAPI.AuthType = sarama.KRB5_KEYTAB_AUTH
	scfg.Net.SASL.GSSAPI.ServiceName = cfg.SASL.Kerberos.ServiceName
	scfg.Net.SASL.GSSAPI.Username = cfg.SASL.Kerberos.Username
	scfg.Net.SASL.GSSAPI.Realm = cfg.SASL.Kerberos.Realm
	scfg.Net.SASL.GSSAPI.KeyTabPath = cfg.SASL.Kerberos.KeytabPath
	scfg.Net.SASL.GSSAPI.DisablePAFXFAST = true

	client, err := sarama.NewClient(cfg.Brokers, scfg)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &Collector{
		client:     client,
		cfg:        cfg,
		workers:    workers,
		logger:     logger,
		retryMax:   5,
		retryBase:  300 * time.Millisecond,
		kafkaVer:   kafkaVer,
		topicCache: make(map[string][]int32),
	}, nil
}

func (c *Collector) Close() error {
	return c.client.Close()
}

func (c *Collector) Collect(ctx context.Context) (Snapshot, error) {
	snapshot := Snapshot{
		PartitionLag:    make(map[GroupTopicPartition]float64, 1024),
		TopicLag:        make(map[GroupTopic]float64, 256),
		GroupLag:        make(map[string]float64, 128),
		LatestOffset:    make(map[TopicPartition]float64, 1024),
		CommittedOffset: make(map[GroupTopicPartition]float64, 1024),
	}

	topics := configuredTopics(c.cfg.Consumers)
	partitionsByTopic := c.loadPartitions(ctx, topics)
	latestByPartition, latestErr := c.fetchLatestOffsets(ctx, partitionsByTopic)
	if latestErr != nil {
		c.logger.Warn("latest offset fetch had partial failures", zap.Error(latestErr))
	}

	for tp, latest := range latestByPartition {
		snapshot.LatestOffset[tp] = float64(latest)
	}

	for _, binding := range c.cfg.Consumers {
		select {
		case <-ctx.Done():
			return snapshot, ctx.Err()
		default:
		}

		committedByPartition, err := c.fetchCommittedOffsets(ctx, binding.Group, binding.Topics, partitionsByTopic)
		if err != nil {
			c.logger.Warn("committed offset fetch failed", zap.String("group", binding.Group), zap.Error(err))
		}

		var groupTotal float64
		for _, topic := range binding.Topics {
			partitions, ok := partitionsByTopic[topic]
			if !ok || len(partitions) == 0 {
				snapshot.TopicLag[GroupTopic{Group: binding.Group, Topic: topic}] = 0
				continue
			}

			var topicTotal float64
			for _, partition := range partitions {
				tp := TopicPartition{Topic: topic, Partition: partition}
				gtp := GroupTopicPartition{Group: binding.Group, Topic: topic, Partition: partition}

				latest, hasLatest := latestByPartition[tp]
				if !hasLatest {
					latest = -1
				}
				committed, hasCommitted := committedByPartition[tp]
				if !hasCommitted {
					committed = -1
				}

				lag := computeLag(latest, committed)
				topicTotal += float64(lag)

				snapshot.PartitionLag[gtp] = float64(lag)
				snapshot.CommittedOffset[gtp] = float64(committed)
			}

			snapshot.TopicLag[GroupTopic{Group: binding.Group, Topic: topic}] = topicTotal
			groupTotal += topicTotal
		}

		snapshot.GroupLag[binding.Group] = groupTotal
	}

	if latestErr != nil {
		snapshot.CollectionSuccess = 0
		return snapshot, nil
	}

	snapshot.CollectionSuccess = 1
	return snapshot, nil
}

func (c *Collector) loadPartitions(ctx context.Context, topics []string) map[string][]int32 {
	out := make(map[string][]int32, len(topics))
	if len(topics) == 0 {
		return out
	}

	type result struct {
		topic      string
		partitions []int32
	}

	jobs := make(chan string)
	results := make(chan result, len(topics))
	workerCount := min(c.workers, len(topics))
	if workerCount == 0 {
		workerCount = 1
	}

	var wg sync.WaitGroup
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for topic := range jobs {
				parts, err := c.getPartitionsWithRetry(ctx, topic)
				if err != nil {
					c.logger.Warn("topic metadata unavailable",
						zap.String("topic", topic),
						zap.Error(err),
					)
					continue
				}
				results <- result{topic: topic, partitions: parts}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, topic := range topics {
			select {
			case <-ctx.Done():
				return
			case jobs <- topic:
			}
		}
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	for res := range results {
		out[res.topic] = res.partitions
	}

	return out
}

func (c *Collector) getPartitionsWithRetry(ctx context.Context, topic string) ([]int32, error) {
	var lastErr error
	for attempt := 0; attempt < c.retryMax; attempt++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		parts, err := c.client.Partitions(topic)
		if err == nil {
			sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
			c.metaMu.Lock()
			c.topicCache[topic] = parts
			c.metaMu.Unlock()
			return parts, nil
		}

		lastErr = err
		_ = c.client.RefreshMetadata(topic)
		backoff := c.retryBase * time.Duration(math.Pow(2, float64(attempt)))
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	c.metaMu.RLock()
	cached := c.topicCache[topic]
	c.metaMu.RUnlock()
	if len(cached) > 0 {
		return cached, nil
	}

	return nil, lastErr
}

func (c *Collector) fetchLatestOffsets(ctx context.Context, partitionsByTopic map[string][]int32) (map[TopicPartition]int64, error) {
	out := make(map[TopicPartition]int64, 2048)
	type leaderBatch struct {
		broker *sarama.Broker
		parts  map[string][]int32
	}

	batches := make(map[int32]*leaderBatch)
	for topic, partitions := range partitionsByTopic {
		for _, partition := range partitions {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			default:
			}

			leader, err := c.client.Leader(topic, partition)
			if err != nil {
				c.logger.Warn("leader lookup failed", zap.String("topic", topic), zap.Int32("partition", partition), zap.Error(err))
				continue
			}
			batch, ok := batches[leader.ID()]
			if !ok {
				batch = &leaderBatch{broker: leader, parts: make(map[string][]int32)}
				batches[leader.ID()] = batch
			}
			batch.parts[topic] = append(batch.parts[topic], partition)
		}
	}

	var (
		mu    sync.Mutex
		errMu sync.Mutex
		errs  []error
	)
	jobs := make(chan *leaderBatch)
	workerCount := min(c.workers, len(batches))
	if workerCount == 0 {
		workerCount = 1
	}
	var wg sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range jobs {
				req := &sarama.OffsetRequest{Version: 1}
				for topic, partitions := range batch.parts {
					for _, partition := range partitions {
						req.AddBlock(topic, partition, sarama.OffsetNewest, 1)
					}
				}

				resp, err := batch.broker.GetAvailableOffsets(req)
				if err != nil {
					errMu.Lock()
					errs = append(errs, fmt.Errorf("broker %d: %w", batch.broker.ID(), err))
					errMu.Unlock()
					continue
				}

				local := make(map[TopicPartition]int64, 256)
				for topic, partitions := range batch.parts {
					for _, partition := range partitions {
						block := resp.GetBlock(topic, partition)
						if block == nil || block.Err != sarama.ErrNoError || len(block.Offsets) == 0 {
							continue
						}
						local[TopicPartition{Topic: topic, Partition: partition}] = block.Offsets[0]
					}
				}

				mu.Lock()
				for tp, off := range local {
					out[tp] = off
				}
				mu.Unlock()
			}
		}()
	}

	go func() {
		defer close(jobs)
		for _, batch := range batches {
			select {
			case <-ctx.Done():
				return
			case jobs <- batch:
			}
		}
	}()

	wg.Wait()
	if len(errs) > 0 {
		return out, errors.Join(errs...)
	}
	return out, nil
}

func (c *Collector) fetchCommittedOffsets(
	ctx context.Context,
	group string,
	topics []string,
	partitionsByTopic map[string][]int32,
) (map[TopicPartition]int64, error) {
	out := make(map[TopicPartition]int64, 1024)
	coordinator, err := c.client.Coordinator(group)
	if err != nil {
		return out, fmt.Errorf("find coordinator for group %s: %w", group, err)
	}

	request := &sarama.OffsetFetchRequest{ConsumerGroup: group, Version: c.offsetFetchVersion()}
	addedPartitions := 0
	for _, topic := range topics {
		partitions, ok := partitionsByTopic[topic]
		if !ok {
			continue
		}
		for _, partition := range partitions {
			request.AddPartition(topic, partition)
			addedPartitions++
		}
	}

	if addedPartitions == 0 {
		return out, nil
	}

	response, err := coordinator.FetchOffset(request)
	if err != nil {
		return out, fmt.Errorf("fetch committed offsets for group %s: %w", group, err)
	}

	for _, topic := range topics {
		partitions, ok := partitionsByTopic[topic]
		if !ok {
			continue
		}
		for _, partition := range partitions {
			block := response.GetBlock(topic, partition)
			if block == nil {
				continue
			}
			if block.Err != sarama.ErrNoError && block.Err != sarama.ErrUnknownTopicOrPartition {
				continue
			}
			out[TopicPartition{Topic: topic, Partition: partition}] = block.Offset
		}
	}

	return out, nil
}

func (c *Collector) offsetFetchVersion() int16 {
	switch {
	case c.kafkaVer.IsAtLeast(sarama.V3_0_0_0):
		return 7
	case c.kafkaVer.IsAtLeast(sarama.V2_5_0_0):
		return 6
	case c.kafkaVer.IsAtLeast(sarama.V2_1_0_0):
		return 5
	case c.kafkaVer.IsAtLeast(sarama.V2_0_0_0):
		return 4
	default:
		return 1
	}
}

func configuredTopics(bindings []config.ConsumerBinding) []string {
	set := make(map[string]struct{})
	for _, binding := range bindings {
		for _, topic := range binding.Topics {
			set[topic] = struct{}{}
		}
	}
	out := make([]string, 0, len(set))
	for topic := range set {
		out = append(out, topic)
	}
	sort.Strings(out)
	return out
}

func computeLag(latest, committed int64) int64 {
	if latest < 0 {
		return 0
	}
	if committed < 0 {
		return latest
	}
	if committed >= latest {
		return 0
	}
	return latest - committed
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
