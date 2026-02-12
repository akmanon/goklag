package metrics

import (
	"strconv"
	"sync"

	"goklag/internal/kafka"

	"github.com/prometheus/client_golang/prometheus"
)

type Store struct {
	registry *prometheus.Registry

	partitionLag    *prometheus.GaugeVec
	topicLag        *prometheus.GaugeVec
	groupLag        *prometheus.GaugeVec
	latestOffset    *prometheus.GaugeVec
	committedOffset *prometheus.GaugeVec

	mu sync.Mutex

	prevPartitionLag    map[string]struct{}
	prevTopicLag        map[string]struct{}
	prevGroupLag        map[string]struct{}
	prevLatestOffset    map[string]struct{}
	prevCommittedOffset map[string]struct{}
}

func NewStore() *Store {
	registry := prometheus.NewRegistry()
	store := &Store{
		registry: registry,
		partitionLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_partition_lag",
			Help: "Consumer lag per partition.",
		}, []string{"group", "topic", "partition"}),
		topicLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_topic_lag",
			Help: "Total consumer lag per topic.",
		}, []string{"group", "topic"}),
		groupLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_consumer_group_total_lag",
			Help: "Total consumer lag per group.",
		}, []string{"group"}),
		latestOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_partition_latest_offset",
			Help: "Latest broker offset per partition.",
		}, []string{"topic", "partition"}),
		committedOffset: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_partition_committed_offset",
			Help: "Committed offset per consumer group partition.",
		}, []string{"group", "topic", "partition"}),
		prevPartitionLag:    make(map[string]struct{}),
		prevTopicLag:        make(map[string]struct{}),
		prevGroupLag:        make(map[string]struct{}),
		prevLatestOffset:    make(map[string]struct{}),
		prevCommittedOffset: make(map[string]struct{}),
	}

	registry.MustRegister(
		store.partitionLag,
		store.topicLag,
		store.groupLag,
		store.latestOffset,
		store.committedOffset,
	)

	return store
}

func (s *Store) Registry() *prometheus.Registry {
	return s.registry
}

func (s *Store) Update(snapshot kafka.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()

	currentPartitionLag := make(map[string]struct{}, len(snapshot.PartitionLag))
	for key, value := range snapshot.PartitionLag {
		partition := strconv.Itoa(int(key.Partition))
		s.partitionLag.WithLabelValues(key.Group, key.Topic, partition).Set(value)
		currentPartitionLag[join3(key.Group, key.Topic, partition)] = struct{}{}
	}

	currentTopicLag := make(map[string]struct{}, len(snapshot.TopicLag))
	for key, value := range snapshot.TopicLag {
		s.topicLag.WithLabelValues(key.Group, key.Topic).Set(value)
		currentTopicLag[join2(key.Group, key.Topic)] = struct{}{}
	}

	currentGroupLag := make(map[string]struct{}, len(snapshot.GroupLag))
	for group, value := range snapshot.GroupLag {
		s.groupLag.WithLabelValues(group).Set(value)
		currentGroupLag[group] = struct{}{}
	}

	currentLatestOffset := make(map[string]struct{}, len(snapshot.LatestOffset))
	for key, value := range snapshot.LatestOffset {
		partition := strconv.Itoa(int(key.Partition))
		s.latestOffset.WithLabelValues(key.Topic, partition).Set(value)
		currentLatestOffset[join2(key.Topic, partition)] = struct{}{}
	}

	currentCommittedOffset := make(map[string]struct{}, len(snapshot.CommittedOffset))
	for key, value := range snapshot.CommittedOffset {
		partition := strconv.Itoa(int(key.Partition))
		s.committedOffset.WithLabelValues(key.Group, key.Topic, partition).Set(value)
		currentCommittedOffset[join3(key.Group, key.Topic, partition)] = struct{}{}
	}

	for oldKey := range s.prevPartitionLag {
		if _, ok := currentPartitionLag[oldKey]; !ok {
			group, topic, partition := split3(oldKey)
			s.partitionLag.DeleteLabelValues(group, topic, partition)
		}
	}

	for oldKey := range s.prevTopicLag {
		if _, ok := currentTopicLag[oldKey]; !ok {
			group, topic := split2(oldKey)
			s.topicLag.DeleteLabelValues(group, topic)
		}
	}

	for oldKey := range s.prevGroupLag {
		if _, ok := currentGroupLag[oldKey]; !ok {
			s.groupLag.DeleteLabelValues(oldKey)
		}
	}

	for oldKey := range s.prevLatestOffset {
		if _, ok := currentLatestOffset[oldKey]; !ok {
			topic, partition := split2(oldKey)
			s.latestOffset.DeleteLabelValues(topic, partition)
		}
	}

	for oldKey := range s.prevCommittedOffset {
		if _, ok := currentCommittedOffset[oldKey]; !ok {
			group, topic, partition := split3(oldKey)
			s.committedOffset.DeleteLabelValues(group, topic, partition)
		}
	}

	s.prevPartitionLag = currentPartitionLag
	s.prevTopicLag = currentTopicLag
	s.prevGroupLag = currentGroupLag
	s.prevLatestOffset = currentLatestOffset
	s.prevCommittedOffset = currentCommittedOffset
}

func join2(a, b string) string {
	return a + "\x1f" + b
}

func split2(v string) (string, string) {
	for i := 0; i < len(v); i++ {
		if v[i] == '\x1f' {
			return v[:i], v[i+1:]
		}
	}
	return v, ""
}

func join3(a, b, c string) string {
	return a + "\x1f" + b + "\x1f" + c
}

func split3(v string) (string, string, string) {
	a, rest := split2(v)
	b, c := split2(rest)
	return a, b, c
}
