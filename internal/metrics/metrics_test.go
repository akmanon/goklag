package metrics

import (
	"testing"

	dto "github.com/prometheus/client_model/go"
	"goklag/internal/kafka"
)

func TestUpdateAndCleanupSeries(t *testing.T) {
	t.Parallel()

	store := NewStore()

	first := kafka.Snapshot{
		PartitionLag: map[kafka.GroupTopicPartition]float64{
			{Group: "group-1", Topic: "topic-a", Partition: 0}: 10,
		},
		TopicLag: map[kafka.GroupTopic]float64{
			{Group: "group-1", Topic: "topic-a"}: 10,
		},
		GroupLag: map[string]float64{
			"group-1": 10,
		},
		LatestOffset: map[kafka.TopicPartition]float64{
			{Topic: "topic-a", Partition: 0}: 100,
		},
		CommittedOffset: map[kafka.GroupTopicPartition]float64{
			{Group: "group-1", Topic: "topic-a", Partition: 0}: 90,
		},
	}
	store.Update(first)

	assertMetricValue(t, store, "kafka_consumer_partition_lag", map[string]string{
		"group": "group-1", "topic": "topic-a", "partition": "0",
	}, 10)
	assertMetricValue(t, store, "kafka_consumer_topic_lag", map[string]string{
		"group": "group-1", "topic": "topic-a",
	}, 10)
	assertMetricValue(t, store, "kafka_consumer_group_total_lag", map[string]string{
		"group": "group-1",
	}, 10)

	store.Update(kafka.Snapshot{
		PartitionLag:    map[kafka.GroupTopicPartition]float64{},
		TopicLag:        map[kafka.GroupTopic]float64{},
		GroupLag:        map[string]float64{},
		LatestOffset:    map[kafka.TopicPartition]float64{},
		CommittedOffset: map[kafka.GroupTopicPartition]float64{},
	})

	assertMetricMissing(t, store, "kafka_consumer_partition_lag", map[string]string{
		"group": "group-1", "topic": "topic-a", "partition": "0",
	})
	assertMetricMissing(t, store, "kafka_consumer_topic_lag", map[string]string{
		"group": "group-1", "topic": "topic-a",
	})
	assertMetricMissing(t, store, "kafka_consumer_group_total_lag", map[string]string{
		"group": "group-1",
	})
	assertMetricMissing(t, store, "kafka_partition_latest_offset", map[string]string{
		"topic": "topic-a", "partition": "0",
	})
	assertMetricMissing(t, store, "kafka_partition_committed_offset", map[string]string{
		"group": "group-1", "topic": "topic-a", "partition": "0",
	})
}

func assertMetricValue(t *testing.T, store *Store, name string, labels map[string]string, want float64) {
	t.Helper()
	value, found := findMetric(t, store, name, labels)
	if !found {
		t.Fatalf("metric %s with labels %v not found", name, labels)
	}
	if value != want {
		t.Fatalf("metric %s with labels %v got %v want %v", name, labels, value, want)
	}
}

func assertMetricMissing(t *testing.T, store *Store, name string, labels map[string]string) {
	t.Helper()
	_, found := findMetric(t, store, name, labels)
	if found {
		t.Fatalf("metric %s with labels %v should be missing", name, labels)
	}
}

func findMetric(t *testing.T, store *Store, name string, labels map[string]string) (float64, bool) {
	t.Helper()
	mfs, err := store.Registry().Gather()
	if err != nil {
		t.Fatalf("gather metrics: %v", err)
	}

	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, metric := range mf.GetMetric() {
			if !labelsMatch(metric.GetLabel(), labels) {
				continue
			}
			if metric.Gauge == nil {
				return 0, false
			}
			return metric.Gauge.GetValue(), true
		}
	}
	return 0, false
}

func labelsMatch(actual []*dto.LabelPair, expected map[string]string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for _, label := range actual {
		want, ok := expected[label.GetName()]
		if !ok {
			return false
		}
		if label.GetValue() != want {
			return false
		}
	}
	return true
}
