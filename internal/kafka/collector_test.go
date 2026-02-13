package kafka

import (
	"reflect"
	"testing"

	"goklag/internal/config"
)

func TestComputeLag(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		latest    int64
		committed int64
		want      int64
	}{
		{name: "no latest offset", latest: -1, committed: 0, want: 0},
		{name: "missing committed offset", latest: 50, committed: -1, want: 50},
		{name: "committed ahead", latest: 100, committed: 100, want: 0},
		{name: "normal lag", latest: 120, committed: 95, want: 25},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := computeLag(tc.latest, tc.committed); got != tc.want {
				t.Fatalf("computeLag(%d,%d)=%d want %d", tc.latest, tc.committed, got, tc.want)
			}
		})
	}
}

func TestConfiguredTopics_DedupSorted(t *testing.T) {
	t.Parallel()
	bindings := []config.ConsumerBinding{
		{Group: "group-1", Topics: []string{"topic-b", "topic-a"}},
		{Group: "group-2", Topics: []string{"topic-c", "topic-a"}},
	}

	got := configuredTopics(bindings)
	want := []string{"topic-a", "topic-b", "topic-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("configuredTopics()=%v want %v", got, want)
	}
}
