package hdfsoffset

import (
	"testing"
)

func TestLatestOffsetFilePath(t *testing.T) {
	t.Parallel()

	input := []byte(`Found 4 items
-rw-r--r-- 1 hdfs supergroup 8 2026-02-18 00:00 /jobs/appname/offset/7654
-rw-r--r-- 1 hdfs supergroup 8 2026-02-18 00:01 /jobs/appname/offset/7656
-rw-r--r-- 1 hdfs supergroup 8 2026-02-18 00:02 /jobs/appname/offset/not-a-number
-rw-r--r-- 1 hdfs supergroup 8 2026-02-18 00:03 /jobs/appname/offset/7655
`)

	got, err := latestOffsetFilePath(input)
	if err != nil {
		t.Fatalf("latestOffsetFilePath() error = %v", err)
	}

	want := "/jobs/appname/offset/7656"
	if got != want {
		t.Fatalf("latestOffsetFilePath() = %q, want %q", got, want)
	}
}

func TestLatestOffsetFilePath_NoNumericFiles(t *testing.T) {
	t.Parallel()

	input := []byte(`Found 1 items
-rw-r--r-- 1 hdfs supergroup 8 2026-02-18 00:00 /jobs/appname/offset/abc
`)

	_, err := latestOffsetFilePath(input)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestParseTopicOffsets(t *testing.T) {
	t.Parallel()

	content := []byte(`{"topic-a":{"0":"101","1":205}}`)
	offsets, err := parseTopicOffsets(content, "topic-a")
	if err != nil {
		t.Fatalf("parseTopicOffsets() error = %v", err)
	}

	if got := offsets[0]; got != 101 {
		t.Fatalf("partition 0 offset = %d, want 101", got)
	}
	if got := offsets[1]; got != 205 {
		t.Fatalf("partition 1 offset = %d, want 205", got)
	}
}

func TestParseTopicOffsets_MissingTopic(t *testing.T) {
	t.Parallel()

	content := []byte(`{"topic-a":{"0":"101"}}`)
	_, err := parseTopicOffsets(content, "topic-b")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
