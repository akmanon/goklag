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

	got, err := latestOffsetFile(input)
	if err != nil {
		t.Fatalf("latestOffsetFile() error = %v", err)
	}

	if got.path != "/jobs/appname/offset/7656" {
		t.Fatalf("latestOffsetFile().path = %q, want %q", got.path, "/jobs/appname/offset/7656")
	}
	if got.size != 8 {
		t.Fatalf("latestOffsetFile().size = %d, want %d", got.size, 8)
	}
}

func TestLatestOffsetFilePath_NoNumericFiles(t *testing.T) {
	t.Parallel()

	input := []byte(`Found 1 items
-rw-r--r-- 1 hdfs supergroup 8 2026-02-18 00:00 /jobs/appname/offset/abc
`)

	_, err := latestOffsetFile(input)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestValidateOffsetFileSize(t *testing.T) {
	t.Parallel()

	if err := validateOffsetFileSize(offsetFile{path: "/jobs/appname/offset/7656", size: maxOffsetFileSizeBytes - 1}); err != nil {
		t.Fatalf("expected no error for size below limit, got %v", err)
	}

	err := validateOffsetFileSize(offsetFile{path: "/jobs/appname/offset/7657", size: maxOffsetFileSizeBytes})
	if err == nil {
		t.Fatal("expected error for size at limit, got nil")
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

func TestParseTopicOffsets_SparkCheckpointThreeLineFormat(t *testing.T) {
	t.Parallel()

	content := []byte(`v1
{"batchWatermarkMs":0,"batchTimestampMs":1771487442188,"conf":{"spark.sql.streaming.stateStore.providerClass":"org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"}}
{"topic_name":{"2":33443953,"5":1800190,"4":1781233,"1":33448138,"3":1787026,"0":33423016}}`)

	offsets, err := parseTopicOffsets(content, "topic_name")
	if err != nil {
		t.Fatalf("parseTopicOffsets() error = %v", err)
	}

	if got := offsets[0]; got != 33423016 {
		t.Fatalf("partition 0 offset = %d, want 33423016", got)
	}
	if got := offsets[5]; got != 1800190 {
		t.Fatalf("partition 5 offset = %d, want 1800190", got)
	}
}
