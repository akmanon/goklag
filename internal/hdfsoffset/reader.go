package hdfsoffset

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path"
	"strconv"
	"strings"
)

type Reader struct {
	binary string
}

const maxOffsetFileSizeBytes int64 = 1 << 20

type offsetFile struct {
	path string
	size int64
}

func NewReader() *Reader {
	return &Reader{binary: "hdfs"}
}

func (r *Reader) ReadTopicOffsets(ctx context.Context, topic, basePath string) (map[int32]int64, error) {
	lsOutput, err := r.run(ctx, "dfs", "-ls", basePath)
	if err != nil {
		return nil, fmt.Errorf("list hdfs offset path %q: %w", basePath, err)
	}

	file, err := latestOffsetFile(lsOutput)
	if err != nil {
		return nil, fmt.Errorf("select latest offset file in %q: %w", basePath, err)
	}
	if err := validateOffsetFileSize(file); err != nil {
		return nil, err
	}

	content, err := r.run(ctx, "dfs", "-cat", file.path)
	if err != nil {
		return nil, fmt.Errorf("read hdfs offset file %q: %w", file.path, err)
	}

	offsets, err := parseTopicOffsets(content, topic)
	if err != nil {
		return nil, fmt.Errorf("parse hdfs offset file %q: %w", file.path, err)
	}

	return offsets, nil
}

func (r *Reader) run(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, r.binary, args...)
	output, err := cmd.CombinedOutput()
	if err != nil {
		commandText := strings.TrimSpace(r.binary + " " + strings.Join(args, " "))
		stderr := strings.TrimSpace(string(output))
		if stderr == "" {
			return nil, fmt.Errorf("run %q: %w", commandText, err)
		}
		return nil, fmt.Errorf("run %q: %w: %s", commandText, err, stderr)
	}
	return output, nil
}

func latestOffsetFile(lsOutput []byte) (offsetFile, error) {
	scanner := bufio.NewScanner(strings.NewReader(string(lsOutput)))
	var (
		maxValue int64
		selected offsetFile
		hasValue bool
	)

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "Found ") {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}

		candidatePath := fields[len(fields)-1]
		name := path.Base(candidatePath)
		value, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			continue
		}
		size, err := strconv.ParseInt(fields[len(fields)-4], 10, 64)
		if err != nil {
			continue
		}

		if !hasValue || value > maxValue {
			hasValue = true
			maxValue = value
			selected = offsetFile{path: candidatePath, size: size}
		}
	}

	if err := scanner.Err(); err != nil {
		return offsetFile{}, fmt.Errorf("scan hdfs ls output: %w", err)
	}
	if !hasValue {
		return offsetFile{}, fmt.Errorf("no numeric offset files found")
	}

	return selected, nil
}

func parseTopicOffsets(content []byte, topic string) (map[int32]int64, error) {
	raw, err := decodeOffsetsPayload(content)
	if err != nil {
		return nil, err
	}

	rawTopicOffsets, ok := raw[topic]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", topic)
	}

	offsets := make(map[int32]int64, len(rawTopicOffsets))
	for partitionText, rawOffset := range rawTopicOffsets {
		partitionID, err := strconv.ParseInt(partitionText, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("parse partition id %q: %w", partitionText, err)
		}

		offset, err := parseOffset(rawOffset)
		if err != nil {
			return nil, fmt.Errorf("parse committed offset for partition %q: %w", partitionText, err)
		}

		offsets[int32(partitionID)] = offset
	}

	return offsets, nil
}

func parseOffset(raw json.RawMessage) (int64, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		offset, convErr := strconv.ParseInt(text, 10, 64)
		if convErr != nil {
			return 0, fmt.Errorf("invalid numeric string %q: %w", text, convErr)
		}
		return offset, nil
	}

	var numeric int64
	if err := json.Unmarshal(raw, &numeric); err == nil {
		return numeric, nil
	}

	return 0, fmt.Errorf("unsupported offset value: %s", string(raw))
}

func validateOffsetFileSize(file offsetFile) error {
	if file.size >= maxOffsetFileSizeBytes {
		return fmt.Errorf("offset file %q is too large: %d bytes (must be < %d bytes)", file.path, file.size, maxOffsetFileSizeBytes)
	}
	return nil
}

func decodeOffsetsPayload(content []byte) (map[string]map[string]json.RawMessage, error) {
	raw := make(map[string]map[string]json.RawMessage)
	if err := json.Unmarshal(content, &raw); err == nil {
		return raw, nil
	}

	lines := strings.Split(strings.TrimSpace(string(content)), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			continue
		}

		candidate := make(map[string]map[string]json.RawMessage)
		if err := json.Unmarshal([]byte(line), &candidate); err == nil {
			return candidate, nil
		}
	}

	return nil, fmt.Errorf("unmarshal json: unable to decode offsets payload")
}
