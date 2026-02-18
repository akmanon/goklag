package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoad_DefaultLagPollInterval(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "config.yaml")

	content := `kafka:
  brokers:
    - broker1:9092
  version: 3.5.0
  sasl:
    mechanism: GSSAPI
    kerberos:
      realm: EXAMPLE.COM
      service_name: kafka
      username: user@EXAMPLE.COM
      keytab_path: /tmp/user.keytab
      kerberos_config_path: /etc/krb5.conf
  consumers:
    - group: group-1
      topics:
        - topic-a

server:
  port: 9090
  scrape_interval_seconds: 15
  worker_pool_size: 8
  request_timeout_seconds: 10
`

	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(cfgPath)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if got := cfg.Server.LagPollIntervalSeconds; got != 60 {
		t.Fatalf("expected default lag_poll_interval_seconds 60, got %d", got)
	}
	if got := cfg.Server.LagPollInterval(); got != time.Minute {
		t.Fatalf("expected lag poll duration 1m, got %s", got)
	}
}

func TestValidate_InvalidLagPollInterval(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Kafka: KafkaConfig{
			Brokers: []string{"broker1:9092"},
			Version: "3.5.0",
			SASL: SASLConfig{
				Mechanism: "GSSAPI",
				Kerberos: KerberosConfig{
					Realm:              "EXAMPLE.COM",
					ServiceName:        "kafka",
					Username:           "user@EXAMPLE.COM",
					KeytabPath:         "/tmp/user.keytab",
					KerberosConfigPath: "/etc/krb5.conf",
				},
			},
			Consumers: []ConsumerBinding{{Group: "group-1", Topics: []string{"topic-a"}}},
		},
		Server: ServerConfig{
			Port:                   9090,
			ScrapeIntervalSeconds:  15,
			LagPollIntervalSeconds: 0,
			WorkerPoolSize:         8,
			RequestTimeoutSeconds:  10,
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "server.lag_poll_interval_seconds") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidate_InvalidHDFSOffsetBinding(t *testing.T) {
	t.Parallel()
	cfg := &Config{
		Kafka: KafkaConfig{
			Brokers: []string{"broker1:9092"},
			Version: "3.5.0",
			SASL: SASLConfig{
				Mechanism: "GSSAPI",
				Kerberos: KerberosConfig{
					Realm:              "EXAMPLE.COM",
					ServiceName:        "kafka",
					Username:           "user@EXAMPLE.COM",
					KeytabPath:         "/tmp/user.keytab",
					KerberosConfigPath: "/etc/krb5.conf",
				},
			},
			Consumers: []ConsumerBinding{{Group: "group-1", Topics: []string{"topic-a"}}},
			HDFSOffset: []HDFSOffsetBinding{
				{Topic: "topic-a", Path: ""},
			},
		},
		Server: ServerConfig{
			Port:                   9090,
			ScrapeIntervalSeconds:  15,
			LagPollIntervalSeconds: 60,
			WorkerPoolSize:         8,
			RequestTimeoutSeconds:  10,
		},
	}

	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected validation error, got nil")
	}
	if !strings.Contains(err.Error(), "kafka.hdfs_offset[0].path") {
		t.Fatalf("unexpected error: %v", err)
	}
}
