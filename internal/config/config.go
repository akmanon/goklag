package config

import (
	"fmt"
	"runtime"
	"time"

	"github.com/spf13/viper"
)

type Config struct {
	Kafka  KafkaConfig  `mapstructure:"kafka"`
	Server ServerConfig `mapstructure:"server"`
}

type KafkaConfig struct {
	Brokers   []string          `mapstructure:"brokers"`
	Version   string            `mapstructure:"version"`
	SASL      SASLConfig        `mapstructure:"sasl"`
	Consumers []ConsumerBinding `mapstructure:"consumers"`
}

type SASLConfig struct {
	Mechanism string         `mapstructure:"mechanism"`
	Kerberos  KerberosConfig `mapstructure:"kerberos"`
}

type KerberosConfig struct {
	Realm       string `mapstructure:"realm"`
	ServiceName string `mapstructure:"service_name"`
	Username    string `mapstructure:"username"`
	KeytabPath  string `mapstructure:"keytab_path"`
}

type ConsumerBinding struct {
	Group  string   `mapstructure:"group"`
	Topics []string `mapstructure:"topics"`
}

type ServerConfig struct {
	Port                   int `mapstructure:"port"`
	ScrapeIntervalSeconds  int `mapstructure:"scrape_interval_seconds"`
	LagPollIntervalSeconds int `mapstructure:"lag_poll_interval_seconds"`
	WorkerPoolSize         int `mapstructure:"worker_pool_size"`
	RequestTimeoutSeconds  int `mapstructure:"request_timeout_seconds"`
}

func (c ServerConfig) ScrapeInterval() time.Duration {
	return time.Duration(c.ScrapeIntervalSeconds) * time.Second
}

func (c ServerConfig) RequestTimeout() time.Duration {
	return time.Duration(c.RequestTimeoutSeconds) * time.Second
}

func (c ServerConfig) LagPollInterval() time.Duration {
	return time.Duration(c.LagPollIntervalSeconds) * time.Second
}

func Load(path string) (*Config, error) {
	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")

	v.SetDefault("server.port", 9090)
	v.SetDefault("server.scrape_interval_seconds", 15)
	v.SetDefault("server.lag_poll_interval_seconds", 60)
	v.SetDefault("server.request_timeout_seconds", 10)
	v.SetDefault("server.worker_pool_size", defaultWorkerPoolSize())

	if err := v.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func (c *Config) Validate() error {
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("kafka.brokers is required")
	}
	if c.Kafka.Version == "" {
		return fmt.Errorf("kafka.version is required")
	}
	if c.Kafka.SASL.Mechanism == "" {
		return fmt.Errorf("kafka.sasl.mechanism is required")
	}
	if c.Kafka.SASL.Kerberos.Realm == "" {
		return fmt.Errorf("kafka.sasl.kerberos.realm is required")
	}
	if c.Kafka.SASL.Kerberos.ServiceName == "" {
		return fmt.Errorf("kafka.sasl.kerberos.service_name is required")
	}
	if c.Kafka.SASL.Kerberos.Username == "" {
		return fmt.Errorf("kafka.sasl.kerberos.username is required")
	}
	if c.Kafka.SASL.Kerberos.KeytabPath == "" {
		return fmt.Errorf("kafka.sasl.kerberos.keytab_path is required")
	}
	if len(c.Kafka.Consumers) == 0 {
		return fmt.Errorf("kafka.consumers is required")
	}
	for i, binding := range c.Kafka.Consumers {
		if binding.Group == "" {
			return fmt.Errorf("kafka.consumers[%d].group is required", i)
		}
		if len(binding.Topics) == 0 {
			return fmt.Errorf("kafka.consumers[%d].topics is required", i)
		}
	}
	if c.Server.Port <= 0 {
		return fmt.Errorf("server.port must be > 0")
	}
	if c.Server.ScrapeIntervalSeconds <= 0 {
		return fmt.Errorf("server.scrape_interval_seconds must be > 0")
	}
	if c.Server.WorkerPoolSize <= 0 {
		return fmt.Errorf("server.worker_pool_size must be > 0")
	}
	if c.Server.LagPollIntervalSeconds <= 0 {
		return fmt.Errorf("server.lag_poll_interval_seconds must be > 0")
	}
	if c.Server.RequestTimeoutSeconds <= 0 {
		return fmt.Errorf("server.request_timeout_seconds must be > 0")
	}

	return nil
}

func defaultWorkerPoolSize() int {
	workers := runtime.NumCPU() * 4
	if workers < 8 {
		return 8
	}
	if workers > 64 {
		return 64
	}
	return workers
}
