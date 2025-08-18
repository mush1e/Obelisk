package config

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"

	obeliskErrors "github.com/mush1e/obelisk/internal/errors"
)

type Config struct {
	Server  ServerConfig  `yaml:"server"`
	Storage StorageConfig `yaml:"storage"`
	Metrics MetricsConfig `yaml:"metrics"`
	Health  HealthConfig  `yaml:"health"`
}

type ServerConfig struct {
	TCPAddr        string        `yaml:"tcp_addr"`
	HTTPAddr       string        `yaml:"http_addr"`
	ReadTimeout    time.Duration `yaml:"read_timeout"`
	WriteTimeout   time.Duration `yaml:"write_timeout"`
	MaxConnections int           `yaml:"max_connections"`
}

type TopicConfig struct {
	Name       string `yaml:"name"`
	Partitions int    `yaml:"partitions"`
}

type StorageConfig struct {
	DataDir           string        `yaml:"data_dir"`
	BatchSize         uint32        `yaml:"batch_size"`
	FlushInterval     time.Duration `yaml:"flush_interval"`
	MaxMessageSize    uint32        `yaml:"max_message_size"`
	FilePoolTimeout   time.Duration `yaml:"file_pool_timeout"`
	CleanupInterval   time.Duration `yaml:"cleanup_interval"`
	DefaultPartitions int           `yaml:"default_partitions"`
	Topics            []TopicConfig `yaml:"topics"`
}

type MetricsConfig struct {
	Enabled    bool   `yaml:"enabled"`
	Path       string `yaml:"path"`
	BufferSize int    `yaml:"buffer_size"`
}

type HealthConfig struct {
	CheckInterval    time.Duration `yaml:"check_interval"`
	SuccessThreshold float64       `yaml:"success_threshold"`
	HistorySize      int           `yaml:"history_size"`
}

func LoadConfig(path string) (*Config, error) {
	config := &Config{}

	// Set defaults first
	setDefaults(config)

	// Load from file if provided
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, obeliskErrors.NewConfigurationError("load_config",
					"config file not found", err)
			}
			return nil, obeliskErrors.NewConfigurationError("load_config",
				"failed to read config file", err)
		}

		if err := yaml.Unmarshal(data, config); err != nil {
			return nil, obeliskErrors.NewConfigurationError("parse_config",
				"invalid YAML format in config file", err)
		}
	}

	// Override with environment variables
	overrideFromEnv(config)

	return config, nil
}

func setDefaults(config *Config) {
	// Server defaults
	config.Server.TCPAddr = ":8080"
	config.Server.HTTPAddr = ":8081"
	config.Server.ReadTimeout = 30 * time.Second
	config.Server.WriteTimeout = 30 * time.Second
	config.Server.MaxConnections = 1000

	// Storage defaults
	config.Storage.DataDir = "data/topics"
	config.Storage.BatchSize = 100
	config.Storage.FlushInterval = 5 * time.Second
	config.Storage.MaxMessageSize = 10 * 1024 * 1024 // 10MB
	config.Storage.FilePoolTimeout = time.Hour
	config.Storage.CleanupInterval = 2 * time.Minute
	config.Storage.DefaultPartitions = 4

	// Metrics defaults
	config.Metrics.Enabled = true
	config.Metrics.Path = "/metrics"
	config.Metrics.BufferSize = 1000

	// Health defaults
	config.Health.CheckInterval = 30 * time.Second
	config.Health.SuccessThreshold = 0.95
	config.Health.HistorySize = 100
}

func overrideFromEnv(config *Config) {
	if addr := os.Getenv("OBELISK_TCP_ADDR"); addr != "" {
		config.Server.TCPAddr = addr
	}
	if addr := os.Getenv("OBELISK_HTTP_ADDR"); addr != "" {
		config.Server.HTTPAddr = addr
	}
	if dir := os.Getenv("OBELISK_DATA_DIR"); dir != "" {
		config.Storage.DataDir = dir
	}
	if enabled := os.Getenv("OBELISK_METRICS_ENABLED"); enabled == "false" {
		config.Metrics.Enabled = false
	}
}

func (c *Config) Validate() error {
	if c.Storage.BatchSize == 0 {
		return obeliskErrors.NewConfigurationError("validate_config",
			"batch_size must be greater than 0", nil)
	}
	if c.Storage.FlushInterval <= 0 {
		return obeliskErrors.NewConfigurationError("validate_config",
			"flush_interval must be positive", nil)
	}
	if c.Health.SuccessThreshold < 0 || c.Health.SuccessThreshold > 1 {
		return obeliskErrors.NewConfigurationError("validate_config",
			"success_threshold must be between 0 and 1", nil)
	}
	if c.Server.MaxConnections <= 0 {
		return obeliskErrors.NewConfigurationError("validate_config",
			"max_connections must be greater than 0", nil)
	}
	if c.Storage.MaxMessageSize == 0 || c.Storage.MaxMessageSize > 100*1024*1024 {
		return obeliskErrors.NewConfigurationError("validate_config",
			"max_message_size must be between 1 byte and 100MB", nil)
	}
	return nil
}

// GetTopicPartitions returns partition count for a topic
func (c *Config) GetTopicPartitions(topicName string) int {
	// Check if topic has specific config
	for _, topic := range c.Storage.Topics {
		if topic.Name == topicName {
			return topic.Partitions
		}
	}

	// Fall back to default
	return c.Storage.DefaultPartitions
}

// SetTopicPartitions sets partition count for a topic
func (c *Config) SetTopicPartitions(topicName string, partitions int) {
	// Update existing topic config
	for i := range c.Storage.Topics {
		if c.Storage.Topics[i].Name == topicName {
			c.Storage.Topics[i].Partitions = partitions
			return
		}
	}

	// Add new topic config
	c.Storage.Topics = append(c.Storage.Topics, TopicConfig{
		Name:       topicName,
		Partitions: partitions,
	})
}

// GetConfigExample returns an example YAML configuration
func GetConfigExample() string {
	return `# Obelisk Message Broker Configuration

server:
  tcp_addr: ":8080"          # TCP server for message ingestion
  http_addr: ":8081"         # HTTP server for REST API and metrics
  read_timeout: "30s"        # Connection read timeout
  write_timeout: "30s"       # Connection write timeout
  max_connections: 1000      # Maximum concurrent connections

storage:
  data_dir: "data/topics"    # Directory for topic storage
  batch_size: 100            # Messages per batch before flush
  flush_interval: "5s"       # Maximum time between flushes
  max_message_size: 10485760 # 10MB maximum message size
  file_pool_timeout: "1h"    # File handle timeout
  cleanup_interval: "2m"     # File cleanup check interval

metrics:
  enabled: true              # Enable Prometheus metrics
  path: "/metrics"           # Metrics endpoint path
  buffer_size: 1000          # Metrics buffer size

health:
  check_interval: "30s"      # Health check frequency
  success_threshold: 0.95    # Required success rate (95%)
  history_size: 100          # Number of operations to track
`
}
