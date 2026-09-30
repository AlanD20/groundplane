package config

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// EtcdConfig contains tunable settings only. Node identity, loopback listeners,
// data directory, image and GP's transaction capacity are bootstrap-owned.
type EtcdConfig struct {
	QuotaBackendBytes       int64  `yaml:"quota-backend-bytes"`
	SnapshotCount           uint64 `yaml:"snapshot-count"`
	HeartbeatInterval       uint64 `yaml:"heartbeat-interval"`
	ElectionTimeout         uint64 `yaml:"election-timeout"`
	AutoCompactionRetention string `yaml:"auto-compaction-retention"`
	LogLevel                string `yaml:"log-level"`
}

func DefaultEtcdConfig() EtcdConfig {
	return EtcdConfig{
		QuotaBackendBytes:       2 << 30,
		SnapshotCount:           10000,
		HeartbeatInterval:       100,
		ElectionTimeout:         1000,
		AutoCompactionRetention: "0",
		LogLevel:                "info",
	}
}

func DefaultEtcdDocument() ([]byte, error) { return yaml.Marshal(DefaultEtcdConfig()) }

func ParseEtcdDocument(ctx context.Context, document []byte) (EtcdConfig, error) {
	if len(document) == 0 || len(document) > 8192 {
		return EtcdConfig{}, fmt.Errorf("config: etcd document must contain 1 through 8192 bytes")
	}
	cfg := DefaultEtcdConfig()
	if err := decodeDocument(ctx, "etcd configuration", bytes.NewReader(document), &cfg); err != nil {
		return EtcdConfig{}, err
	}
	if cfg.QuotaBackendBytes < 16<<20 || cfg.QuotaBackendBytes > 8<<30 || cfg.SnapshotCount < 100 ||
		cfg.SnapshotCount > 1000000 {
		return EtcdConfig{}, fmt.Errorf(
			"config: etcd quota must be 16 MiB through 8 GiB and snapshot-count 100 through 1000000",
		)
	}
	if cfg.HeartbeatInterval < 10 || cfg.HeartbeatInterval > 1000 || cfg.ElectionTimeout < 5*cfg.HeartbeatInterval ||
		cfg.ElectionTimeout > 50000 {
		return EtcdConfig{}, fmt.Errorf(
			"config: etcd heartbeat must be 10 through 1000 ms and election timeout at least five heartbeats, at most 50000 ms",
		)
	}
	if cfg.AutoCompactionRetention != "0" {
		duration, err := time.ParseDuration(cfg.AutoCompactionRetention)
		if err != nil || duration < time.Minute || duration > 365*24*time.Hour {
			return EtcdConfig{}, fmt.Errorf(
				"config: etcd compaction retention must be 0 or a duration between 1m and 8760h",
			)
		}
	}
	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return EtcdConfig{}, fmt.Errorf("config: etcd log-level must be debug, info, warn or error")
	}
	return cfg, nil
}

func ValidateEtcdDocument(ctx context.Context, document []byte) error {
	_, err := ParseEtcdDocument(ctx, document)
	return err
}
func ValidateControllerDocument(ctx context.Context, document []byte) error {
	_, err := ParseControllerDocument(ctx, document)
	return err
}

func (cfg EtcdConfig) Arguments() []string {
	defaults := DefaultEtcdConfig()
	result := []string{}
	if cfg.QuotaBackendBytes != defaults.QuotaBackendBytes {
		result = append(result, "--quota-backend-bytes="+strconv.FormatInt(cfg.QuotaBackendBytes, 10))
	}
	if cfg.SnapshotCount != defaults.SnapshotCount {
		result = append(result, "--snapshot-count="+strconv.FormatUint(cfg.SnapshotCount, 10))
	}
	if cfg.HeartbeatInterval != defaults.HeartbeatInterval {
		result = append(result, "--heartbeat-interval="+strconv.FormatUint(cfg.HeartbeatInterval, 10))
	}
	if cfg.ElectionTimeout != defaults.ElectionTimeout {
		result = append(result, "--election-timeout="+strconv.FormatUint(cfg.ElectionTimeout, 10))
	}
	if cfg.AutoCompactionRetention != defaults.AutoCompactionRetention {
		result = append(
			result,
			"--auto-compaction-mode=periodic",
			"--auto-compaction-retention="+cfg.AutoCompactionRetention,
		)
	}
	if cfg.LogLevel != defaults.LogLevel {
		result = append(result, "--log-level="+cfg.LogLevel)
	}
	return result
}
