package config

import (
	"fmt"
	"time"
)

// MaintenanceConfig controls disposable diagnostics and bounded storage work.
// Recovery and request-replay evidence retain their independent safety guards.
type MaintenanceConfig struct {
	DiagnosticRetention  string `yaml:"diagnostic_retention"`
	CleanupInterval      string `yaml:"cleanup_interval"`
	CleanupBatchSize     int    `yaml:"cleanup_batch_size"`
	CleanupTimeBudget    string `yaml:"cleanup_time_budget"`
	HistoryRetention     string `yaml:"history_retention"`
	DefragMinimumBytes   int64  `yaml:"defrag_minimum_bytes"`
	DefragMinimumPercent int    `yaml:"defrag_minimum_percent"`
	DefragStartUTC       string `yaml:"defrag_start_utc"`
	DefragWindow         string `yaml:"defrag_window"`
}

type MaintenancePolicy struct {
	DiagnosticRetention  time.Duration
	CleanupInterval      time.Duration
	CleanupBatchSize     int
	CleanupTimeBudget    time.Duration
	HistoryRetention     time.Duration
	DefragMinimumBytes   int64
	DefragMinimumPercent int
	DefragStartMinuteUTC int
	DefragWindow         time.Duration
}

func DefaultMaintenanceConfig() MaintenanceConfig {
	policy := DefaultMaintenancePolicy()
	return MaintenanceConfig{
		DiagnosticRetention: policy.DiagnosticRetention.String(), CleanupInterval: policy.CleanupInterval.String(),
		CleanupBatchSize: policy.CleanupBatchSize, CleanupTimeBudget: policy.CleanupTimeBudget.String(),
		HistoryRetention: policy.HistoryRetention.String(), DefragMinimumBytes: policy.DefragMinimumBytes,
		DefragMinimumPercent: policy.DefragMinimumPercent,
		DefragStartUTC:       fmt.Sprintf("%02d:%02d", policy.DefragStartMinuteUTC/60, policy.DefragStartMinuteUTC%60),
		DefragWindow:         policy.DefragWindow.String(),
	}
}

func DefaultMaintenancePolicy() MaintenancePolicy {
	return MaintenancePolicy{DiagnosticRetention: 90 * 24 * time.Hour,
		CleanupInterval: time.Minute, CleanupBatchSize: 32, CleanupTimeBudget: 2 * time.Second,
		HistoryRetention: time.Hour, DefragMinimumBytes: 64 << 20, DefragMinimumPercent: 25,
		DefragStartMinuteUTC: 3 * 60, DefragWindow: 15 * time.Minute}
}

func (cfg MaintenanceConfig) Policy() (MaintenancePolicy, error) {
	var policy MaintenancePolicy
	values := []struct {
		name             string
		raw              string
		minimum, maximum time.Duration
		target           *time.Duration
	}{
		{"diagnostic_retention", cfg.DiagnosticRetention, time.Hour, 90 * 24 * time.Hour, &policy.DiagnosticRetention},
		{"cleanup_interval", cfg.CleanupInterval, 10 * time.Second, time.Hour, &policy.CleanupInterval},
		{
			"cleanup_time_budget",
			cfg.CleanupTimeBudget,
			100 * time.Millisecond,
			10 * time.Second,
			&policy.CleanupTimeBudget,
		},
		{"history_retention", cfg.HistoryRetention, 10 * time.Minute, 7 * 24 * time.Hour, &policy.HistoryRetention},
		{"defrag_window", cfg.DefragWindow, time.Minute, time.Hour, &policy.DefragWindow},
	}
	for _, value := range values {
		duration, err := time.ParseDuration(value.raw)
		if err != nil || duration < value.minimum || duration > value.maximum {
			return MaintenancePolicy{}, fmt.Errorf(
				"config: maintenance.%s must be between %s and %s",
				value.name,
				value.minimum,
				value.maximum,
			)
		}
		*value.target = duration
	}
	start, err := time.Parse("15:04", cfg.DefragStartUTC)
	if err != nil || start.Format("15:04") != cfg.DefragStartUTC {
		return MaintenancePolicy{}, fmt.Errorf("config: maintenance.defrag_start_utc must be HH:MM in UTC")
	}
	if cfg.CleanupBatchSize < 1 || cfg.CleanupBatchSize > 128 || policy.CleanupTimeBudget >= policy.CleanupInterval ||
		cfg.DefragMinimumBytes < 16<<20 || cfg.DefragMinimumBytes > 8<<30 ||
		cfg.DefragMinimumPercent < 10 || cfg.DefragMinimumPercent > 90 {
		return MaintenancePolicy{}, fmt.Errorf("config: maintenance cleanup or defragmentation limits are invalid")
	}
	policy.CleanupBatchSize = cfg.CleanupBatchSize
	policy.DefragMinimumBytes, policy.DefragMinimumPercent = cfg.DefragMinimumBytes, cfg.DefragMinimumPercent
	policy.DefragStartMinuteUTC = start.Hour()*60 + start.Minute()
	return policy, nil
}
