// Package config is the ONE config reader used by the Controller, the
// Agent, and the CLI — YAML in, validated struct out, defaults first,
// strict key checking (unknown keys are rejected, same as the Blueprint
// schema rule). Nothing outside this package parses config. See
// architecture.md, "Configuration (locked: one reader for controller +
// agent)".
package config

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"gopkg.in/yaml.v3"
)

// LogConfig is embedded in every daemon config — both destinations are
// configurable, never hard-coded (see internal/common/logging).
type LogConfig struct {
	Level   string `yaml:"level"`
	Console struct {
		Enabled bool `yaml:"enabled"`
	} `yaml:"console"`
	File struct {
		Enabled bool   `yaml:"enabled"`
		Path    string `yaml:"path"`
	} `yaml:"file"`
}

// ControllerConfig is /etc/groundplane/controller.yaml — the "minimal
// startup config" of the DR contract: the only two things etcd cannot
// hold (this file + the controller age key) are exported with the DR
// bundle. See mvp.md, "Everything is etcd (locked)".
type ControllerConfig struct {
	EnvironmentPool string `yaml:"environment_pool"`
	SystemPool      string `yaml:"system_pool"`
	Etcd            struct {
		Endpoints []string `yaml:"endpoints"`
		KeyPrefix string   `yaml:"key_prefix"`
	} `yaml:"etcd"`
	Listen struct {
		HTTP string `yaml:"http"` // human API (REST/JSON)
	} `yaml:"listen"`
	Scheduler struct {
		TickInterval string `yaml:"tick_interval"` // e.g. "30s"
	} `yaml:"scheduler"`
	Storage struct {
		VolumeRoot string `yaml:"volume_root"`
	} `yaml:"storage"`
	Agent struct {
		Image   string             `yaml:"image"`
		Runtime AgentRuntimeConfig `yaml:"runtime"`
	} `yaml:"agent"`
	Runner struct {
		NetworkPool  string `yaml:"network_pool"`
		HostUIDRange string `yaml:"host_uid_range"`
		SubUIDRange  string `yaml:"subuid_range"`
		SubGIDRange  string `yaml:"subgid_range"`
	} `yaml:"runner"`
	AgeKeyPath string    `yaml:"age_key_path"` // /etc/groundplane/controller.age
	Log        LogConfig `yaml:"log"`
}

// AllocationPools is the canonical machine IPAM boundary consumed by
// Controller application services after config validation.
type AllocationPools struct {
	Environment netip.Prefix
	System      netip.Prefix
	Runner      RunnerAllocationPools
}

type InclusiveRange struct {
	First uint32
	Last  uint32
}

type RunnerAllocationPools struct {
	Network netip.Prefix
	HostUID InclusiveRange
	SubUID  InclusiveRange
	SubGID  InclusiveRange
}

func DefaultControllerConfig() ControllerConfig {
	var c ControllerConfig
	c.Etcd.Endpoints = []string{"127.0.0.1:2379"}
	c.Etcd.KeyPrefix = "/groundplane/"
	c.Listen.HTTP = "127.0.0.1:8080"
	c.Scheduler.TickInterval = "30s"
	c.Storage.VolumeRoot = environmentpath.DefaultVolumeRoot
	c.Agent.Runtime.PullIntervalSeconds = 2
	c.Agent.Runtime.MaxConcurrentTasks = 3
	c.Agent.Runtime.Labels = map[string]string{}
	c.AgeKeyPath = "/etc/groundplane/controller.age"
	c.Log.Level = "warn"
	c.Log.Console.Enabled = true
	c.Log.File.Enabled = true
	c.Log.File.Path = "/var/log/groundplane/controller.log"
	return c
}

// AgentRuntimeConfig is the durable execution policy in the Controller-owned
// Agent runtime document. The Controller sends the same values over the
// authenticated stream so reconnects and runtime materialization share one
// contract.
type AgentRuntimeConfig struct {
	PullIntervalSeconds int32             `yaml:"pull_interval_seconds"`
	MaxConcurrentTasks  int32             `yaml:"max_concurrent_tasks"`
	Labels              map[string]string `yaml:"labels"`
}

// AgentConfig is the complete Controller-owned runtime document injected into
// the managed Agent container.
type AgentConfig struct {
	AgentID string             `yaml:"agent_id"`
	Log     LogConfig          `yaml:"log"`
	Runtime AgentRuntimeConfig `yaml:"runtime"`
	Storage struct {
		VolumeRoot string `yaml:"volume_root"`
	} `yaml:"storage"`
}

func DefaultAgentConfig() AgentConfig {
	var c AgentConfig
	c.Storage.VolumeRoot = environmentpath.DefaultVolumeRoot
	c.Log.Level = "warn"
	c.Log.Console.Enabled = true
	c.Log.File.Enabled = true
	c.Log.File.Path = "/var/log/groundplane/agent.log"
	return c
}

// CLIConfig is ~/.config/groundplane/config.yaml — the CLI's global
// config (the -c/--config default from api-cli.md).
type CLIConfig struct {
	Host        string `yaml:"host"`
	Tenant      string `yaml:"tenant"`
	Project     string `yaml:"project"`
	Environment string `yaml:"env"`
	Output      string `yaml:"output"`
	NoColor     bool   `yaml:"no_color"`
}

func DefaultCLIConfig() CLIConfig {
	return CLIConfig{
		Host:   "http://127.0.0.1:8080",
		Output: "TABLE",
	}
}

func (c ControllerConfig) Validate() error {
	if _, err := c.AllocationPools(); err != nil {
		return err
	}
	if len(c.Etcd.Endpoints) == 0 {
		return fmt.Errorf("config: controller etcd.endpoints must contain at least one endpoint")
	}
	for i, endpoint := range c.Etcd.Endpoints {
		if strings.TrimSpace(endpoint) == "" {
			return fmt.Errorf("config: controller etcd.endpoints[%d] is empty", i)
		}
	}
	if !strings.HasPrefix(c.Etcd.KeyPrefix, "/") || !strings.HasSuffix(c.Etcd.KeyPrefix, "/") {
		return fmt.Errorf("config: controller etcd.key_prefix must begin and end with /")
	}
	if err := validateHumanHTTPAddress("controller listen.http", c.Listen.HTTP); err != nil {
		return err
	}
	tick, err := time.ParseDuration(c.Scheduler.TickInterval)
	if err != nil {
		return fmt.Errorf("config: controller scheduler.tick_interval: %w", err)
	}
	if tick <= 0 {
		return fmt.Errorf("config: controller scheduler.tick_interval must be positive")
	}
	if err := environmentpath.ValidateRoot(c.Storage.VolumeRoot); err != nil {
		return fmt.Errorf("config: controller storage.volume_root: %w", err)
	}
	if c.Agent.Image != "" && !imageref.IsDigestPinned(c.Agent.Image) {
		return fmt.Errorf("config: controller agent.image must be empty or a digest-pinned OCI reference")
	}
	if err := validateAgentRuntime("controller agent.runtime", c.Agent.Runtime); err != nil {
		return err
	}
	if !filepath.IsAbs(c.AgeKeyPath) {
		return fmt.Errorf("config: controller age_key_path must be absolute")
	}
	return validateLog("controller", c.Log)
}

// AllocationPools parses and validates the required machine allocation roots.
func (c ControllerConfig) AllocationPools() (AllocationPools, error) {
	environmentPool, err := ipam.ParseIPv4Prefix(c.EnvironmentPool)
	if err != nil {
		return AllocationPools{}, fmt.Errorf("config: controller environment_pool: %w", err)
	}
	systemPool, err := ipam.ParseIPv4Prefix(c.SystemPool)
	if err != nil {
		return AllocationPools{}, fmt.Errorf("config: controller system_pool: %w", err)
	}
	if err := ipam.ValidateRootPair(environmentPool, systemPool); err != nil {
		return AllocationPools{}, fmt.Errorf("config: controller allocation pools: %w", err)
	}
	runnerPool, err := ipam.ParseIPv4Prefix(c.Runner.NetworkPool)
	if err != nil || runnerPool.String() != c.Runner.NetworkPool || runnerPool.Bits() <= systemPool.Bits() ||
		runnerPool.Bits() > 29 || !systemPool.Contains(runnerPool.Addr()) {
		return AllocationPools{}, fmt.Errorf("config: controller runner.network_pool must be a canonical IPv4 child of system_pool with /29 children")
	}
	hostUID, err := parseInclusiveRange("runner.host_uid_range", c.Runner.HostUIDRange)
	if err != nil {
		return AllocationPools{}, err
	}
	subUID, err := parseInclusiveRange("runner.subuid_range", c.Runner.SubUIDRange)
	if err != nil {
		return AllocationPools{}, err
	}
	subGID, err := parseInclusiveRange("runner.subgid_range", c.Runner.SubGIDRange)
	if err != nil {
		return AllocationPools{}, err
	}
	hostCount := uint64(hostUID.Last) - uint64(hostUID.First) + 1
	subUIDCount := uint64(subUID.Last) - uint64(subUID.First) + 1
	subGIDCount := uint64(subGID.Last) - uint64(subGID.First) + 1
	networkCount := uint64(1) << uint(29-runnerPool.Bits())
	if hostCount < 5 || hostCount > math.MaxUint32 || subUIDCount != hostCount*65536 ||
		subGIDCount != hostCount*65536 || networkCount < hostCount {
		return AllocationPools{}, fmt.Errorf("config: controller runner allocation ranges must define at least five matching fixed-size slots")
	}
	return AllocationPools{
		Environment: environmentPool,
		System:      systemPool,
		Runner:      RunnerAllocationPools{Network: runnerPool, HostUID: hostUID, SubUID: subUID, SubGID: subGID},
	}, nil
}

func parseInclusiveRange(label string, value string) (InclusiveRange, error) {
	parts := strings.Split(value, "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return InclusiveRange{}, fmt.Errorf("config: controller %s must be first-last", label)
	}
	first, firstErr := strconv.ParseUint(parts[0], 10, 32)
	last, lastErr := strconv.ParseUint(parts[1], 10, 32)
	if firstErr != nil || lastErr != nil || strconv.FormatUint(first, 10) != parts[0] ||
		strconv.FormatUint(last, 10) != parts[1] || last < first {
		return InclusiveRange{}, fmt.Errorf("config: controller %s must be a canonical unsigned inclusive range", label)
	}
	return InclusiveRange{First: uint32(first), Last: uint32(last)}, nil
}

func (c AgentConfig) Validate() error {
	if err := ids.Validate(ids.KindAgent, c.AgentID); err != nil {
		return fmt.Errorf("config: agent agent_id must be a canonical agt-prefixed ULID: %w", err)
	}
	if err := validateAgentRuntime("agent runtime", c.Runtime); err != nil {
		return err
	}
	if err := environmentpath.ValidateRoot(c.Storage.VolumeRoot); err != nil {
		return fmt.Errorf("config: agent storage.volume_root: %w", err)
	}
	return validateLog("agent", c.Log)
}

func validateAgentRuntime(label string, runtime AgentRuntimeConfig) error {
	if runtime.PullIntervalSeconds <= 0 {
		return fmt.Errorf("config: %s.pull_interval_seconds must be positive", label)
	}
	if runtime.MaxConcurrentTasks <= 0 {
		return fmt.Errorf("config: %s.max_concurrent_tasks must be positive", label)
	}
	for key, value := range runtime.Labels {
		if !utf8.ValidString(key) || strings.IndexByte(key, 0) >= 0 ||
			!utf8.ValidString(value) || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("config: %s.labels must contain valid NUL-free UTF-8", label)
		}
	}
	return nil
}

func (c CLIConfig) Validate() error {
	host, err := url.Parse(c.Host)
	if err != nil {
		return fmt.Errorf("config: cli host: %w", err)
	}
	if (host.Scheme != "http" && host.Scheme != "https") || host.Host == "" {
		return fmt.Errorf("config: cli host must be an absolute http or https URL")
	}
	if host.User != nil || (host.Path != "" && host.Path != "/") || host.RawQuery != "" || host.Fragment != "" {
		return fmt.Errorf("config: cli host must contain only scheme and authority")
	}
	switch strings.ToUpper(strings.TrimSpace(c.Output)) {
	case "TABLE", "JSON", "YAML":
		return nil
	default:
		return fmt.Errorf("config: cli output must be TABLE, JSON, or YAML")
	}
}

func validateHumanHTTPAddress(label, address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return fmt.Errorf("config: %s must be host:port: %w", label, err)
	}
	if host != "127.0.0.1" {
		return fmt.Errorf("config: %s host must be exactly 127.0.0.1", label)
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return fmt.Errorf("config: %s port must be an integer from 1 through 65535", label)
	}
	return nil
}

func validateLog(label string, c LogConfig) error {
	switch strings.ToUpper(strings.TrimSpace(c.Level)) {
	case "DEBUG", "INFO", "WARN", "WARNING", "ERROR":
	default:
		return fmt.Errorf("config: %s log.level must be DEBUG, INFO, WARN, or ERROR", label)
	}
	if c.File.Enabled && !filepath.IsAbs(c.File.Path) {
		return fmt.Errorf("config: %s log.file.path must be absolute when file logging is enabled", label)
	}
	return nil
}

// Load reads path into out, which must already hold the applicable
// defaults (DefaultControllerConfig(), etc.) — Load only overlays what
// the file specifies. Unknown keys are a hard error (strict key
// checking, same rule the Blueprint schema uses). A missing file is not
// an error: defaults stand, same as mvp.md's DR story implies (a fresh
// host needs only the two bootstrap files, not a fully populated one).
//
// ctx is accepted (and currently unused) per docs/standards.md,
// section 4: every function with side effects (this one does file I/O)
// takes ctx first, no exceptions — so a future context-aware file read
// (e.g. reading config from a mounted secret store with a timeout) never
// needs a signature change at every call site.
func Load(ctx context.Context, path string, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("config: open %s: %w", path, err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true) // strict: unknown keys are rejected at write time
	if err := dec.Decode(out); err != nil {
		if err == io.EOF {
			return nil // an empty file is absence, same as a missing one — defaults stand
		}
		return fmt.Errorf("config: %s: %w", path, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var trailing any
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("config: %s: multiple YAML documents are not allowed", path)
		}
		return fmt.Errorf("config: %s: trailing document: %w", path, err)
	}
	return nil
}

// Merge order everywhere in this project: built-in defaults < config file
// < env vars < flags. Callers apply env/flags themselves after Load,
// typically via pflag bindings in internal/cli/root.go and small
// per-field overrides in cmd/controller and cmd/agent's main().
