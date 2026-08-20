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
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
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
	Etcd struct {
		Endpoints []string `yaml:"endpoints"`
		KeyPrefix string   `yaml:"key_prefix"`
	} `yaml:"etcd"`
	Listen struct {
		HTTP string `yaml:"http"` // human API (REST/JSON)
	} `yaml:"listen"`
	Scheduler struct {
		TickInterval string `yaml:"tick_interval"` // e.g. "30s"
	} `yaml:"scheduler"`
	AgeKeyPath string    `yaml:"age_key_path"` // /etc/groundplane/controller.age
	Log        LogConfig `yaml:"log"`
}

func DefaultControllerConfig() ControllerConfig {
	var c ControllerConfig
	c.Etcd.Endpoints = []string{"127.0.0.1:2379"}
	c.Etcd.KeyPrefix = "/groundplane/"
	c.Listen.HTTP = "127.0.0.1:8080"
	c.Scheduler.TickInterval = "30s"
	c.AgeKeyPath = "/etc/groundplane/controller.age"
	c.Log.Level = "warn"
	c.Log.Console.Enabled = true
	c.Log.File.Enabled = true
	c.Log.File.Path = "/var/log/groundplane/controller.log"
	return c
}

// AgentConfig is the Controller-owned runtime document injected into the
// managed Agent container. Channel configuration arrives over the authenticated
// stream; this file contains only stable identity and logging configuration.
type AgentConfig struct {
	AgentID string    `yaml:"agent_id"`
	Log     LogConfig `yaml:"log"`
}

func DefaultAgentConfig() AgentConfig {
	var c AgentConfig
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
	if !filepath.IsAbs(c.AgeKeyPath) {
		return fmt.Errorf("config: controller age_key_path must be absolute")
	}
	return validateLog("controller", c.Log)
}

func (c AgentConfig) Validate() error {
	if err := ids.Validate(ids.KindAgent, c.AgentID); err != nil {
		return fmt.Errorf("config: agent agent_id must be a canonical agt-prefixed ULID: %w", err)
	}
	return validateLog("agent", c.Log)
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
