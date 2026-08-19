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
	"os"

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
		GRPC string `yaml:"grpc"` // agent channel
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
	c.Listen.GRPC = "127.0.0.1:8081"
	c.Scheduler.TickInterval = "30s"
	c.AgeKeyPath = "/etc/groundplane/controller.age"
	c.Log.Level = "warn"
	c.Log.Console.Enabled = true
	c.Log.File.Enabled = true
	c.Log.File.Path = "/var/log/groundplane/controller.log"
	return c
}

// AgentConfig is /etc/groundplane/agent.yaml — the bootstrap SEED only
// (how to find the Controller, where to log). Runtime config (pull
// interval, max concurrent tasks, labels) is Controller-owned in etcd and
// served over the channel; `agent config set` writes to etcd, never here.
type AgentConfig struct {
	Controller struct {
		Address string `yaml:"address"` // Controller gRPC address to dial out to
	} `yaml:"controller"`
	JoinTokenPath string    `yaml:"join_token_path"`
	Log           LogConfig `yaml:"log"`
}

func DefaultAgentConfig() AgentConfig {
	var c AgentConfig
	c.Controller.Address = "127.0.0.1:8081"
	c.JoinTokenPath = "/etc/groundplane/agent.token"
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
func Load(ctx context.Context, path string, out interface{}) error {
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
	return nil
}

// Merge order everywhere in this project: built-in defaults < config file
// < env vars < flags. Callers apply env/flags themselves after Load,
// typically via pflag bindings in internal/cli/root.go and small
// per-field overrides in cmd/controller and cmd/agent's main().
