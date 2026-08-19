package app

import (
	"context"
	"log/slog"
	"os"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
)

const DefaultAgentConfigPath = "/etc/groundplane/agent.yaml"

// Agent is the wired Agent binary: config, logging, and the gRPC
// client. cmd/agent/main.go is nothing but NewAgent + Run.
type Agent struct {
	Config config.AgentConfig
	Logger *slog.Logger
	Client *agent.Client
}

func NewAgent(ctx context.Context, configPath string) (*Agent, error) {
	cfg := config.DefaultAgentConfig()
	if err := config.Load(ctx, configPath, &cfg); err != nil {
		return nil, err
	}

	logger, err := logging.Setup(logging.Options{
		Level:   logging.ResolveLevel(false, false, os.Getenv("GROUNDPLANE_LOG_LEVEL"), cfg.Log.Level),
		Console: logging.ConsoleConfig{Enabled: cfg.Log.Console.Enabled},
		File:    logging.FileConfig{Enabled: cfg.Log.File.Enabled, Path: cfg.Log.File.Path},
	})
	if err != nil {
		return nil, err
	}

	client := agent.NewClient(cfg.Controller.Address, logger)

	return &Agent{Config: cfg, Logger: logger, Client: client}, nil
}

// Run connects to the Controller (bootstrap join-token seed from
// JoinTokenPath) and runs the Agent's channel loop until ctx is
// cancelled.
func (a *Agent) Run(ctx context.Context) error {
	joinToken, _ := os.ReadFile(a.Config.JoinTokenPath)
	if err := a.Client.Connect(ctx, string(joinToken)); err != nil {
		a.Logger.Warn("agent: connect not wired yet, running worker pool standalone for scaffolding", slog.Any("error", err))
	}
	return a.Client.Run(ctx)
}
