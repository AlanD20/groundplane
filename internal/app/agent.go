package app

import (
	"context"
	"encoding/base64"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"syscall"

	"github.com/AlanD20/groundplane/internal/agent"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/logging"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const DefaultAgentConfigPath = agentprotocol.RuntimeConfigPath

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
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	level, err := logging.ResolveLevel(false, false, os.Getenv("GROUNDPLANE_LOG_LEVEL"), cfg.Log.Level)
	if err != nil {
		return nil, err
	}
	logger, err := logging.Setup(logging.Options{
		Level:   level,
		Console: logging.ConsoleConfig{Enabled: cfg.Log.Console.Enabled},
		File:    logging.FileConfig{Enabled: cfg.Log.File.Enabled, Path: cfg.Log.File.Path},
	})
	if err != nil {
		return nil, err
	}

	token, err := readAgentToken(ctx, agentprotocol.TokenPath)
	if err != nil {
		return nil, err
	}
	defer clear(token)
	client, err := agent.NewClient(agentprotocol.SocketPath, cfg.AgentID, token, logger)
	if err != nil {
		return nil, err
	}

	return &Agent{Config: cfg, Logger: logger, Client: client}, nil
}

func (a *Agent) Run(ctx context.Context) error {
	return a.Client.Run(ctx)
}

func readAgentToken(ctx context.Context, tokenPath string) ([]byte, error) {
	return readAgentTokenForUID(ctx, tokenPath, 0)
}

func readAgentTokenForUID(ctx context.Context, tokenPath string, expectedUID uint32) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !filepath.IsAbs(tokenPath) || filepath.Clean(tokenPath) != tokenPath {
		return nil, errs.New(errs.CodeValidationFailed, "agent: invalid channel token path")
	}
	directory, err := os.OpenRoot(filepath.Dir(tokenPath))
	if err != nil {
		return nil, errs.New(errs.CodeInternal, "agent: read channel token")
	}
	defer directory.Close()
	name := filepath.Base(tokenPath)
	entry, err := directory.Lstat(name)
	if err != nil || !secureTokenFile(entry, expectedUID) {
		return nil, errs.New(errs.CodeValidationFailed, "agent: invalid channel token file")
	}
	file, err := directory.Open(name)
	if err != nil {
		return nil, errs.New(errs.CodeInternal, "agent: read channel token")
	}
	opened, err := file.Stat()
	if err != nil || !os.SameFile(entry, opened) || !secureTokenFile(opened, expectedUID) {
		// Best effort: preserve the validation failure if closing also fails.
		_ = file.Close()
		return nil, errs.New(errs.CodeValidationFailed, "agent: invalid channel token file")
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, agentprotocol.EncodedTokenBytes+1))
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		clear(encoded)
		return nil, errs.New(errs.CodeInternal, "agent: read channel token")
	}
	defer clear(encoded)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(encoded) != agentprotocol.EncodedTokenBytes {
		return nil, errs.New(errs.CodeValidationFailed, "agent: channel token has invalid encoding")
	}
	token := make([]byte, agentprotocol.RawTokenBytes)
	written, err := base64.RawURLEncoding.Strict().Decode(token, encoded)
	if err != nil || written != agentprotocol.RawTokenBytes {
		clear(token)
		return nil, errs.New(errs.CodeValidationFailed, "agent: channel token has invalid encoding")
	}
	return token, nil
}

func secureTokenFile(info os.FileInfo, expectedUID uint32) bool {
	if info == nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o400 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == expectedUID
}
