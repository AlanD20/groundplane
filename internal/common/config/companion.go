package config

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

// LoadCompanion decodes optional release-specific settings beside an existing
// configuration file so an older binary can still read the primary file.
func LoadCompanion(ctx context.Context, primaryPath, feature string, out any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if primaryPath == "" || feature == "" || strings.Trim(feature, "abcdefghijklmnopqrstuvwxyz-") != "" {
		return fmt.Errorf("config: invalid companion identity")
	}
	path := primaryPath + "." + feature + ".yaml"
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("config: open %s: %w", path, err)
	}
	defer file.Close()
	const limit = 1 << 20
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return fmt.Errorf("config: read %s: %w", path, err)
	}
	if len(data) > limit {
		return fmt.Errorf("config: %s exceeds the companion configuration size limit", path)
	}
	return decodeDocument(ctx, path, bytes.NewReader(data), out)
}

type gitDeliveryListeners struct {
	ScopedRunnerHTTP     string `yaml:"scoped_runner_http"`
	ScopedRunnerRegistry string `yaml:"scoped_runner_registry"`
	GitHubWebhooks       string `yaml:"github_webhooks"`
	ScopedRunnerImage    string `yaml:"scoped_runner_image"`
}

func (c *ControllerConfig) loadGitDeliveryCompanion(ctx context.Context, path string) error {
	var companion gitDeliveryListeners
	if err := LoadCompanion(ctx, path, "github-delivery", &companion); err != nil {
		return err
	}
	c.Listen.ScopedRunnerHTTP = companion.ScopedRunnerHTTP
	c.Listen.ScopedRunnerRegistry = companion.ScopedRunnerRegistry
	c.Listen.GitHubWebhooks = companion.GitHubWebhooks
	c.Runner.ScopedImage = companion.ScopedRunnerImage
	return c.Validate()
}
