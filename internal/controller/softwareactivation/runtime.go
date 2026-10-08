package softwareactivation

import (
	"context"
	"log/slog"
	"time"
)

// Run advances durable parent operations separately from the serial native Task
// worker. Waiting for a child never occupies the worker needed to execute it.
func (service *Service) Run(ctx context.Context, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		result, err := service.Tick(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			logger.Error("software activation tick failed", "error", err)
		}
		if err == nil && result.Progressed {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
