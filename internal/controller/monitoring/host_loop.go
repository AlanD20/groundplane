package monitoring

import (
	"context"
	"log/slog"
	"time"

	metrics "github.com/AlanD20/groundplane/internal/common/monitoring"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type HostHistory interface {
	AppendHost(context.Context, metrics.HostMeasurement) error
}

// Run shares one collector across viewers. History retains the latest accepted
// reading in each closed 15-second bucket. Missed buckets stay absent; Console
// reads never insert a synthetic sample or refresh the observation timestamp.
func (sampler *HostSampler) Run(ctx context.Context, history HostHistory, logger *slog.Logger) error {
	if ctx == nil || sampler == nil || history == nil || logger == nil {
		return errs.New(errs.KindInternal, "host monitoring loop dependencies are required")
	}
	ticker := time.NewTicker(LiveInterval)
	defer ticker.Stop()
	var pending *HostSample
	for {
		read, cancel := context.WithTimeout(ctx, 4*time.Second)
		err := sampler.Collect(read)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			logger.DebugContext(ctx, "host monitoring sample unavailable", slog.Any("error", err))
		} else if current, found := sampler.Current(); found {
			if pending != nil && !pending.Observed.At.Truncate(metrics.SampleInterval).Equal(current.Observed.At.Truncate(metrics.SampleInterval)) {
				write, finish := context.WithTimeout(ctx, 2*time.Second)
				err := history.AppendHost(write, metrics.HostMeasurement{HostID: sampler.hostID, Point: pending.Point, Available: true})
				finish()
				if err != nil {
					logger.DebugContext(ctx, "host monitoring history unavailable", slog.Any("error", err))
				}
			}
			pending = &current
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
