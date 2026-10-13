package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The start, observations and terminal event share one increasing delivery
// ordinal. Observations are informational Running events, not effect receipts.
func (p *WorkerPool) healthWaitEvents(
	ctx context.Context,
	start TaskProgress,
	base uint64,
) (func(*agentpb.HealthWaitProgress), *uint64) {
	ordinal := base
	return func(observation *agentpb.HealthWaitProgress) {
		ordinal++
		progress := start
		progress.Ordinal, progress.HealthWait = ordinal, observation
		p.emitProgress(ctx, progress)
	}, &ordinal
}
