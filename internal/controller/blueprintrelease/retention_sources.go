package blueprintrelease

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// retireCapturedSources releases execution-only holds before the owner retires
// its work input. Resource-owned configurations and applied runtime stay intact.
// Each call performs at most one pending cleanup batch and preserves metadata
// needed to resume after that batch commits.
func (service *Service) retireCapturedSources(ctx context.Context, view workqueue.View, at time.Time,
) (etcd.ReleaseWorkSources, bool, workqueue.Change, error) {
	if service == nil || service.ledger == nil || service.scripts == nil {
		return etcd.ReleaseWorkSources{}, false, workqueue.Change{}, errs.New(
			errs.KindInternal,
			"Release retention is not configured",
		)
	}
	sources, err := service.ledger.LoadRetiringReleaseWorkInput(ctx, view)
	if err != nil {
		return sources, false, workqueue.Change{}, err
	}
	ready := false
	defer func() {
		if !ready {
			clear(sources.Rollout.Record.CandidateArtifact)
		}
	}()
	guards := workqueue.Change{Conditions: sources.Conditions}
	if sources.Input.Secrets != nil {
		retired, pins, err := service.ledger.RetireReleaseWorkSecretPins(ctx, view, *sources.Input.Secrets)
		if err != nil || !retired {
			return etcd.ReleaseWorkSources{}, false, workqueue.Change{}, err
		}
		guards.Conditions = append(guards.Conditions, pins.Conditions...)
	}
	for _, hook := range sources.Rollout.Record.Render.Hooks {
		retired, references, err := service.scripts.RetireReleaseHookSources(
			ctx,
			view,
			hook.ScriptExecutionID,
			hook.Input,
			at,
		)
		if err != nil || !retired {
			return etcd.ReleaseWorkSources{}, false, workqueue.Change{}, err
		}
		guards.Conditions = append(guards.Conditions, references.Conditions...)
	}
	ready = true
	return sources, true, guards, nil
}
