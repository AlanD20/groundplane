package runtime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// localAgentWork keeps host maintenance on native execution ownership. Slots
// alone are insufficient: an interrupted operation can release capacity while
// retaining exact resource effects that must still be reconciled.
type localAgentWork struct {
	queue  *workqueue.Repository
	agents *agentregistration.Repository
}

func NewWork(queue *workqueue.Repository, agents *agentregistration.Repository) (*localAgentWork, error) {
	if queue == nil || agents == nil {
		return nil, errs.New(errs.KindInternal, "Agent maintenance requires native execution and host binding storage")
	}
	return &localAgentWork{queue: queue, agents: agents}, nil
}

func validateWorkBound(maximum int32) error {
	if maximum < 1 || maximum > 256 {
		return errs.New(errs.KindValidationFailed, "Agent maintenance concurrency is invalid")
	}
	return nil
}

func (owner *localAgentWork) RequireIdle(ctx context.Context, agentID string, generation uint64, maximum int32) error {
	if err := validateWorkBound(maximum); err != nil {
		return err
	}
	binding, err := owner.agents.MaintenanceBinding(ctx, agentID, generation)
	if err != nil {
		return err
	}
	idle, err := owner.queue.HostIdle(ctx, binding.HostID, binding.ReadRevision)
	if err != nil {
		return err
	}
	if !idle {
		return errs.New(errs.KindResourceInUse, "Agent host still has running work or unresolved recovery")
	}
	return nil
}

// AbortActive closes only forward admission, requests cancellation through each
// visible Task, then waits for operation-owned settlement. Recovery remains
// dispatchable. Neither timeout nor a missing connection authorizes removal.
func (owner *localAgentWork) AbortActive(
	ctx context.Context,
	agentID string,
	generation uint64,
	maximum int32,
	_ string,
) error {
	if err := validateWorkBound(maximum); err != nil {
		return err
	}
	binding, err := owner.agents.MaintenanceBinding(ctx, agentID, generation)
	if err != nil {
		return err
	}
	if err := owner.agents.PauseForward(ctx, binding); err != nil {
		return err
	}
	for {
		binding, err = owner.agents.MaintenanceBinding(ctx, agentID, generation)
		if err != nil {
			return err
		}
		after := ""
		for {
			page, err := owner.queue.HostActivity(ctx, binding.HostID, after, 64, binding.ReadRevision)
			if err != nil {
				return err
			}
			for _, work := range page.Work {
				if work.Task.Task.Stop != nil {
					continue
				}
				if err := workexecution.CancellationFailure(work.Task.Task); err != nil {
					return errs.Newf(
						errs.KindResourceInUse,
						"Agent removal must wait for protected Task %s to finish its operation or recovery",
						work.Task.Task.ID,
					)
				}
				at := time.Now().UTC()
				if at.Before(work.Task.Task.UpdatedAt) {
					at = work.Task.Task.UpdatedAt
				}
				stop, err := workqueue.PrepareStop(work.Task, workexecution.Cancelled, at)
				if err != nil {
					return err
				}
				result, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: binding.Conditions}, stop)
				if err != nil {
					return err
				}
				if !result.Succeeded {
					// Reload on the next outer observation. This never repeats a
					// command or assumes the competing completion released effects.
					break
				}
			}
			if !page.More {
				break
			}
			after = page.LastID
		}
		// A fresh binding revision observes cancellation settlements written
		// after the page snapshot, including cleanup without an occupied slot.
		binding, err = owner.agents.MaintenanceBinding(ctx, agentID, generation)
		if err != nil {
			return err
		}
		idle, err := owner.queue.HostIdle(ctx, binding.HostID, binding.ReadRevision)
		if err != nil {
			return err
		}
		if idle {
			return nil
		}
		timer := time.NewTimer(100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

var _ localagent.Tasks = (*localAgentWork)(nil)

func (owner *localAgentWork) ResumeForward(ctx context.Context, agentID string, generation uint64) error {
	return owner.agents.ResumeForward(ctx, agentID, generation)
}
