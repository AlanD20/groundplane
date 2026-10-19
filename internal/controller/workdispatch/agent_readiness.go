package workdispatch

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Ready enables only the current authenticated process. It inventories durable
// reservations before enabling admission. Unresolved older attempts retain both
// their slots and claims; readiness never asserts their effects have stopped.
// Free slots can serve unrelated work without adopting any older assignment.
func (delivery *AgentDeliveries) Ready(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	capacity uint16,
	stagingReady bool,
) ([]Issue, error) {
	if delivery == nil || ctx == nil || capacity == 0 || capacity > 256 {
		return nil, errs.New(errs.KindInternal, "Agent readiness requires configured capacity")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	identity := authorization.Identity()
	if identity.Executor != workexecution.Agent {
		return nil, errs.New(errs.KindStateConflict, "Agent readiness requires an Agent identity")
	}
	current, err := delivery.queue.ReadExecutor(
		ctx,
		workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID},
	)
	if err != nil {
		return nil, err
	}
	if current.Revision == 0 || current.Record.Identity != identity {
		return nil, errs.New(errs.KindStateConflict, "Agent process changed before readiness")
	}
	var unavailable []workexecution.Operation
	if !stagingReady {
		for _, operation := range current.Record.Operations {
			switch operation {
			case workexecution.Backup, workexecution.BackupPrune, workexecution.Restore:
				unavailable = append(unavailable, operation)
			}
		}
	}
	if current.Record.Ready && current.Record.Capacity == capacity &&
		slices.Equal(current.Record.UnavailableOperations, unavailable) {
		return nil, nil
	}
	// Registration and reconnect disable admission. A capacity change on an
	// already-ready process preserves the existing reservations without adopting
	// or resuming older work.
	var issues []Issue
	if !current.Record.Ready {
		interrupted, err := delivery.queue.InterruptedDeliveries(ctx, identity)
		if err != nil {
			return nil, err
		}
		for _, prior := range interrupted {
			attempt := prior.Attempt.Attempt
			if prior.Item.Item.Operation == workexecution.Resolver {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.resolvers.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.BackingProvision {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.backings.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.BackupPrune ||
				prior.Item.Item.Operation == workexecution.Backup ||
				prior.Item.Item.Operation == workexecution.Restore {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.backups.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}

			if prior.Item.Item.Operation == workexecution.Environment {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.environments.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Attach {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.attaches.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.EntryConfiguration {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.entries.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Component {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.components.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Network {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.networks.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.ServiceRemoval {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.removals.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.ServiceLifecycle {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.lifecycles.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Volume {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.volumes.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Detach {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.detaches.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Hierarchy {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.hierarchy.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Script {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.scripts.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.ReleaseRestoration {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.restorations.ReconcileInterrupted(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Release && attempt.Direction == workexecution.Recovery {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if err := delivery.releases.SettleInterruptedRecovery(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			if prior.Item.Item.Operation == workexecution.Release && attempt.Direction == workexecution.Forward {
				view, err := delivery.queue.ReadResume(
					ctx,
					attempt.TaskID,
					attempt.WorkItemID,
					attempt.ID,
					attempt.Executor,
				)
				if err != nil {
					return issues, err
				}
				if view.Attempt.Attempt.FinishedAt != nil {
					continue
				}
				if view.Checkpoint.Record == nil {
					if err := delivery.releases.RejectInterruptedBeforeEffects(ctx, view, authorization); err != nil {
						issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
					}
					continue
				}
				if err := delivery.releases.QueueInterruptedRecovery(ctx, view, authorization); err != nil {
					issues = append(issues, Issue{TaskID: attempt.TaskID, WorkID: attempt.WorkItemID, Err: err})
				}
				continue
			}
			issues = append(issues, Issue{TaskID: prior.Item.Item.TaskID, WorkID: prior.Item.Item.ID,
				Err: errs.New(
					errs.KindStateConflict,
					"previous Agent process has unresolved work; its execution slot and resource claims remain reserved",
				)})
		}
		if _, err := delivery.queue.Deliveries(ctx, identity); err != nil {
			return issues, err
		}
	}
	change, err := workqueue.PrepareAvailability(current, identity, true, capacity, unavailable)
	if err != nil {
		return issues, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := delivery.queue.Commit(ctx, workqueue.Change{Conditions: guards}, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return issues, err
	}
	if !result.Succeeded {
		return issues, errs.New(errs.KindStateConflict, "Agent ownership changed before readiness was saved")
	}
	return issues, nil
}
