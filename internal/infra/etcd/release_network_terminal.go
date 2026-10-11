package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func releaseNetworkEffectsSettled(task TaskRecord) bool {
	return taskjournal.IsTerminalTaskStatus(task.Status) &&
		(task.StartedAt == nil || task.Result != nil && !task.Result.ReconciliationRequired)
}

func (repository *TaskRepository) finishPendingReleaseNetwork(ctx context.Context,
	current keyvalue.Versioned[TaskRecord], at time.Time,
) (bool, error) {
	dependency, applies, err := repository.readReleaseNetworkDependency(ctx, current.Record, current.ReadRevision)
	if err != nil || !applies {
		return false, err
	}
	if taskjournal.IsReleaseNetworkChild(current.Record.Params) {
		if dependency.parent.Record.Status != taskjournal.TaskStatusPending ||
			!TaskForwardDeadline(current.Record, at).After(at) {
			_, err := repository.AbortPendingTask(ctx, current.Record.ID, at)
			return err == nil, err
		}
		return false, nil
	}
	timedOut := !at.Before(current.Record.CreatedAt.Add(time.Duration(current.Record.TimeoutSeconds) * time.Second))
	child := dependency.child.Record
	if timedOut && child.Status == taskjournal.TaskStatusPending {
		_, err := repository.AbortPendingTask(ctx, child.ID, at)
		return err == nil, err
	}
	if !releaseNetworkEffectsSettled(child) {
		return false, nil // Do not discard an assigned or uncertain prerequisite to satisfy a timeout.
	}
	status := taskjournal.TaskStatusFailed
	diagnostic := taskjournal.TaskResultDiagnosticReleaseNetworkFailed
	conditions := dependency.conditions
	if child.Status == taskjournal.TaskStatusCompleted {
		if timedOut {
			status, diagnostic = taskjournal.TaskStatusTimedOut, taskjournal.TaskResultDiagnosticDNSActivationTimedOut
		} else {
			ack, ready, err := readResolverAcknowledgement(ctx, repository.store, current.ReadRevision)
			if kind, _ := errs.KindOf(err); kind == errs.KindValidationFailed {
				return false, nil
			}
			if err != nil {
				return false, err
			}
			if releaseNetworkResolverReady(ack, ready, dependency.upstreams) {
				return false, nil
			}
			failed, sources, err := failedResolverConfigurationRequest(ctx, repository.store, current.ReadRevision, ack)
			if err != nil || failed == "" {
				return false, err
			}
			diagnostic = taskjournal.TaskResultDiagnosticDNSActivationFailed
			conditions = append(conditions, sources...)
		}
	} else if timedOut {
		status, diagnostic = taskjournal.TaskStatusTimedOut, taskjournal.TaskResultDiagnosticDNSActivationTimedOut
	}
	result := taskjournal.TaskResultRecord{Kind: taskjournal.TaskResultCompose, Diagnostic: diagnostic}
	if child.Status != taskjournal.TaskStatusCompleted && child.Result != nil {
		result.Failure, result.ExitCode = taskprogress.CloneFailure(child.Result.Failure), child.Result.ExitCode
	}
	plan, err := repository.preparePendingTaskTerminal(ctx, current, status, at, &result)
	if err != nil {
		return false, err
	}
	defer plan.clear()
	conditions = append(plan.conditions, conditions...)
	transaction, err := repository.store.Transact(ctx, conditions, plan.mutations)
	keyvalue.ClearValues(transaction.FailureReads)
	if err != nil {
		return false, err
	}
	if !transaction.Succeeded {
		return true, nil
	}
	terminal := keyvalue.Versioned[TaskRecord]{
		Record:       plan.record,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}
	_, err = repository.finishUnassignedReleaseTerminal(ctx, terminal)
	return true, err
}
