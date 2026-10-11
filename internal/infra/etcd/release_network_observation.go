package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReleasePreparationSnapshot describes the native prerequisite, not permission
// to dispatch it. All evidence comes from the visible Task's fixed read revision.
type ReleasePreparationSnapshot struct {
	State         string
	NetworkCount  int
	Deadline      time.Time
	Failure       *taskjournal.TaskResultRecord
	FailureStatus taskjournal.TaskStatus
}

func (repository *TaskRepository) GetReleasePreparation(ctx context.Context,
	task TaskRecord, revision int64,
) (*ReleasePreparationSnapshot, error) {
	if task.Status != taskjournal.TaskStatusPending || task.RetryOf != "" ||
		task.Params[taskjournal.TaskReleaseNetworkChildParam] == "" || taskjournal.IsInternalChild(task.Params) {
		return nil, nil
	}
	dependency, applies, err := repository.readReleaseNetworkDependency(ctx, task, revision)
	if err != nil || !applies {
		return nil, err
	}
	child := dependency.child.Record
	observation := &ReleasePreparationSnapshot{
		State:        "network_queued",
		NetworkCount: len(dependency.preparation.Artifact.GetNetworks()),
		Deadline:     task.CreatedAt.Add(time.Duration(task.TimeoutSeconds) * time.Second),
	}
	switch child.Status {
	case taskjournal.TaskStatusPending:
		return observation, nil
	case taskjournal.TaskStatusRunning:
		observation.State = "network_preparing"
		recovery, err := repository.GetTaskRecovery(ctx, child, revision)
		if err != nil {
			return nil, err
		}
		if recovery != nil {
			observation.State = "network_reconciling"
			if recovery.State == "proof_required" {
				observation.State = "network_proof_required"
			}
			observation.Failure, observation.FailureStatus = &recovery.PrimaryResult, recovery.PrimaryStatus
		}
		return observation, nil
	case taskjournal.TaskStatusFailed, taskjournal.TaskStatusTimedOut, taskjournal.TaskStatusAborted:
		observation.State = "network_failed"
		observation.Failure, observation.FailureStatus = taskjournal.CloneTaskResult(child.Result), child.Status
		return observation, nil
	case taskjournal.TaskStatusCompleted:
		if child.Result == nil || child.Result.ReconciliationRequired {
			return nil, errs.New(errs.KindInternal, "completed network prerequisite has no settled result")
		}
	default:
		return nil, errs.New(errs.KindInternal, "network prerequisite status is invalid")
	}
	observation.State = "dns_waiting"
	ack, ready, err := readResolverAcknowledgement(ctx, repository.store, revision)
	if kind, _ := errs.KindOf(err); kind == errs.KindValidationFailed {
		return observation, nil
	}
	if err != nil {
		return nil, err
	}
	if releaseNetworkResolverReady(ack, ready, dependency.upstreams) {
		observation.State = "ready"
		return observation, nil
	}
	failed, _, err := failedResolverConfigurationRequest(ctx, repository.store, revision, ack)
	if err != nil {
		return nil, err
	}
	if failed != "" {
		observation.State = "dns_failed"
	}
	return observation, nil
}

func releaseNetworkResolverReady(ack resolverAcknowledgement, ready bool, upstreams []string) bool {
	if !ready {
		return false
	}
	for _, address := range upstreams {
		if !slices.Contains(ack.input.ZoneListeners, address) {
			return false
		}
	}
	return true
}
