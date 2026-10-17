package blueprintunits

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareVolumeCompletionReceipt records the effect of the exact native
// attempt. Its caller must join the Volume owner's final host proof, applied
// storage record and work completion in the same transaction. This method
// grants no host effects and creates no parallel execution record.
func (repository *Repository) PrepareVolumeCompletionReceipt(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	return repository.prepareForwardResourceReceipt(ctx, view, ids.KindVolume, workexecution.Volume, false)
}

func (repository *Repository) PrepareNetworkCompletionReceipt(ctx context.Context, view workqueue.ResumeView, removed bool) (workqueue.Change, error) {
	return repository.prepareForwardResourceReceipt(ctx, view, ids.KindNetwork, workexecution.Network, removed)
}

func (repository *Repository) prepareForwardResourceReceipt(ctx context.Context, view workqueue.ResumeView,
	kind ids.Kind, operation workexecution.Operation, removed bool,
) (workqueue.Change, error) {
	checkpoint := view.Checkpoint.Record
	if checkpoint == nil || checkpoint.AttemptID != view.Attempt.Attempt.ID {
		return workqueue.Change{}, invalidRecord()
	}
	return repository.prepareResourceCompletionReceipt(ctx, view, kind, operation, resourceReceiptEvidence{
		direction: workexecution.Forward, result: checkpoint.Evidence, removal: removed, absent: removed,
	})
}

func (repository *Repository) PrepareNetworkRecoveryReceipt(ctx context.Context, view workqueue.ResumeView,
	source networkexecution.Recovery, boundary networkexecution.Progress, action agentpb.NetworkWorkAction,
	report networkexecution.RecoveryReport,
) (workqueue.Change, error) {
	_, evidence, err := networkexecution.EncodeRecoveryReport(view.Attempt.Attempt, source, action, view.Work.Item.Item.TargetID, boundary, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	removed := action == agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE
	return repository.prepareResourceCompletionReceipt(ctx, view, ids.KindNetwork, workexecution.Network, resourceReceiptEvidence{
		direction: workexecution.Recovery, result: evidence, removal: removed, absent: report.Observation.Absent,
		preserve: removed && report.Observation.Present,
	})
}

type resourceReceiptEvidence struct {
	direction workexecution.Direction
	result    workexecution.Input
	removal   bool
	absent    bool
	preserve  bool
}

func (repository *Repository) prepareResourceCompletionReceipt(ctx context.Context, view workqueue.ResumeView,
	kind ids.Kind, operation workexecution.Operation, proof resourceReceiptEvidence,
) (workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if task.Validate() != nil || item.Validate() != nil || attempt.Validate() != nil ||
		task.Action != workexecution.ActionApply || task.Scope.EnvironmentID == "" ||
		item.Operation != operation || item.TaskID != task.ID || item.Input == nil ||
		!view.ActiveAttempt || attempt.FinishedAt != nil || attempt.Direction != proof.direction ||
		attempt.TaskID != task.ID || attempt.WorkItemID != item.ID || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Agent || attempt.Executor.HostID != item.Placement.HostID ||
		proof.result.Validate() != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint resource receipt requires its active proven attempt")
	}
	graph, err := repository.LoadCapturedGraph(ctx, task, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	var unit *Unit
	for index, captured := range graph.Record.Items {
		if captured.ID != item.ID {
			continue
		}
		if captured.TaskID != item.TaskID || captured.Operation != item.Operation || captured.TargetID != item.TargetID ||
			captured.Placement != item.Placement || !slices.Equal(captured.Dependencies, item.Dependencies) ||
			!slices.Equal(captured.Claims, item.Claims) || !captured.CreatedAt.Equal(item.CreatedAt) ||
			!captured.Deadline.Equal(item.Deadline) {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint resource receipt differs from its captured work")
		}
		unit = &graph.Record.Units[index]
		break
	}
	if unit == nil || unit.Target.Kind != kind || unit.Removal != proof.removal ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint resource receipt has no captured storage effect")
	}
	snapshot, err := repository.LoadAtRevision(ctx, task.Scope.EnvironmentID, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if snapshot.Epoch.Sequence == ^uint64(0) {
		return workqueue.Change{}, invalidRecord()
	}
	var appliedRevision int64
	var prior AppliedRecord
	found := false
	for _, applied := range snapshot.Applied {
		if applied.Record.Target == unit.Target {
			if found {
				return workqueue.Change{}, corruptRecord()
			}
			found, appliedRevision = true, applied.Revision
			prior = applied.Record
		}
	}
	if !found {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint resource receipt has no prior resource state")
	}
	state := Applied
	if proof.absent {
		state = Absent
	}
	receipt := AppliedRecord{EnvironmentID: task.Scope.EnvironmentID, Target: unit.Target,
		State: state, Fingerprint: unit.Fingerprint, ParentTaskID: task.ID,
		Execution: &AppliedExecution{TaskID: task.ID, WorkItemID: item.ID, AttemptID: attempt.ID,
			Executor: attempt.Executor, Input: attempt.Input, Result: proof.result}}
	if proof.preserve {
		if prior.State != Applied {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Present Network has no proven prior configuration")
		}
		receipt.Fingerprint = prior.Fingerprint
	}
	if proof.absent {
		receipt.Fingerprint = ""
	}
	value, err := EncodeApplied(receipt)
	if err != nil {
		return workqueue.Change{}, err
	}
	epoch, err := EncodeEpoch(EpochRecord{EnvironmentID: snapshot.EnvironmentID, Sequence: snapshot.Epoch.Sequence + 1})
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	// A newer desired head does not erase an already authorized host effect.
	// Its fingerprint remains the original one; newer reconciliation must
	// compare it, not silently adopt it as the newer intent's result.
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: CapturedGraphKey(snapshot.EnvironmentID, task.ID), ModRevision: graph.Revision},
		{Key: EpochKey(snapshot.EnvironmentID), ModRevision: snapshot.EpochRevision},
		{Key: AppliedKey(snapshot.EnvironmentID, unit.Target), ModRevision: appliedRevision},
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: AppliedKey(snapshot.EnvironmentID, unit.Target), Value: value},
		{Type: keyvalue.MutationPut, Key: EpochKey(snapshot.EnvironmentID), Value: epoch},
	}}, nil
}
