package attachments

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/attachplan"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *MutationService) prepareAcceptedDetach(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, request acceptedAttachDetach,
) (taskdependencies.Prepared, error) {
	defer clear(request.Intent.Ciphertext)
	task := accepted.Task
	current, err := service.repository.GetAttach(ctx, task.TargetID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	snapshot, err := service.repository.CaptureDetachSnapshot(ctx, current)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer snapshot.Clear()
	source := snapshot.Source
	if task.Scope != (workexecution.Scope{TenantID: source.TenantID, ProjectID: source.ProjectID, EnvironmentID: source.Attach.EnvironmentID}) {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Attach ownership changed after Detach was accepted",
		)
	}
	placement, err := service.repository.ResolveDetachHost(ctx)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	consumer, err := attachplan.ConsumerTransition(
		ctx,
		snapshot,
		nil,
		ids.DeriveAt(ids.KindConfig, task.CreatedAt, task.ID, "detach-consumer"),
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if consumer != nil {
		defer func() { clear(consumer.Baseline.CanonicalYaml); clear(consumer.Candidate.CanonicalYaml) }()
	}
	workID := ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "detach")
	input, err := attachremoval.NewDetachInput(ctx, task.ID, workID, &snapshot, nil, consumer)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer input.Clear()
	if !input.HasHostEffects() {
		placement.Executor = workexecution.Controller
	}
	reference, err := input.Reference(ctx, ids.DeriveAt(ids.KindPlan, task.CreatedAt, workID, "captured-input"))
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	timeout := attachexecution.RemovalBudget(0)
	if source.Attach.HookBundle && source.Backing.Desired.Hooks != nil && source.Backing.Desired.Hooks.Detach != nil {
		timeout = attachexecution.RemovalBudget(int64(source.Backing.Desired.Hooks.Detach.TimeoutSeconds))
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = reference, 1, at
	item := workexecution.Item{ID: workID, TaskID: task.ID, Operation: workexecution.Detach, TargetID: task.TargetID,
		Placement: placement, Input: &reference, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		Claims: input.Claims(placement.HostID), CreatedAt: at, UpdatedAt: at,
		Deadline: acceptedaction.ExecutionDeadline(task, at, timeout)}
	capture, err := attachremoval.NewDetachCapture(task, item, input)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer capture.Clear()
	if attachremoval.NeedsAgent(source) && source.Backing.Desired.Adapter == "custom" {
		capture.Hook, err = service.facts.SealBackingHookInputsAtRevision(ctx, capture.OperationID,
			source.Attach.BackingProjectID, *source.Backing.Desired.Hooks, source.SnapshotRevision)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		if capture.Hook != nil {
			capture.Sources = append([]tasksecretpinrecord.Record(nil), capture.Hook.SecretSources...)
		}
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetAttach, ID: task.TargetID}
	_, marker, err := newAttachMutationResponse(request.Locator, request.Intent, task.ID, task.CreatedAt, &target)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(marker.Response.Body)
	publication, err := service.repository.PrepareDetachWork(ctx, etcd.DetachWorkPublication{Accepted: accepted,
		Task: task, Item: item, Input: input, Capture: capture, Marker: marker})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	return taskdependencies.Prepared{
		Task:      task,
		Graph:     []workexecution.Item{item},
		Authority: publication.Change,
		Finalize:  publication.Finish,
	}, nil
}
