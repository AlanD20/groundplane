package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DetachWorkPublication struct {
	Task       workexecution.Task
	Item       workexecution.Item
	Input      attachremoval.DetachInput
	Marker     idempotency.IdempotencyMarker
	Initiation *TaskInitiation
}

func (repository *AttachRepository) CaptureDetachSnapshot(ctx context.Context, current keyvalue.Versioned[attachrecord.Record]) (attachremoval.DetachSnapshot, error) {
	if err := attachrecord.ValidateAttachVersion(current); err != nil {
		return attachremoval.DetachSnapshot{}, err
	}
	if current.ReadRevision < current.Revision {
		return attachremoval.DetachSnapshot{}, errs.New(errs.KindStateConflict, "Detach requires the Attach's complete read snapshot")
	}
	snapshot, err := attachremoval.ReadDetachSnapshot(ctx, repository.store, current.ReadRevision, current.Record.ID)
	if err != nil {
		return attachremoval.DetachSnapshot{}, err
	}
	if snapshot.Source.AttachRevision != current.Revision || !attachrecord.AttachImmutableEqual(snapshot.Source.Attach, current.Record) {
		snapshot.Clear()
		return attachremoval.DetachSnapshot{}, errs.New(errs.KindStateConflict, "Detach source changed while capturing its effects")
	}
	return snapshot, nil
}

func (repository *AttachRepository) ResolveDetachHost(ctx context.Context) (workexecution.Placement, error) {
	host, err := hostbindings.ReadLocal(ctx, repository.store, 0)
	if err != nil {
		return workexecution.Placement{}, err
	}
	return workexecution.Placement{Executor: workexecution.Agent, HostID: host.Binding.HostID}, nil
}

// PublishDetachWork atomically publishes direct Detach, its desired omission,
// immutable input root and replay. It does not call the old Task publisher,
// generate an ExecutionPlan, or start a child operation for credential cleanup.
func (repository *AttachRepository) PublishDetachWork(ctx context.Context, request DetachWorkPublication) (IdempotencyTransactionResult, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateDetachWorkPublication(ctx, request); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, request.Marker); err != nil || found {
		return existing, err
	}
	source, task, item := request.Input.Snapshot.Source, request.Task, request.Item
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, source.Attach.EnvironmentID, source.SnapshotRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, source.SnapshotRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if item.Placement.HostID != host.Binding.HostID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Detach target host changed before publication")
	}
	detaching, err := attachrecord.BeginAttachDetaching(source.Attach, task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	checks, err := backingchecks.RequireAttachIdle(ctx, repository.store, source.Attach.ID, source.SnapshotRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	sources, err := attachremoval.SourceConditions(ctx, repository.store, source)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	desired, err := prepareAttachDesiredAuthoring(ctx, repository.store, source.Attach, "", true, request.Marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearRouteHeadPublication(desired)
	ancestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, source.SnapshotRevision,
		hierarchydeletion.MutationScope{TenantID: source.TenantID, ProjectID: source.ProjectID}, nil, nil)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer ancestry.Clear()
	groups := [][]keyvalue.Condition{fence.TransactionConditions(), host.Conditions, sources,
		request.Input.Snapshot.ConsumerConditions(), desired.conditions, ancestry.Conditions(), {checks}}
	if request.Initiation != nil {
		groups = append(groups, request.Initiation.fences)
	}
	admission, err := resourcePublicationConditions(groups...)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	inputs, err := attachremoval.NewInputRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	reference, staged, err := inputs.StageDetach(ctx, item.Input.ID, request.Input, admission)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(staged.Mutations)
	if reference != *item.Input {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Detach staged input differs from its admitted reference")
	}
	publication, err := workqueue.PrepareSingleTask(task, item, request.Marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(publication.Mutations)
	value, err := attachrecord.EncodeAttachRecord(detaching)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epoch.Value)
	conditions, err := resourcePublicationConditions(staged.Conditions, publication.Conditions)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	mutations := append([]keyvalue.Mutation(nil), publication.Mutations...)
	mutations = append(mutations, staged.Mutations...)
	mutations = append(mutations, desired.mutations...)
	mutations = append(mutations, ancestry.Mutations()...)
	mutations = append(mutations, epoch, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: attachrecord.AttachKey(source.Attach.ID), Value: value})
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, resourceWorkPublicationConflict(conditions))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replay, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	transactions, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return replay.applyEnvironmentBlueprint(ctx, request.Marker, plan, transactions)
}

func validateDetachWorkPublication(ctx context.Context, request DetachWorkPublication) error {
	task, item, source := request.Task, request.Item, request.Input.Snapshot.Source
	if err := request.Input.Validate(ctx); err != nil {
		return err
	}
	if task.Validate() != nil || item.Validate() != nil || item.Input == nil || item.Operation != workexecution.Detach ||
		task.Action != workexecution.ActionDetach || item.TargetID != source.Attach.ID || task.TargetID != source.Attach.ID ||
		request.Input.TaskID != task.ID || request.Input.WorkItemID != item.ID ||
		task.Scope != (workexecution.Scope{TenantID: source.TenantID, ProjectID: source.ProjectID, EnvironmentID: source.Attach.EnvironmentID}) ||
		len(item.Dependencies) != 0 || !slices.Equal(item.Claims, request.Input.Claims(item.Placement.HostID)) ||
		task.Intent.SHA256 != request.Marker.Intent.CiphertextDigest || request.Marker.TaskID != task.ID ||
		request.Marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment || request.Marker.Locator.ScopeID != source.Attach.EnvironmentID ||
		request.Marker.Locator.Method != "DELETE" || request.Marker.Locator.Route != "/attaches/{id}" ||
		request.Marker.ReplayTarget == nil || *request.Marker.ReplayTarget != (idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetAttach, ID: source.Attach.ID}) {
		return errs.New(errs.KindValidationFailed, "Detach publication differs from its captured resource, scope or replay")
	}
	wantExecutor := workexecution.Controller
	if request.Input.HasHostEffects() {
		wantExecutor = workexecution.Agent
	}
	if item.Placement.Executor != wantExecutor {
		return errs.New(errs.KindValidationFailed, "Detach executor differs from its captured effects")
	}
	if request.Initiation != nil {
		owner := request.Initiation.Owner()
		if owner.TenantID != task.Scope.TenantID || owner.ProjectID != task.Scope.ProjectID || owner.EnvironmentID != task.Scope.EnvironmentID ||
			workexecution.Actor(request.Initiation.Actor()) != task.Actor {
			return errs.New(errs.KindScopeUnauthorized, "Detach initiation does not own the captured Environment")
		}
	} else if task.Actor != workexecution.OperatorActor {
		return errs.New(errs.KindScopeUnauthorized, "system Detach requires its captured initiation")
	}
	return nil
}
