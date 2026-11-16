package etcd

import (
	"context"
	"errors"
	"slices"
	"time"

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
	Accepted workqueue.TaskSnapshot
	Task     workexecution.Task
	Item     workexecution.Item
	Input    attachremoval.DetachInput
	Capture  attachremoval.WorkCapture
	Marker   idempotency.IdempotencyMarker
}

func (repository *AttachRepository) CaptureDetachSnapshot(
	ctx context.Context,
	current keyvalue.Versioned[attachrecord.Record],
) (attachremoval.DetachSnapshot, error) {
	if err := attachrecord.ValidateAttachVersion(current); err != nil {
		return attachremoval.DetachSnapshot{}, err
	}
	if current.ReadRevision < current.Revision {
		return attachremoval.DetachSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Detach requires the Attach's complete read snapshot",
		)
	}
	snapshot, err := attachremoval.ReadDetachSnapshot(ctx, repository.store, current.ReadRevision, current.Record.ID)
	if err != nil {
		return attachremoval.DetachSnapshot{}, err
	}
	if snapshot.Source.AttachRevision != current.Revision ||
		!attachrecord.AttachImmutableEqual(snapshot.Source.Attach, current.Record) {
		snapshot.Clear()
		return attachremoval.DetachSnapshot{}, errs.New(
			errs.KindStateConflict,
			"Detach source changed while capturing its effects",
		)
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

// PrepareDetachWork captures exact effects after whole-Task prerequisites.
// Its guarded source/resource fragment joins promotion of the accepted Task.
func (repository *AttachRepository) PrepareDetachWork(
	ctx context.Context,
	request DetachWorkPublication,
) (_ DetachWorkCapture, resultErr error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return DetachWorkCapture{}, err
	}
	if err := validateDetachWorkPublication(ctx, request); err != nil {
		return DetachWorkCapture{}, err
	}
	source, task, item := request.Input.Snapshot.Source, request.Task, request.Item
	fence, err := environmentfence.LoadOrdinary(
		ctx,
		repository.store,
		source.Attach.EnvironmentID,
		source.SnapshotRevision,
	)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, source.SnapshotRevision)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	if item.Placement.HostID != host.Binding.HostID {
		return DetachWorkCapture{}, errs.New(
			errs.KindStateConflict,
			"Detach target host changed before publication",
		)
	}
	detaching, ownership, err := attachremoval.PrepareDetachClaim(
		ctx,
		repository.store,
		source,
		task.ID,
		item.Placement.HostID,
	)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	defer keyvalue.ClearMutationValues(ownership.Mutations)
	checks, err := backingchecks.RequireAttachIdle(ctx, repository.store, source.Attach.ID, source.SnapshotRevision)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	sources, err := attachremoval.SourceConditions(ctx, repository.store, source)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	desired, err := prepareAttachDesiredAuthoring(ctx, repository.store, source.Attach, "", true, request.Marker)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	defer clearDesiredMutationFragment(desired)
	ancestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, source.SnapshotRevision,
		hierarchydeletion.MutationScope{TenantID: source.TenantID, ProjectID: source.ProjectID}, nil, nil)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	defer ancestry.Clear()
	groups := [][]keyvalue.Condition{fence.TransactionConditions(), host.Conditions, sources, ownership.Conditions,
		request.Input.Snapshot.ConsumerConditions(), desired.conditions, ancestry.Conditions(), {checks}}
	groups = append(
		groups,
		[]keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: request.Accepted.Revision}},
	)
	admission, err := resourcePublicationConditions(groups...)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	inputs, err := attachremoval.NewInputRepository(repository.store)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	custody := DetachWorkCapture{
		store:            repository.store,
		taskID:           task.ID,
		workID:           item.ID,
		operationID:      request.Capture.OperationID,
		backingProjectID: source.Attach.BackingProjectID,
		input:            *item.Input,
	}
	defer func() {
		if resultErr != nil {
			cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer cancel()
			resultErr = errors.Join(resultErr, custody.Finish(cleanup, false))
		}
	}()
	captured, err := inputs.PrepareDetachPublicationCapture(
		ctx,
		workqueue.TaskSnapshot{Task: task, Revision: request.Accepted.Revision},
		item,
		request.Input,
		request.Capture,
		admission,
	)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	defer keyvalue.ClearMutationValues(captured.Mutations)
	admission, err = resourcePublicationConditions(admission, captured.Conditions)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	reference, staged, err := inputs.StageDetach(ctx, item.Input.ID, request.Input, admission)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	defer keyvalue.ClearMutationValues(staged.Mutations)
	if reference != *item.Input {
		return DetachWorkCapture{}, errs.New(
			errs.KindStateConflict,
			"Detach staged input differs from its admitted reference",
		)
	}
	value, err := attachrecord.EncodeAttachRecord(detaching)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	defer clear(value)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return DetachWorkCapture{}, err
	}
	defer clear(epoch.Value)
	conditions, err := resourcePublicationConditions(staged.Conditions)
	if err != nil {
		return DetachWorkCapture{}, err
	}
	var mutations []keyvalue.Mutation
	mutations = append(mutations, ownership.Mutations...)
	mutations = append(mutations, captured.Mutations...)
	mutations = append(mutations, staged.Mutations...)
	mutations = append(mutations, desired.mutations...)
	mutations = append(mutations, ancestry.Mutations()...)
	mutations = append(
		mutations,
		epoch,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: attachrecord.AttachKey(source.Attach.ID), Value: value},
	)
	owned := make([]keyvalue.Mutation, len(mutations))
	for index, mutation := range mutations {
		owned[index] = mutation
		owned[index].Value = append([]byte(nil), mutation.Value...)
	}
	custody.Change = workqueue.Change{Conditions: conditions, Mutations: owned}
	return custody, nil

}

func validateDetachWorkPublication(ctx context.Context, request DetachWorkPublication) error {
	task, item, source := request.Task, request.Item, request.Input.Snapshot.Source
	if err := request.Input.Validate(ctx); err != nil {
		return err
	}
	if task.Validate() != nil || item.Validate() != nil || item.Input == nil ||
		item.Operation != workexecution.Detach ||
		request.Input.Cascade != nil ||
		task.Action != workexecution.ActionDetach ||
		item.TargetID != source.Attach.ID ||
		task.TargetID != source.Attach.ID ||
		request.Input.TaskID != task.ID ||
		request.Input.WorkItemID != item.ID ||
		task.Scope != (workexecution.Scope{TenantID: source.TenantID, ProjectID: source.ProjectID, EnvironmentID: source.Attach.EnvironmentID}) ||
		len(item.Dependencies) != 0 ||
		!slices.Equal(item.Claims, request.Input.Claims(item.Placement.HostID)) ||
		task.Intent != *item.Input ||
		request.Accepted.Revision <= 0 || !request.Accepted.Task.PendingPreparation() || request.Accepted.Task.ID != task.ID ||
		request.Marker.TaskID != task.ID ||
		request.Marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		request.Marker.Locator.ScopeID != source.Attach.EnvironmentID ||
		request.Marker.Locator.Method != "DELETE" ||
		request.Marker.Locator.Route != "/attaches/{id}" ||
		request.Marker.ReplayTarget == nil ||
		*request.Marker.ReplayTarget != (idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetAttach, ID: source.Attach.ID}) {
		return errs.New(
			errs.KindValidationFailed,
			"Detach publication differs from its captured resource, scope or replay",
		)
	}
	wantExecutor := workexecution.Controller
	if request.Input.HasHostEffects() {
		wantExecutor = workexecution.Agent
	}
	if item.Placement.Executor != wantExecutor {
		return errs.New(errs.KindValidationFailed, "Detach executor differs from its captured effects")
	}
	if task.Actor != workexecution.OperatorActor {
		return errs.New(errs.KindScopeUnauthorized, "Direct Detach requires an operator Task")
	}
	return nil
}
