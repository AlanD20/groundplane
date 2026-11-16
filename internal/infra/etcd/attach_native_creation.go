package etcd

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type AttachWorkPublication struct {
	Accepted   workqueue.TaskSnapshot
	At         time.Time
	Deadline   time.Time
	Task       workexecution.Task
	WorkID     string
	Scope      AttachCreateScope
	Generation blueprintattachinputs.Generation
	Marker     idempotency.IdempotencyMarker
}

// PrepareAttachWork captures a direct action after its whole-Task prerequisites.
// The returned authority joins promotion of the original accepted Task.
func (repository *AttachRepository) PrepareAttachWork(
	ctx context.Context,
	request AttachWorkPublication,
) (_ AttachWorkCapture, resultErr error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return AttachWorkCapture{}, err
	}
	task, scope, draft := request.Task, request.Scope, request.Generation
	if task.Validate() != nil || task.Action != workexecution.ActionAttach || task.PublicationComplete ||
		task.ExpectedItems != 1 ||
		blueprintattachinputs.ValidateDraft(draft) != nil ||
		len(scope.Services) != 1 ||
		task.TargetID != draft.AttachID ||
		draft.ParentTaskID != task.ID ||
		task.Actor != workexecution.OperatorActor ||
		task.Scope != (workexecution.Scope{TenantID: scope.Tenant.Record.ID, ProjectID: scope.Project.Record.ID, EnvironmentID: draft.EnvironmentID}) ||
		request.Accepted.Revision <= 0 || !request.Accepted.Task.PendingPreparation() || request.Accepted.Task.ID != task.ID ||
		request.At.Before(
			task.CreatedAt,
		) || request.Deadline.After(task.Deadline) || !request.Deadline.After(request.At) ||
		request.Marker.TaskID != task.ID ||
		request.Marker.Locator.Method != "POST" ||
		request.Marker.Locator.Route != "/attaches" ||
		request.Marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		request.Marker.Locator.ScopeID != draft.EnvironmentID ||
		request.Marker.ReplayTarget != nil {
		return AttachWorkCapture{}, errs.New(
			errs.KindValidationFailed,
			"Attach publication differs from its captured action or replay",
		)
	}
	record, err := draft.PendingRecord(task.ID, task.CreatedAt)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	var ownFacts *attachrecord.EncryptedFacts
	if record.OwnsCredential() && len(draft.GeneratedInputs.Ciphertext) != 0 {
		ownFacts = &draft.GeneratedInputs
	}
	if err := validateAttachCreateScope(ctx, scope, record, ownFacts); err != nil {
		return AttachWorkCapture{}, err
	}
	if draft.AdapterKey != scope.BackingService.Record.Desired.Adapter ||
		draft.Authentication != scope.BackingService.Record.Desired.Authentication ||
		draft.Provision == nil && (scope.BackingService.Record.Desired.Hooks == nil ||
			!blueprintattachinputs.MatchesConfiguration(draft, *scope.BackingService.Record.Desired.Hooks)) {
		return AttachWorkCapture{}, errs.New(
			errs.KindValidationFailed,
			"Attach captured procedure differs from its backing Service",
		)
	}
	revision := scope.Services[0].ReadRevision
	if revision <= 0 {
		return AttachWorkCapture{}, errs.New(
			errs.KindStateConflict,
			"Attach consumer requires a fixed read snapshot",
		)
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, record.EnvironmentID, revision)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	host, err := hostbindings.ReadLocal(ctx, repository.store, revision)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	inputs, err := attachprovision.NewInputRepository(repository.store)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	captured, runtimeGuards, err := inputs.CaptureConsumer(
		ctx,
		draft,
		host.Binding.HostID,
		ids.DeriveAt(ids.KindConfig, task.CreatedAt, task.ID, "attach-consumer"),
		revision,
	)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	// Only these fields were allocated here. Encrypted draft fields remain
	// owned by the Controller caller until publication returns.
	defer func() {
		clear(captured.ConsumerTransition)
		if captured.ConsumerRuntime != nil {
			clear(captured.ConsumerRuntime.Runtime.CurrentArtifact)
			clear(captured.ConsumerRuntime.Runtime.RetainedPriorArtifact)
		}
	}()
	desired, err := prepareAttachDesiredAuthoring(ctx, repository.store, record, "", false, request.Marker)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	defer clearDesiredMutationFragment(desired)
	ancestry, err := hierarchydeletion.BindMutationEpochs(ctx, repository.store, revision,
		hierarchydeletion.MutationScope{TenantID: task.Scope.TenantID, ProjectID: task.Scope.ProjectID}, nil, nil)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	defer ancestry.Clear()
	sourceGuards, err := attachDesiredHeadConditions(
		record.EnvironmentID,
		scope.DesiredHead.Revision,
		scope.BackingService,
		scope.Services,
	)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	sourceGuards = append(sourceGuards, services.ServiceRuntimeCondition(scope.BackingService))
	admission, err := resourcePublicationConditions(fence.TransactionConditions(), host.Conditions, runtimeGuards,
		desired.conditions, ancestry.Conditions(), sourceGuards, []keyvalue.Condition{
			{Key: hierarchy.TenantKey(scope.Tenant.Record.ID), ModRevision: scope.Tenant.Revision},
			{Key: hierarchy.ProjectKey(scope.Project.Record.ID), ModRevision: scope.Project.Revision},
			{Key: hierarchy.EnvironmentKey(scope.Environment.Record.ID), ModRevision: scope.Environment.Revision},
			{Key: hierarchy.ProjectKey(scope.BackingProject.Record.ID), ModRevision: scope.BackingProject.Revision},
			{
				Key:         hierarchy.EnvironmentKey(scope.BackingEnvironment.Record.ID),
				ModRevision: scope.BackingEnvironment.Revision,
			},
		})
	if err != nil {
		return AttachWorkCapture{}, err
	}
	admission = append(
		admission,
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: request.Accepted.Revision},
	)
	prepared, err := inputs.PrepareInput(ctx, task, request.WorkID, captured, admission)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	defer keyvalue.ClearMutationValues(prepared.Change.Mutations)
	transferred := false
	defer func() {
		if transferred {
			return
		}
		defer blueprintattachinputs.Clear(&prepared.Generation)
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		resultErr = errors.Join(resultErr, prepared.Finish(cleanupCtx))
	}()
	reference, err := blueprintattachinputs.WorkInput(prepared.Generation)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	task.Intent = reference
	placement := workexecution.Placement{Executor: workexecution.Agent, HostID: host.Binding.HostID}
	if prepared.Generation.SettlesInController() {
		placement.Executor = workexecution.Controller
	}
	item := workexecution.Item{
		ID:        request.WorkID,
		TaskID:    task.ID,
		Operation: workexecution.Attach,
		TargetID:  record.ID,
		Placement: placement,
		Input:     &reference,
		Phase:     workexecution.Ready,
		Cleanup:   workexecution.CleanupNone,
		Claims:    attachprovision.ProvisionClaims(prepared.Generation, host.Binding.HostID),
		CreatedAt: request.At,
		UpdatedAt: request.At,
		Deadline:  request.Deadline,
	}
	binding, err := inputs.PrepareDirectBinding(ctx, task, item, prepared.Generation, revision)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	defer keyvalue.ClearMutationValues(binding.Mutations)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return AttachWorkCapture{}, err
	}
	defer clear(epoch.Value)
	conditions, err := resourcePublicationConditions(
		prepared.Change.Conditions,
		binding.Conditions,
	)
	if err != nil {
		return AttachWorkCapture{}, err
	}
	mutations := append([]keyvalue.Mutation(nil), prepared.Change.Mutations...)
	mutations = append(mutations, binding.Mutations...)
	mutations = append(mutations, desired.mutations...)
	mutations = append(mutations, ancestry.Mutations()...)
	mutations = append(mutations, epoch)
	owned := make([]keyvalue.Mutation, len(mutations))
	for index, mutation := range mutations {
		owned[index] = mutation
		owned[index].Value = append([]byte(nil), mutation.Value...)
	}
	transferred = true
	return AttachWorkCapture{
		Task:   task,
		Item:   item,
		Change: workqueue.Change{Conditions: conditions, Mutations: owned},
		input:  prepared,
	}, nil
}
