package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

func (repository *AttachRepository) BeginAttachDetachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.BeginAttachDetachWithTaskHookInputs(ctx, scope, current, nil, renderInput, task, marker)
}

func (repository *AttachRepository) BeginAttachDetachWithTaskHookInputs(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.beginAttachDetachWithTask(ctx, scope, current, hookInputs, renderInput, task, marker, nil)
}

func (repository *AttachRepository) BeginAttachDetachWithTaskInitiation(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	initiation TaskInitiation,
) (IdempotencyTransactionResult, error) {
	return repository.BeginAttachDetachWithTaskInitiationHookInputs(
		ctx, scope, current, nil, renderInput, task, marker, initiation,
	)
}

func (repository *AttachRepository) BeginAttachDetachWithTaskInitiationHookInputs(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	initiation TaskInitiation,
) (IdempotencyTransactionResult, error) {
	return repository.beginAttachDetachWithTask(ctx, scope, current, hookInputs, renderInput, task, marker, &initiation)
}

func (repository *AttachRepository) beginAttachDetachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	provided *TaskInitiation,
) (_ IdempotencyTransactionResult, returnErr error) {
	if err := validateAttachDetachScope(ctx, scope, current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	detaching, err := attachrecord.BeginAttachDetaching(current.Record, task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachDetachTask(current.Record, detaching, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachTaskRenderInputScope(scope, detaching, task, renderInput); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	mutationContext, err := loadOrdinaryEnvironmentMutationContext(
		ctx,
		repository.store,
		current.Record.EnvironmentID,
		attachrecord.AttachKey(current.Record.ID),
		scope.Project.Record.ID,
		scope.Project.Record.TenantID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachRuntimeEpoch(mutationContext, current.Record.EnvironmentID, renderInput); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	versionedTenant, versionedProject, versionedEnvironment, err := mutationContext.versionHierarchy(
		&scope.Tenant,
		scope.Project,
		scope.Environment,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if attachDetachWithTaskOperationCount(detaching) > etcdstore.MaximumOperations {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Attach detach combination exceeds the atomic transaction limit",
		)
	}
	revision := mutationContext.readRevision
	exclusionCondition, err := requireAttachBackupSourceExclusionAbsent(
		ctx, repository.store, current.Record.ID, revision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	dependents, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachGrantedByPrefix(current.Record.ID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if dependents == nil || dependents.ReadRevision != revision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Attach detach grant index read returned an invalid revision",
		)
	}
	if len(dependents.Values) != 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse,
			"Attach is referenced by another Attach grant",
		)
	}
	credentialDependents, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachCredentialByPrefix(current.Record.ID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if credentialDependents == nil || credentialDependents.ReadRevision != revision {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Attach credential reference index read returned an invalid revision",
		)
	}
	if len(credentialDependents.Values) != 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindResourceInUse,
			"Attach credential is used by another Service",
		)
	}
	defer clearRangeValues(credentialDependents.Values)
	var credentialReferenceCondition *etcdstore.Condition
	if !current.Record.OwnsCredential() {
		key := attachrecord.AttachCredentialByKey(current.Record.CredentialAttachID, current.Record.ID)
		read, readErr := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
		if readErr != nil {
			return IdempotencyTransactionResult{}, readErr
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil ||
			string(read.Values[0].Value) != current.Record.ID {
			return IdempotencyTransactionResult{}, attachrecord.CorruptAttachRecord()
		}
		defer etcdstore.ClearValues(read.Values)
		condition := etcdstore.Condition{Key: key, ModRevision: read.Values[0].ModRevision}
		credentialReferenceCondition = &condition
	}

	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	publication, err := prepareBackingHookTaskPublication(ctx, repository.store, task, hookInputs)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer publication.clear()
	defer func() { returnErr = publication.finish(ctx, repository.store, returnErr) }()
	task = publication.task
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	attachValue, err := attachrecord.EncodeAttachRecord(detaching)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(attachValue)
	renderInputValue, err := encodeAttachTaskRenderInput(renderInput)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(renderInputValue)
	environmentValue, err := hierarchyrecord.EncodeEnvironment(scope.Environment.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(environmentValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	taskReference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskReference)
	planReferenceKey, planReferenceValue, _, err := prepareAttachTaskPlanReference(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(planReferenceValue)
	desiredHeadConditions, err := attachDesiredHeadConditions(
		current.Record.EnvironmentID,
		scope.DesiredHead.Revision,
		scope.BackingService,
		scope.Services,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}

	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{Key: attachrecord.AttachKey(current.Record.ID), ModRevision: current.Revision},
		exclusionCondition,
		{Key: attachrecord.AttachCredentialByPrefix(current.Record.ID), Prefix: true},
		{Key: hierarchyrecord.EnvironmentKey(scope.Environment.Record.ID), ModRevision: scope.Environment.Revision},
		{Key: hierarchyrecord.ProjectKey(scope.Project.Record.ID), ModRevision: scope.Project.Revision},
		{Key: hierarchyrecord.EnvironmentKey(scope.BackingEnvironment.Record.ID), ModRevision: scope.BackingEnvironment.Revision},
		{Key: hierarchyrecord.ProjectKey(scope.BackingProject.Record.ID), ModRevision: scope.BackingProject.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
		{Key: deletionTombstoneKey("environment", current.Record.EnvironmentID)},
		{Key: deletionTombstoneKey("project", scope.Project.Record.ID)},
		{Key: deletionTombstoneKey("tenant", scope.Project.Record.TenantID)},
		{Key: deletionTombstoneKey("environment", current.Record.BackingEnvironmentID)},
		{Key: deletionTombstoneKey("project", current.Record.BackingProjectID)},
		{Key: deletionTombstoneKey("service", current.Record.BackingServiceID)},
		{Key: attachTaskRenderInputKey(task.PlanID)},
		{Key: planReferenceKey},
		{Key: hierarchyrecord.TenantKey(scope.Tenant.Record.ID), ModRevision: scope.Tenant.Revision},
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(current.Record.EnvironmentID, renderInput.DesiredRevisionID),
			ModRevision: scope.ComposeProjection.Revision,
		},
	}
	conditions = append(conditions, desiredHeadConditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(detaching.ID), Value: attachValue},
		{Type: etcdstore.MutationPut, Key: attachTaskRenderInputKey(task.PlanID), Value: renderInputValue},
		{Type: etcdstore.MutationPut, Key: planReferenceKey, Value: planReferenceValue},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentKey(scope.Environment.Record.ID), Value: environmentValue},
	}
	for _, service := range scope.Services {
		serviceID := service.Record.Desired.ID
		conditions = append(conditions,
			etcdstore.Condition{Key: deletionTombstoneKey("service", serviceID)},
		)
	}
	for _, grant := range scope.Grants {
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(grant.Record.ID), ModRevision: grant.Revision},
			etcdstore.Condition{Key: deletionTombstoneKey("attach", grant.Record.ID)},
		)
	}
	if !current.Record.OwnsCredential() {
		owner := *scope.CredentialOwner
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(owner.Record.ID), ModRevision: owner.Revision},
			*credentialReferenceCondition,
			etcdstore.Condition{Key: deletionTombstoneKey("attach", owner.Record.ID)},
		)
	}
	initiation := TaskInitiation{}
	if provided == nil {
		initiation, err = newEnvironmentTaskInitiation(
			versionedTenant, versionedProject, versionedEnvironment, taskjournal.TaskActorOperator,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
	} else {
		initiation = *provided
		conditions = append(conditions, initiation.fences...)
		if attachDetachWithTaskOperationCount(current.Record)+len(initiation.fences) > etcdstore.MaximumOperations {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"attach detach initiation exceeds the atomic transaction limit",
			)
		}
	}
	conditions, mutations, classify, err := publication.bind(
		conditions, mutations, classifyAttachDetachTaskConflict,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return repository.publishAttachRuntimeTask(ctx, task, renderInput, initiation, marker,
		mutationContext, conditions, mutations, classify)
}

func validateAttachDetachScope(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := validateAttachVersion(current); err != nil {
		return err
	}
	record := current.Record
	if record.Status != core.AttachReady &&
		(record.Status != core.AttachFailed || record.Operation != attachrecord.AttachOperationProvision) {
		return errs.New(errs.KindStateConflict, "Attach is not eligible for initial detach")
	}
	if scope.Tenant.Revision <= 0 || scope.Project.Revision <= 0 || scope.Environment.Revision <= 0 ||
		scope.DesiredHead.Revision <= 0 || scope.ComposeProjection.Revision <= 0 ||
		scope.BackingProject.Revision <= 0 || scope.BackingEnvironment.Revision <= 0 ||
		scope.BackingService.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach detach scope records must be versioned")
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID || scope.Project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		scope.Project.Record.TenantID == "" || scope.Environment.Record.ProjectID != scope.Project.Record.ID ||
		record.EnvironmentID != scope.Environment.Record.ID {
		return errs.New(errs.KindScopeUnauthorized, "Attach detach consumer hierarchy is invalid")
	}
	if scope.BackingProject.Record.Kind != hierarchyrecord.ProjectKindBacking || scope.BackingProject.Record.TenantID != "" ||
		scope.BackingEnvironment.Record.ProjectID != scope.BackingProject.Record.ID ||
		scope.BackingService.Record.EnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingProjectID != scope.BackingProject.Record.ID ||
		record.BackingEnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingServiceID != scope.BackingService.Record.Desired.ID ||
		record.BackingNetworkID != scope.BackingService.Record.BackingNetworkID {
		return errs.New(errs.KindScopeUnauthorized, "Attach detach backing hierarchy is invalid")
	}
	serviceIDs := make([]string, 0, len(scope.Services))
	for _, service := range scope.Services {
		if service.Revision <= 0 || service.Record.EnvironmentID != record.EnvironmentID {
			return errs.New(errs.KindScopeUnauthorized, "Attach detach Service is outside the Environment")
		}
		serviceIDs = append(serviceIDs, service.Record.Desired.ID)
	}
	slices.Sort(serviceIDs)
	if len(serviceIDs) != 1 || serviceIDs[0] != record.ServiceID {
		return errs.New(errs.KindValidationFailed, "Attach detach Services do not match the durable record")
	}
	grantIDs := make([]string, 0, len(scope.Grants))
	for _, grant := range scope.Grants {
		if grant.Revision <= 0 || grant.Record.EnvironmentID != record.EnvironmentID ||
			grant.Record.BackingServiceID != record.BackingServiceID ||
			grant.Record.BackingNetworkID != record.BackingNetworkID || grant.Record.Status != core.AttachReady {
			return errs.New(
				errs.KindScopeUnauthorized,
				"Attach detach grant must be ready in the same Environment and backing Service",
			)
		}
		grantIDs = append(grantIDs, grant.Record.ID)
	}
	slices.Sort(grantIDs)
	if !slices.Equal(grantIDs, record.GrantAttachIDs) {
		return errs.New(errs.KindValidationFailed, "Attach detach grants do not match the durable record")
	}
	if record.OwnsCredential() {
		if scope.CredentialOwner != nil {
			return errs.New(errs.KindValidationFailed, "Credential-owning Attach detach has another owner")
		}
	} else {
		owner := scope.CredentialOwner
		if owner == nil || owner.Revision <= 0 || owner.Record.ID != record.CredentialAttachID ||
			!owner.Record.OwnsCredential() || owner.Record.Status != core.AttachReady ||
			owner.Record.EnvironmentID != record.EnvironmentID ||
			owner.Record.BackingServiceID != record.BackingServiceID {
			return errs.New(errs.KindScopeUnauthorized, "Attach detach credential owner is invalid")
		}
	}
	return nil
}

func validateAttachDetachTask(
	current attachrecord.Record,
	detaching attachrecord.Record,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	validOwnership := detaching.Status == core.AttachDetaching && detaching.Operation == attachrecord.AttachOperationDetach &&
		detaching.TaskID == task.ID && attachImmutableEqual(current, detaching)
	validTaskShape := task.Type == taskjournal.TaskDetach && task.Target == current.ID && task.Executor == taskjournal.TaskExecutorAgent &&
		task.Status == taskjournal.TaskStatusPending && len(task.Params) == 1 && len(task.Materializations) == 0 &&
		task.Params[taskjournal.TaskMutationEnvironmentParam] == current.EnvironmentID
	if !validOwnership || !validTaskShape {
		return errs.New(errs.KindValidationFailed, "Attach detach Task does not own its detaching Attach")
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.EnvironmentID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || marker.ReplayTarget == nil ||
		marker.ReplayTarget.Kind != idempotencyrecord.IdempotencyReplayTargetAttach || marker.ReplayTarget.ID != current.ID {
		return errs.New(errs.KindValidationFailed, "Attach detach marker does not match its Environment-scoped Task")
	}
	return nil
}

func attachDetachWithTaskOperationCount(record attachrecord.Record) int {
	operations := 45 + (2 * len(record.GrantAttachIDs))
	if len(record.GrantAttachIDs) != 0 {
		operations += 2
	}
	if !record.OwnsCredential() {
		operations += 3
	}
	return operations
}

func classifyAttachDetachTaskConflict(_ int64, reads []*etcdstore.KeyValue) error {
	if len(reads) < 6 {
		return errs.New(errs.KindInternal, "Attach detach Task conflict evidence is incomplete")
	}
	for _, read := range reads[:4] {
		if read != nil {
			return errs.New(errs.KindStateConflict, "Attach detach Task operation is already active")
		}
	}
	if reads[5] != nil {
		exclusion := reads[5]
		record, err := backupruntime.DecodeBackupSourceTargetExclusionRecord(exclusion.Value)
		if err != nil || record.TargetKind != backupruntime.BackupSourceTargetAttach {
			return errs.New(errs.KindInternal, "attach detach Task exclusion evidence is corrupt")
		}
		expectedKey, err := backupruntime.BackupSourceTargetExclusionKey(record.TargetKind, record.TargetID)
		if err != nil || exclusion.Key != expectedKey {
			return errs.New(errs.KindInternal, "attach detach Task exclusion evidence is misbucketed")
		}
		return errs.New(errs.KindResourceInUse, "attach is an active backup source")
	}
	return errs.New(errs.KindStateConflict, "Attach detach scope changed concurrently")
}
