package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

func (repository *AttachRepository) CreateAttachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	record attachrecord.Record,
	facts *attachrecord.EncryptedFacts,
	renderInput attachrender.AttachTaskRenderInput,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.CreateAttachWithTaskHookInputs(ctx, scope, record, facts, nil, renderInput, task, marker)
}

func (repository *AttachRepository) CreateAttachWithTaskHookInputs(
	ctx context.Context,
	scope AttachCreateScope,
	record attachrecord.Record,
	facts *attachrecord.EncryptedFacts,
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
	renderInput attachrender.AttachTaskRenderInput,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (_ IdempotencyTransactionResult, returnErr error) {
	if err := validateAttachCreateScope(ctx, scope, record, facts); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachCreationTask(record, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachTaskRenderInputScope(scope, record, task, renderInput); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	mutationContext, err := environmentfence.LoadMutationContext(
		ctx,
		repository.store,
		record.EnvironmentID,
		attachrecord.AttachKey(record.ID),
		scope.Project.Record.ID,
		scope.Project.Record.TenantID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachRuntimeEpoch(mutationContext, record.EnvironmentID, renderInput); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	versionedTenant, versionedProject, versionedEnvironment, err := mutationContext.VersionHierarchy(
		&scope.Tenant,
		scope.Project,
		scope.Environment,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if attachCreateWithTaskOperationCount(record, facts != nil) > etcdstore.MaximumOperations {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Attach consumer and grant combination exceeds the atomic transaction limit",
		)
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
	if err := ValidateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	recordValue, err := attachrecord.EncodeAttachRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(recordValue)
	renderInputValue, err := attachrender.EncodeAttachTaskRenderInput(renderInput)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(renderInputValue)
	environmentValue, err := hierarchyrecord.EncodeEnvironment(scope.Environment.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(environmentValue)
	taskValue, err := EncodeTaskRecord(task)
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
		record.EnvironmentID,
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
		{Key: attachrecord.AttachKey(record.ID)},
		{Key: attachrecord.AttachNameKey(record.EnvironmentID, record.Name)},
		{Key: attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID)},
		{Key: attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID)},
		{Key: attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID)},
		{Key: hierarchyrecord.EnvironmentKey(scope.Environment.Record.ID), ModRevision: scope.Environment.Revision},
		{Key: hierarchyrecord.ProjectKey(scope.Project.Record.ID), ModRevision: scope.Project.Revision},
		{
			Key:         hierarchyrecord.EnvironmentKey(scope.BackingEnvironment.Record.ID),
			ModRevision: scope.BackingEnvironment.Revision,
		},
		{Key: hierarchyrecord.ProjectKey(scope.BackingProject.Record.ID), ModRevision: scope.BackingProject.Revision},
		{Key: deletionrecord.TombstoneKey("attach", record.ID)},
		{Key: deletionrecord.TombstoneKey("environment", record.EnvironmentID)},
		{Key: deletionrecord.TombstoneKey("project", scope.Project.Record.ID)},
		{Key: deletionrecord.TombstoneKey("tenant", scope.Project.Record.TenantID)},
		{Key: deletionrecord.TombstoneKey("environment", record.BackingEnvironmentID)},
		{Key: deletionrecord.TombstoneKey("project", record.BackingProjectID)},
		{Key: deletionrecord.TombstoneKey("service", record.BackingServiceID)},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), record.BackingNetworkID)},
		{Key: attachrender.AttachTaskRenderInputKey(task.PlanID)},
		{Key: planReferenceKey},
		{Key: hierarchyrecord.TenantKey(scope.Tenant.Record.ID), ModRevision: scope.Tenant.Revision},
		{
			Key:         blueprints.EnvironmentBlueprintRootKey(record.EnvironmentID, renderInput.DesiredRevisionID),
			ModRevision: scope.ComposeProjection.Revision,
		},
	}
	conditions = append(conditions, desiredHeadConditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: taskReference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(record.ID), Value: recordValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   attachrecord.AttachNameKey(record.EnvironmentID, record.Name),
			Value: []byte(record.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID),
			Value: []byte(record.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID),
			Value: []byte(record.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID),
			Value: []byte(record.ID),
		},
		{Type: etcdstore.MutationPut, Key: attachrender.AttachTaskRenderInputKey(task.PlanID), Value: renderInputValue},
		{Type: etcdstore.MutationPut, Key: planReferenceKey, Value: planReferenceValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentKey(scope.Environment.Record.ID),
			Value: environmentValue,
		},
	}
	for _, service := range scope.Services {
		serviceID := service.Record.Desired.ID
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachServiceKey(serviceID, record.ID)},
			etcdstore.Condition{Key: deletionrecord.TombstoneKey("service", serviceID)},
		)
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: attachrecord.AttachServiceKey(serviceID, record.ID), Value: []byte(record.ID),
		})
	}
	grantValues := make([][]byte, 0, len(scope.Grants))
	defer func() {
		for _, value := range grantValues {
			clear(value)
		}
	}()
	for _, grant := range scope.Grants {
		grantID := grant.Record.ID
		grantValue, encodeErr := attachrecord.EncodeAttachRecord(grant.Record)
		if encodeErr != nil {
			return IdempotencyTransactionResult{}, encodeErr
		}
		grantValues = append(grantValues, grantValue)
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(grantID), ModRevision: grant.Revision},
			etcdstore.Condition{Key: attachrecord.AttachGrantedByKey(grantID, record.ID)},
			etcdstore.Condition{Key: deletionrecord.TombstoneKey("attach", grantID)},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachGrantedByKey(grantID, record.ID),
				Value: []byte(record.ID),
			},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(grantID), Value: grantValue},
		)
	}
	var credentialOwnerValue []byte
	if !record.OwnsCredential() {
		owner := *scope.CredentialOwner
		credentialOwnerValue, err = attachrecord.EncodeAttachRecord(owner.Record)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(credentialOwnerValue)
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(owner.Record.ID), ModRevision: owner.Revision},
			etcdstore.Condition{Key: attachrecord.AttachCredentialByKey(owner.Record.ID, record.ID)},
			etcdstore.Condition{Key: deletionrecord.TombstoneKey("attach", owner.Record.ID)},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachCredentialByKey(owner.Record.ID, record.ID),
				Value: []byte(record.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachKey(owner.Record.ID),
				Value: credentialOwnerValue,
			},
		)
	}
	if len(record.GrantAttachIDs) != 0 {
		dependentGrantValue, encodeErr := attachrecord.EncodeAttachDependentGrantIndex(
			record.ID, record.GrantAttachIDs,
		)
		if encodeErr != nil {
			return IdempotencyTransactionResult{}, encodeErr
		}
		defer clear(dependentGrantValue)
		conditions = append(conditions, etcdstore.Condition{Key: attachrecord.AttachDependentGrantKey(record.ID)})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: attachrecord.AttachDependentGrantKey(record.ID), Value: dependentGrantValue,
		})
	}
	var factValue []byte
	if facts != nil {
		factValue, err = attachrecord.EncodeAttachEncryptedFacts(*facts)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(factValue)
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachFactsKey(record.ID),
				Value: factValue,
			},
		)
	}
	initiation, err := newEnvironmentTaskInitiation(
		versionedTenant,
		versionedProject,
		versionedEnvironment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, mutations, classify, err := publication.bind(
		conditions, mutations, classifyAttachTaskCreateConflict,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return repository.publishAttachRuntimeTask(ctx, task, renderInput, initiation, marker,
		mutationContext, conditions, mutations, classify)
}

func validateAttachCreateScope(
	ctx context.Context,
	scope AttachCreateScope,
	record attachrecord.Record,
	facts *attachrecord.EncryptedFacts,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := attachrecord.ValidateAttachRecord(record); err != nil {
		return err
	}
	if scope.Tenant.Revision <= 0 || scope.Project.Revision <= 0 || scope.Environment.Revision <= 0 ||
		scope.DesiredHead.Revision <= 0 || scope.ComposeProjection.Revision <= 0 ||
		scope.BackingProject.Revision <= 0 ||
		scope.BackingEnvironment.Revision <= 0 || scope.BackingService.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach scope records must be versioned")
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID ||
		scope.Project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		scope.Project.Record.TenantID == "" ||
		scope.Environment.Record.ProjectID != scope.Project.Record.ID ||
		record.EnvironmentID != scope.Environment.Record.ID {
		return errs.New(errs.KindScopeUnauthorized, "Attach consumer hierarchy is invalid")
	}
	if scope.BackingProject.Record.Kind != hierarchyrecord.ProjectKindBacking ||
		scope.BackingProject.Record.TenantID != "" ||
		scope.BackingEnvironment.Record.ProjectID != scope.BackingProject.Record.ID ||
		scope.BackingService.Record.EnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingProjectID != scope.BackingProject.Record.ID ||
		record.BackingEnvironmentID != scope.BackingEnvironment.Record.ID ||
		record.BackingServiceID != scope.BackingService.Record.Desired.ID ||
		record.BackingNetworkID != scope.BackingService.Record.BackingNetworkID {
		return errs.New(errs.KindScopeUnauthorized, "Attach backing hierarchy is invalid")
	}
	serviceIDs := make([]string, 0, len(scope.Services))
	for _, service := range scope.Services {
		if service.Revision <= 0 || service.Record.EnvironmentID != record.EnvironmentID {
			return errs.New(errs.KindScopeUnauthorized, "Attach consuming Service is outside the Environment")
		}
		serviceIDs = append(serviceIDs, service.Record.Desired.ID)
	}
	slices.Sort(serviceIDs)
	if len(serviceIDs) != 1 || serviceIDs[0] != record.ServiceID {
		return errs.New(errs.KindValidationFailed, "Attach service_id does not match the resolved Service")
	}
	grantIDs := make([]string, 0, len(scope.Grants))
	for _, grant := range scope.Grants {
		if grant.Revision <= 0 || grant.Record.EnvironmentID != record.EnvironmentID ||
			grant.Record.BackingServiceID != record.BackingServiceID ||
			grant.Record.BackingNetworkID != record.BackingNetworkID || grant.Record.Status != core.AttachReady {
			return errs.New(
				errs.KindScopeUnauthorized,
				"Attach grant must be ready in the same Environment and backing Service",
			)
		}
		grantIDs = append(grantIDs, grant.Record.ID)
	}
	slices.Sort(grantIDs)
	if !slices.Equal(grantIDs, record.GrantAttachIDs) {
		return errs.New(errs.KindValidationFailed, "Attach grant ids do not match the resolved grant records")
	}
	if record.OwnsCredential() {
		if scope.CredentialOwner != nil {
			return errs.New(errs.KindValidationFailed, "Credential-owning Attach cannot reference another owner")
		}
	} else {
		owner := scope.CredentialOwner
		if owner == nil || owner.Revision <= 0 || owner.Record.ID != record.CredentialAttachID ||
			!owner.Record.OwnsCredential() || owner.Record.Status != core.AttachReady ||
			owner.Record.EnvironmentID != record.EnvironmentID ||
			owner.Record.BackingServiceID != record.BackingServiceID ||
			owner.Record.BackingNetworkID != record.BackingNetworkID || facts != nil ||
			len(record.GrantAttachIDs) != 0 || !attachrecord.AttachFactSetsEqual(record.FactSets, owner.Record.FactSets) {
			return errs.New(errs.KindScopeUnauthorized, "Attach existing credential owner is invalid")
		}
	}
	custom := scope.BackingService.Record.Desired.Adapter == "custom"
	if custom {
		hooks := scope.BackingService.Record.Desired.Hooks
		hooked := record.OwnsCredential() && hooks != nil && (hooks.Attach != nil || hooks.Detach != nil)
		if record.HookBundle != hooked || (facts != nil) != hooked || len(record.GrantAttachIDs) != 0 {
			return errs.New(
				errs.KindAdapterCustomOnly,
				"Custom Attach hook bundle does not match its backing Service",
			)
		}
		if hooked && facts.AttachID != record.ID {
			return errs.New(errs.KindValidationFailed, "Custom Attach hook bundle belongs to another Attach")
		}
		if !hooked && len(record.FactSets) != 0 {
			return errs.New(errs.KindAdapterCustomOnly, "Custom Attach without hooks cannot publish facts")
		}
		if hooked {
			return attachrecord.ValidateAttachEncryptedFacts(*facts)
		}
		return nil
	}
	if !record.OwnsCredential() {
		return nil
	}
	if scope.BackingService.Record.Desired.Adapter == "" {
		return errs.New(errs.KindValidationFailed, "Attach backing Service requires an adapter")
	}
	if facts == nil || len(record.FactSets) == 0 {
		return errs.New(errs.KindValidationFailed, "Adapter Attach requires encrypted facts and fact metadata")
	}
	if facts.AttachID != record.ID {
		return errs.New(errs.KindValidationFailed, "Attach fact envelope belongs to another Attach")
	}
	return attachrecord.ValidateAttachEncryptedFacts(*facts)
}

func validateAttachCreationTask(
	record attachrecord.Record,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	pendingAttachOwned := record.Status == core.AttachPending &&
		record.Operation == attachrecord.AttachOperationProvision &&
		record.TaskID == task.ID &&
		record.CreatedAt.Equal(task.CreatedAt)
	validTaskShape := task.Type == taskjournal.TaskAttach && task.Target == record.ID &&
		task.Executor == taskjournal.TaskExecutorAgent &&
		task.Status == taskjournal.TaskStatusPending &&
		len(task.Params) == 1 &&
		len(task.Materializations) == 0 &&
		task.Params[taskjournal.TaskMutationEnvironmentParam] == record.EnvironmentID
	if !pendingAttachOwned || !validTaskShape {
		return errs.New(errs.KindValidationFailed, "Attach creation Task does not own its pending Attach")
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID ||
		marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != record.EnvironmentID ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) ||
		marker.ReplayTarget != nil {
		return errs.New(
			errs.KindValidationFailed,
			"Attach creation marker does not match its Environment-scoped Task",
		)
	}
	return nil
}

func attachCreateWithTaskOperationCount(record attachrecord.Record, hasFacts bool) int {
	operations := 52 + (5 * len(record.GrantAttachIDs))
	if len(record.GrantAttachIDs) != 0 {
		operations += 2
	}
	if hasFacts {
		operations++
	}
	if !record.OwnsCredential() {
		operations += 5
	}
	return operations
}

func classifyAttachCreateConflict(reads []*etcdstore.KeyValue) error {
	if len(reads) > 1 && reads[1] != nil {
		return errs.New(errs.KindNameConflict, "Attach name already exists")
	}
	return errs.New(errs.KindStateConflict, "Attach hierarchy changed concurrently")
}

func classifyAttachTaskCreateConflict(_ int64, reads []*etcdstore.KeyValue) error {
	if len(reads) < 22 {
		return errs.New(errs.KindInternal, "Attach Task conflict evidence is incomplete")
	}
	for _, read := range reads[:4] {
		if read != nil {
			return errs.New(errs.KindStateConflict, "Attach Task operation is already active")
		}
	}
	if reads[21] != nil {
		return errs.New(errs.KindResourceInUse, "backing Zone removal is in progress")
	}
	return classifyAttachCreateConflict(reads[4:])
}
