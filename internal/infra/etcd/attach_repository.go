package etcd

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type AttachCreateScope struct {
	Tenant             etcdstore.Versioned[hierarchyrecord.TenantRecord]
	Project            etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	Environment        etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	DesiredHead        etcdstore.Versioned[EnvironmentBlueprintHead]
	ComposeProjection  etcdstore.Versioned[EnvironmentComposeProjection]
	Services           []etcdstore.Versioned[ServiceRecord]
	BackingProject     etcdstore.Versioned[hierarchyrecord.ProjectRecord]
	BackingEnvironment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	BackingService     etcdstore.Versioned[ServiceRecord]
	CredentialOwner    *etcdstore.Versioned[attachrecord.Record]
	Grants             []etcdstore.Versioned[attachrecord.Record]
}

type AttachRepository struct {
	store etcdstore.Store
}

type attachRemovalStore interface {
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

func NewAttachRepository(store etcdstore.Store) (*AttachRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach repository store is required")
	}
	return &AttachRepository{store: store}, nil
}

func (repository *AttachRepository) CreateAttachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	record attachrecord.Record,
	facts *attachrecord.EncryptedFacts,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.CreateAttachWithTaskHookInputs(ctx, scope, record, facts, nil, renderInput, task, marker)
}

func (repository *AttachRepository) CreateAttachWithTaskHookInputs(
	ctx context.Context,
	scope AttachCreateScope,
	record attachrecord.Record,
	facts *attachrecord.EncryptedFacts,
	hookInputs *BackingHookEncryptedInputs,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
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
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	mutationContext, err := loadOrdinaryEnvironmentMutationContext(
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
	versionedTenant, versionedProject, versionedEnvironment, err := mutationContext.versionHierarchy(
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
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	recordValue, err := attachrecord.EncodeAttachRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(recordValue)
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
	taskReference, err := encodeTaskReference(task.ID)
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
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: attachrecord.AttachKey(record.ID)},
		{Key: attachrecord.AttachNameKey(record.EnvironmentID, record.Name)},
		{Key: attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID)},
		{Key: attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID)},
		{Key: attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID)},
		{Key: hierarchyrecord.EnvironmentKey(scope.Environment.Record.ID), ModRevision: scope.Environment.Revision},
		{Key: hierarchyrecord.ProjectKey(scope.Project.Record.ID), ModRevision: scope.Project.Revision},
		{Key: hierarchyrecord.EnvironmentKey(scope.BackingEnvironment.Record.ID), ModRevision: scope.BackingEnvironment.Revision},
		{Key: hierarchyrecord.ProjectKey(scope.BackingProject.Record.ID), ModRevision: scope.BackingProject.Revision},
		{Key: deletionTombstoneKey("attach", record.ID)},
		{Key: deletionTombstoneKey("environment", record.EnvironmentID)},
		{Key: deletionTombstoneKey("project", scope.Project.Record.ID)},
		{Key: deletionTombstoneKey("tenant", scope.Project.Record.TenantID)},
		{Key: deletionTombstoneKey("environment", record.BackingEnvironmentID)},
		{Key: deletionTombstoneKey("project", record.BackingProjectID)},
		{Key: deletionTombstoneKey("service", record.BackingServiceID)},
		{Key: deletionTombstoneKey(string(DeletionTargetZone), record.BackingNetworkID)},
		{Key: attachTaskRenderInputKey(task.PlanID)},
		{Key: planReferenceKey},
		{Key: hierarchyrecord.TenantKey(scope.Tenant.Record.ID), ModRevision: scope.Tenant.Revision},
		{
			Key:         environmentBlueprintRootKey(record.EnvironmentID, renderInput.DesiredRevisionID),
			ModRevision: scope.ComposeProjection.Revision,
		},
	}
	conditions = append(conditions, desiredHeadConditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(record.ID), Value: recordValue},
		{Type: etcdstore.MutationPut, Key: attachrecord.AttachNameKey(record.EnvironmentID, record.Name), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: attachrecord.AttachOwnerKey(record.EnvironmentID, record.ID), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: attachrecord.AttachBackingServiceKey(record.BackingServiceID, record.ID), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: attachrecord.AttachBackingProjectKey(record.BackingProjectID, record.ID), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: attachTaskRenderInputKey(task.PlanID), Value: renderInputValue},
		{Type: etcdstore.MutationPut, Key: planReferenceKey, Value: planReferenceValue},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentKey(scope.Environment.Record.ID), Value: environmentValue},
	}
	for _, service := range scope.Services {
		serviceID := service.Record.Desired.ID
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachServiceKey(serviceID, record.ID)},
			etcdstore.Condition{Key: deletionTombstoneKey("service", serviceID)},
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
			etcdstore.Condition{Key: deletionTombstoneKey("attach", grantID)},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachGrantedByKey(grantID, record.ID), Value: []byte(record.ID)},
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
			etcdstore.Condition{Key: deletionTombstoneKey("attach", owner.Record.ID)},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachCredentialByKey(owner.Record.ID, record.ID),
				Value: []byte(record.ID),
			},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(owner.Record.ID), Value: credentialOwnerValue},
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
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachFactsKey(record.ID), Value: factValue})
	}
	initiation, err := newEnvironmentTaskInitiation(
		versionedTenant,
		versionedProject,
		versionedEnvironment,
		TaskActorOperator,
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

func (repository *AttachRepository) BeginAttachDetachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.BeginAttachDetachWithTaskHookInputs(ctx, scope, current, nil, renderInput, task, marker)
}

func (repository *AttachRepository) BeginAttachDetachWithTaskHookInputs(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	hookInputs *BackingHookEncryptedInputs,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.beginAttachDetachWithTask(ctx, scope, current, hookInputs, renderInput, task, marker, nil)
}

func (repository *AttachRepository) BeginAttachDetachWithTaskInitiation(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
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
	hookInputs *BackingHookEncryptedInputs,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
	initiation TaskInitiation,
) (IdempotencyTransactionResult, error) {
	return repository.beginAttachDetachWithTask(ctx, scope, current, hookInputs, renderInput, task, marker, &initiation)
}

func (repository *AttachRepository) beginAttachDetachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	current etcdstore.Versioned[attachrecord.Record],
	hookInputs *BackingHookEncryptedInputs,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
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
	if err := validateIdempotencyMarker(marker); err != nil {
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
		defer clearKeyValues(read.Values)
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
	if err := validateIdempotencyMarker(marker); err != nil {
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
	taskReference, err := encodeTaskReference(task.ID)
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
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
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
			Key:         environmentBlueprintRootKey(current.Record.EnvironmentID, renderInput.DesiredRevisionID),
			ModRevision: scope.ComposeProjection.Revision,
		},
	}
	conditions = append(conditions, desiredHeadConditions...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: taskReference},
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
			versionedTenant, versionedProject, versionedEnvironment, TaskActorOperator,
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

func (repository *AttachRepository) GetAttach(ctx context.Context, id string) (etcdstore.Versioned[attachrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindAttach, id); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		attachrecord.AttachKey(id),
		id,
		errs.KindAttachNotFound,
		attachrecord.DecodeAttachRecord,
		func(record attachrecord.Record) string { return record.ID },
	)
}

func (repository *AttachRepository) ResolveAttach(
	ctx context.Context,
	environmentID string,
	reference string,
) (etcdstore.Versioned[attachrecord.Record], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if ids.Validate(ids.KindAttach, reference) == nil {
		current, err := repository.GetAttach(ctx, reference)
		if err != nil {
			return etcdstore.Versioned[attachrecord.Record]{}, err
		}
		if current.Record.EnvironmentID != environmentID {
			return etcdstore.Versioned[attachrecord.Record]{}, errs.New(
				errs.KindScopeUnauthorized,
				"Attach is outside the Environment scope",
			)
		}
		return current, nil
	}
	if reference == "" {
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	index, err := repository.store.Get(ctx, attachrecord.AttachNameKey(environmentID, reference))
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if index == nil || index.Entry == nil {
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(ids.KindAttach, id) != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, attachrecord.CorruptAttachRecord()
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachrecord.AttachKey(id)}, Revision: index.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return etcdstore.Versioned[attachrecord.Record]{}, attachrecord.CorruptAttachRecord()
	}
	record, err := attachrecord.DecodeAttachRecord(result.Values[0].Value)
	if err != nil || record.ID != id || record.EnvironmentID != environmentID || record.Name != reference {
		return etcdstore.Versioned[attachrecord.Record]{}, attachrecord.CorruptAttachRecord()
	}
	return etcdstore.Versioned[attachrecord.Record]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *AttachRepository) ListAttaches(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[attachrecord.Record], error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Page[attachrecord.Record]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"attaches",
		"environment",
		environmentID,
		attachrecord.AttachOwnerPrefix(environmentID),
		attachrecord.AttachKey,
		ids.KindAttach,
		request,
		attachrecord.DecodeAttachRecord,
		func(record attachrecord.Record) string { return record.ID },
		func(record attachrecord.Record) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *AttachRepository) GetAttachFacts(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (attachrecord.EncryptedFacts, bool, error) {
	if err := validateAttachVersion(current); err != nil {
		return attachrecord.EncryptedFacts{}, false, err
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachrecord.AttachFactsKey(current.Record.ID)}, Revision: revision,
	})
	if err != nil {
		return attachrecord.EncryptedFacts{}, false, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		if len(current.Record.FactSets) == 0 && !current.Record.HookBundle {
			return attachrecord.EncryptedFacts{}, false, nil
		}
		return attachrecord.EncryptedFacts{}, false, errs.New(errs.KindInternal, "Attach encrypted facts are missing")
	}
	facts, err := attachrecord.DecodeAttachEncryptedFacts(result.Values[0].Value)
	if err != nil || facts.AttachID != current.Record.ID {
		clear(facts.Ciphertext)
		return attachrecord.EncryptedFacts{}, false, attachrecord.CorruptAttachRecord()
	}
	return facts, true, nil
}

func (repository *AttachRepository) ReplaceLifecycle(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
	replacement attachrecord.Record,
) (etcdstore.Versioned[attachrecord.Record], error) {
	if err := validateAttachVersion(current); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if err := attachrecord.ValidateAttachRecord(replacement); err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if !validAttachLifecycleReplacement(current.Record, replacement) {
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindStateConflict, "Attach lifecycle replacement is invalid")
	}
	conditions := []etcdstore.Condition{
		{Key: attachrecord.AttachKey(current.Record.ID), ModRevision: current.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
	}
	if replacement.Operation == attachrecord.AttachOperationDetach && replacement.Status != core.AttachFailed {
		revision := current.ReadRevision
		if revision < current.Revision {
			revision = current.Revision
		}
		exclusionCondition, err := requireAttachBackupSourceExclusionAbsent(
			ctx, repository.store, current.Record.ID, revision,
		)
		if err != nil {
			return etcdstore.Versioned[attachrecord.Record]{}, err
		}
		conditions = append(conditions, exclusionCondition)
	}
	value, err := attachrecord.EncodeAttachRecord(replacement)
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(
		ctx,
		conditions,
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(current.Record.ID), Value: value}},
	)
	if err != nil {
		return etcdstore.Versioned[attachrecord.Record]{}, err
	}
	if !result.Succeeded {
		if replacement.Operation == attachrecord.AttachOperationDetach && replacement.Status != core.AttachFailed {
			_, exclusionErr := requireAttachBackupSourceExclusionAbsent(
				ctx,
				repository.store,
				current.Record.ID,
				result.Revision,
			)
			if exclusionErr != nil {
				return etcdstore.Versioned[attachrecord.Record]{}, exclusionErr
			}
		}
		return etcdstore.Versioned[attachrecord.Record]{}, errs.New(errs.KindStateConflict, "Attach changed concurrently")
	}
	return etcdstore.Versioned[attachrecord.Record]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *AttachRepository) RenameAttachIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[attachrecord.Record],
	name string,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replacement := current.Record
	replacement.Name = name
	if err := attachrecord.ValidateAttachRecord(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environment.Record.ID != current.Record.EnvironmentID || project.Record.ID != environment.Record.ProjectID ||
		environment.Revision <= 0 || project.Revision <= 0 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindScopeUnauthorized, "Attach rename scope is invalid")
	}
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.Record.EnvironmentID || marker.ReplayTarget == nil ||
		marker.ReplayTarget.Kind != IdempotencyReplayTargetAttach || marker.ReplayTarget.ID != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Attach rename marker must be a completed Environment-scoped direct mutation",
		)
	}
	if err := validateIdempotencyMarker(marker); err != nil {
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
		project.Record.ID,
		project.Record.TenantID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	secondary, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			attachrecord.AttachNameKey(current.Record.EnvironmentID, current.Record.Name),
			attachrecord.AttachOwnerKey(current.Record.EnvironmentID, current.Record.ID),
		},
		Revision: mutationContext.readRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if len(secondary.Values) != 2 || secondary.Values[0] == nil ||
		string(secondary.Values[0].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Attach name index is missing or mismatched")
	}
	if secondary.Values[1] == nil || string(secondary.Values[1].Value) != current.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"Attach owner index is missing or mismatched",
		)
	}
	conditions := []etcdstore.Condition{
		{Key: attachrecord.AttachKey(current.Record.ID), ModRevision: current.Revision},
		{
			Key:         attachrecord.AttachNameKey(current.Record.EnvironmentID, current.Record.Name),
			ModRevision: secondary.Values[0].ModRevision,
		},
		{
			Key:         attachrecord.AttachOwnerKey(current.Record.EnvironmentID, current.Record.ID),
			ModRevision: secondary.Values[1].ModRevision,
		},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, etcdstore.Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	renaming := replacement.Name != current.Record.Name
	newNameIndex := -1
	mutations := []etcdstore.Mutation(nil)
	if renaming {
		newNameIndex = len(conditions)
		conditions = append(conditions, etcdstore.Condition{Key: attachrecord.AttachNameKey(current.Record.EnvironmentID, replacement.Name)})
		value, encodeErr := attachrecord.EncodeAttachRecord(replacement)
		if encodeErr != nil {
			return IdempotencyTransactionResult{}, encodeErr
		}
		defer clear(value)
		mutations = []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(current.Record.ID), Value: value},
			{Type: etcdstore.MutationDelete, Key: attachrecord.AttachNameKey(current.Record.EnvironmentID, current.Record.Name)},
			{
				Type:  etcdstore.MutationPut,
				Key:   attachrecord.AttachNameKey(current.Record.EnvironmentID, replacement.Name),
				Value: []byte(current.Record.ID),
			},
		}
	}
	originalClassify := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Attach rename compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.New(errs.KindAttachNotFound, "Attach was not found")
		}
		if newNameIndex >= 0 && values[newNameIndex] != nil {
			return errs.New(errs.KindNameConflict, "Attach name already exists")
		}
		if values[5] != nil || values[6] != nil || values[7] != nil ||
			project.Record.TenantID != "" && values[8] != nil {
			return errs.New(errs.KindResourceInUse, "Attach ownership deletion is in progress")
		}
		if values[0].ModRevision != current.Revision {
			return errs.New(errs.KindStateConflict, "Attach changed concurrently")
		}
		if values[1] == nil || string(values[1].Value) != current.Record.ID ||
			values[2] == nil || string(values[2].Value) != current.Record.ID {
			return errs.New(errs.KindInternal, "Attach indexes are missing or mismatched")
		}
		return errs.New(errs.KindStateConflict, "Attach rename scope changed concurrently")
	}
	binding, err := mutationContext.bind(ctx, repository.store, conditions, mutations, renaming)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer binding.clear()
	defer clearMutationValues(binding.mutations)
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		return binding.classify(revision, values, originalClassify)
	}
	plan, err := newIdempotencyMutationPlan(binding.conditions, binding.mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *AttachRepository) DeleteDetachedAttach(
	ctx context.Context,
	current etcdstore.Versioned[attachrecord.Record],
) (int64, error) {
	if err := validateAttachVersion(current); err != nil {
		return 0, err
	}
	if current.Record.Status != core.AttachDetached {
		return 0, errs.New(errs.KindStateConflict, "Attach must be detached before record removal")
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	conditions, mutations, values, err := prepareAttachRemoval(ctx, repository.store, current, revision)
	if err != nil {
		return 0, err
	}
	defer func() {
		for _, value := range values {
			clear(value)
		}
	}()
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, errs.New(errs.KindStateConflict, "Attach references changed concurrently")
	}
	return result.Revision, nil
}

func prepareAttachRemoval(
	ctx context.Context,
	store attachRemovalStore,
	current etcdstore.Versioned[attachrecord.Record],
	revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, [][]byte, error) {
	if err := validateAttachVersion(current); err != nil {
		return nil, nil, nil, err
	}
	if revision <= 0 {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Attach removal revision must be positive")
	}
	exclusionCondition, err := requireAttachBackupSourceExclusionAbsent(
		ctx, store, current.Record.ID, revision,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	dependents, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachGrantedByPrefix(current.Record.ID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if dependents == nil || dependents.ReadRevision != revision {
		return nil, nil, nil, errs.New(errs.KindInternal, "Attach grant index read returned an invalid revision")
	}
	if len(dependents.Values) != 0 {
		return nil, nil, nil, errs.New(errs.KindResourceInUse, "Attach is referenced by another Attach grant")
	}
	if dependents != nil {
		defer clearRangeValues(dependents.Values)
	}
	credentialDependents, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachCredentialByPrefix(current.Record.ID), Limit: 1, Revision: revision,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if credentialDependents == nil || credentialDependents.ReadRevision != revision {
		return nil, nil, nil, errs.New(errs.KindInternal, "Attach credential index read returned an invalid revision")
	}
	if len(credentialDependents.Values) != 0 {
		return nil, nil, nil, errs.New(errs.KindResourceInUse, "Attach credential is used by another Service")
	}
	defer clearRangeValues(credentialDependents.Values)
	dependentGrants, err := store.Range(ctx, etcdstore.RangeRequest{
		Prefix: attachrecord.AttachDependentGrantPrefix(current.Record.ID),
		Limit:  2, Revision: revision,
	})
	if err != nil {
		return nil, nil, nil, err
	}
	if dependentGrants == nil || dependentGrants.ReadRevision != revision ||
		dependentGrants.More || len(dependentGrants.Values) > 1 {
		return nil, nil, nil, attachrecord.CorruptAttachRecord()
	}
	defer clearRangeValues(dependentGrants.Values)
	if err := attachrecord.ValidateAttachDependentGrantRange(
		dependentGrants, current.Record.ID, current.Record.GrantAttachIDs,
	); err != nil {
		return nil, nil, nil, err
	}
	if (len(current.Record.GrantAttachIDs) == 0 && len(dependentGrants.Values) != 0) ||
		(len(current.Record.GrantAttachIDs) != 0 && len(dependentGrants.Values) != 1) {
		return nil, nil, nil, attachrecord.CorruptAttachRecord()
	}

	conditions := []etcdstore.Condition{
		{Key: attachrecord.AttachKey(current.Record.ID), ModRevision: current.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
		exclusionCondition,
		{Key: attachrecord.AttachGrantedByPrefix(current.Record.ID), Prefix: true},
		{Key: attachrecord.AttachCredentialByPrefix(current.Record.ID), Prefix: true},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachKey(current.Record.ID)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachNameKey(current.Record.EnvironmentID, current.Record.Name)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachOwnerKey(current.Record.EnvironmentID, current.Record.ID)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachBackingServiceKey(current.Record.BackingServiceID, current.Record.ID)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachBackingProjectKey(current.Record.BackingProjectID, current.Record.ID)},
		{Type: etcdstore.MutationDelete, Key: attachrecord.AttachFactsKey(current.Record.ID)},
	}
	mutations = append(mutations, etcdstore.Mutation{
		Type: etcdstore.MutationDelete, Key: attachrecord.AttachServiceKey(current.Record.ServiceID, current.Record.ID),
	})
	if len(current.Record.GrantAttachIDs) != 0 {
		dependent := dependentGrants.Values[0]
		conditions = append(conditions, etcdstore.Condition{
			Key: attachrecord.AttachDependentGrantKey(current.Record.ID), ModRevision: dependent.ModRevision,
		})
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationDelete, Key: attachrecord.AttachDependentGrantKey(current.Record.ID),
		})
	}
	values := make([][]byte, 0, len(current.Record.GrantAttachIDs)+1)
	if !current.Record.OwnsCredential() {
		keys := []string{
			attachrecord.AttachKey(current.Record.CredentialAttachID),
			attachrecord.AttachCredentialByKey(current.Record.CredentialAttachID, current.Record.ID),
		}
		result, readErr := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
		if readErr != nil {
			return nil, nil, values, readErr
		}
		if result == nil || result.ReadRevision != revision || len(result.Values) != 2 ||
			result.Values[0] == nil || result.Values[1] == nil ||
			string(result.Values[1].Value) != current.Record.ID {
			return nil, nil, values, attachrecord.CorruptAttachRecord()
		}
		owner, decodeErr := attachrecord.DecodeAttachRecord(result.Values[0].Value)
		if decodeErr != nil || owner.ID != current.Record.CredentialAttachID || !owner.OwnsCredential() {
			return nil, nil, values, attachrecord.CorruptAttachRecord()
		}
		ownerValue, encodeErr := attachrecord.EncodeAttachRecord(owner)
		if encodeErr != nil {
			return nil, nil, values, encodeErr
		}
		values = append(values, ownerValue)
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(owner.ID), ModRevision: result.Values[0].ModRevision},
			etcdstore.Condition{Key: keys[1], ModRevision: result.Values[1].ModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: keys[1]},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(owner.ID), Value: ownerValue},
		)
		clearKeyValues(result.Values)
	}
	if len(current.Record.GrantAttachIDs) == 0 {
		return conditions, mutations, values, nil
	}
	keys := make([]string, 0, len(current.Record.GrantAttachIDs)*2)
	for _, grantID := range current.Record.GrantAttachIDs {
		keys = append(keys, attachrecord.AttachKey(grantID), attachrecord.AttachGrantedByKey(grantID, current.Record.ID))
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return nil, nil, nil, attachrecord.CorruptAttachRecord()
	}
	for index, grantID := range current.Record.GrantAttachIDs {
		target := result.Values[index*2]
		reverse := result.Values[index*2+1]
		if target == nil || reverse == nil || string(reverse.Value) != current.Record.ID {
			return nil, nil, values, attachrecord.CorruptAttachRecord()
		}
		targetRecord, decodeErr := attachrecord.DecodeAttachRecord(target.Value)
		if decodeErr != nil || targetRecord.ID != grantID {
			return nil, nil, values, attachrecord.CorruptAttachRecord()
		}
		targetValue, encodeErr := attachrecord.EncodeAttachRecord(targetRecord)
		if encodeErr != nil {
			return nil, nil, values, encodeErr
		}
		values = append(values, targetValue)
		conditions = append(conditions,
			etcdstore.Condition{Key: attachrecord.AttachKey(grantID), ModRevision: target.ModRevision},
			etcdstore.Condition{Key: attachrecord.AttachGrantedByKey(grantID, current.Record.ID), ModRevision: reverse.ModRevision},
		)
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: attachrecord.AttachGrantedByKey(grantID, current.Record.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: attachrecord.AttachKey(grantID), Value: targetValue},
		)
	}
	return conditions, mutations, values, nil
}

func requireAttachBackupSourceExclusionAbsent(
	ctx context.Context,
	store interface {
		GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	},
	attachID string,
	revision int64,
) (etcdstore.Condition, error) {
	key, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, attachID)
	if err != nil {
		return etcdstore.Condition{}, err
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return etcdstore.Condition{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != 1 {
		return etcdstore.Condition{}, errs.New(errs.KindInternal, "attach backup source exclusion read is incomplete")
	}
	if result.Values[0] != nil {
		if evidenceErr := classifyAttachBackupSourceExclusionEvidence(result.Values[0], attachID); evidenceErr != nil {
			return etcdstore.Condition{}, evidenceErr
		}
	}
	return etcdstore.Condition{Key: key}, nil
}

func classifyAttachBackupSourceExclusionEvidence(evidence *etcdstore.KeyValue, attachID string) error {
	if evidence == nil {
		return nil
	}
	expectedKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, attachID)
	if err != nil || evidence.Key != expectedKey {
		return errs.New(errs.KindInternal, "attach backup source exclusion is misbucketed")
	}
	exclusion, decodeErr := decodeBackupSourceTargetExclusionRecord(evidence.Value)
	if decodeErr != nil || exclusion.TargetKind != BackupSourceTargetAttach || exclusion.TargetID != attachID {
		return errs.New(errs.KindInternal, "attach backup source exclusion is corrupt")
	}
	return errs.New(errs.KindResourceInUse, "attach is an active backup source")
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
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID || scope.Project.Record.Kind != hierarchyrecord.ProjectKindTenant ||
		scope.Project.Record.TenantID == "" ||
		scope.Environment.Record.ProjectID != scope.Project.Record.ID ||
		record.EnvironmentID != scope.Environment.Record.ID {
		return errs.New(errs.KindScopeUnauthorized, "Attach consumer hierarchy is invalid")
	}
	if scope.BackingProject.Record.Kind != hierarchyrecord.ProjectKindBacking || scope.BackingProject.Record.TenantID != "" ||
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

func validateAttachCreationTask(record attachrecord.Record, task TaskRecord, marker IdempotencyMarker) error {
	pendingAttachOwned := record.Status == core.AttachPending && record.Operation == attachrecord.AttachOperationProvision &&
		record.TaskID == task.ID && record.CreatedAt.Equal(task.CreatedAt)
	validTaskShape := task.Type == TaskAttach && task.Target == record.ID && task.Executor == TaskExecutorAgent &&
		task.Status == TaskStatusPending && len(task.Params) == 1 && len(task.Materializations) == 0 &&
		task.Params[TaskMutationEnvironmentParam] == record.EnvironmentID
	if !pendingAttachOwned || !validTaskShape {
		return errs.New(errs.KindValidationFailed, "Attach creation Task does not own its pending Attach")
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != record.EnvironmentID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || marker.ReplayTarget != nil {
		return errs.New(
			errs.KindValidationFailed,
			"Attach creation marker does not match its Environment-scoped Task",
		)
	}
	return nil
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
	marker IdempotencyMarker,
) error {
	validOwnership := detaching.Status == core.AttachDetaching && detaching.Operation == attachrecord.AttachOperationDetach &&
		detaching.TaskID == task.ID && attachImmutableEqual(current, detaching)
	validTaskShape := task.Type == TaskDetach && task.Target == current.ID && task.Executor == TaskExecutorAgent &&
		task.Status == TaskStatusPending && len(task.Params) == 1 && len(task.Materializations) == 0 &&
		task.Params[TaskMutationEnvironmentParam] == current.EnvironmentID
	if !validOwnership || !validTaskShape {
		return errs.New(errs.KindValidationFailed, "Attach detach Task does not own its detaching Attach")
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != current.EnvironmentID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || marker.ReplayTarget == nil ||
		marker.ReplayTarget.Kind != IdempotencyReplayTargetAttach || marker.ReplayTarget.ID != current.ID {
		return errs.New(errs.KindValidationFailed, "Attach detach marker does not match its Environment-scoped Task")
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

func validAttachLifecycleReplacement(current attachrecord.Record, replacement attachrecord.Record) bool {
	if !attachImmutableEqual(current, replacement) {
		return false
	}
	switch {
	case current.Status == core.AttachPending && replacement.Status == core.AttachProvisioning:
		return current.Operation == attachrecord.AttachOperationProvision && replacement.Operation == current.Operation &&
			replacement.TaskID == current.TaskID
	case current.Status == core.AttachProvisioning &&
		(replacement.Status == core.AttachReady || replacement.Status == core.AttachFailed):
		return replacement.Operation == current.Operation && replacement.TaskID == current.TaskID
	case current.Status == core.AttachFailed && current.Operation == attachrecord.AttachOperationProvision &&
		replacement.Status == core.AttachPending:
		return replacement.Operation == current.Operation && replacement.TaskID != current.TaskID
	case (current.Status == core.AttachReady || current.Status == core.AttachFailed) &&
		replacement.Status == core.AttachDetaching:
		return replacement.Operation == attachrecord.AttachOperationDetach && replacement.TaskID != current.TaskID
	case current.Status == core.AttachDetaching &&
		(replacement.Status == core.AttachDetached || replacement.Status == core.AttachFailed):
		return replacement.Operation == current.Operation && replacement.TaskID == current.TaskID
	case current.Status == core.AttachFailed && current.Operation == attachrecord.AttachOperationDetach &&
		replacement.Status == core.AttachDetaching:
		return replacement.Operation == current.Operation && replacement.TaskID != current.TaskID
	default:
		return false
	}
}

func attachImmutableEqual(left attachrecord.Record, right attachrecord.Record) bool {
	if left.ID != right.ID || left.EnvironmentID != right.EnvironmentID || left.Name != right.Name ||
		left.BackingProjectID != right.BackingProjectID || left.BackingEnvironmentID != right.BackingEnvironmentID ||
		left.BackingServiceID != right.BackingServiceID || left.BackingNetworkID != right.BackingNetworkID ||
		left.ServiceID != right.ServiceID || left.CredentialAttachID != right.CredentialAttachID ||
		left.HookBundle != right.HookBundle ||
		!left.CreatedAt.Equal(right.CreatedAt) ||
		!slices.Equal(left.GrantAttachIDs, right.GrantAttachIDs) ||
		len(left.FactSets) != len(right.FactSets) {
		return false
	}
	for index := range left.FactSets {
		if left.FactSets[index].GrantAttachID != right.FactSets[index].GrantAttachID ||
			!slices.Equal(left.FactSets[index].Facts, right.FactSets[index].Facts) {
			return false
		}
	}
	return true
}

func validateAttachVersion(current etcdstore.Versioned[attachrecord.Record]) error {
	if current.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach record revision must be positive")
	}
	return attachrecord.ValidateAttachRecord(current.Record)
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
		record, err := decodeBackupSourceTargetExclusionRecord(exclusion.Value)
		if err != nil || record.TargetKind != BackupSourceTargetAttach {
			return errs.New(errs.KindInternal, "attach detach Task exclusion evidence is corrupt")
		}
		expectedKey, err := backupSourceTargetExclusionKey(record.TargetKind, record.TargetID)
		if err != nil || exclusion.Key != expectedKey {
			return errs.New(errs.KindInternal, "attach detach Task exclusion evidence is misbucketed")
		}
		return errs.New(errs.KindResourceInUse, "attach is an active backup source")
	}
	return errs.New(errs.KindStateConflict, "Attach detach scope changed concurrently")
}
