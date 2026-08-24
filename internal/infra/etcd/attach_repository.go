package etcd

import (
	"context"
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const attachRecordPrefix = "/v1/records/attaches/"

type AttachCreateScope struct {
	Tenant             Versioned[TenantRecord]
	Project            Versioned[ProjectRecord]
	Environment        Versioned[EnvironmentRecord]
	BlueprintRevision  Versioned[EnvironmentBlueprintRevision]
	ComposeProjection  Versioned[EnvironmentComposeProjection]
	Services           []Versioned[ServiceRecord]
	BackingProject     Versioned[ProjectRecord]
	BackingEnvironment Versioned[EnvironmentRecord]
	BackingService     Versioned[ServiceRecord]
	Grants             []Versioned[AttachRecord]
}

type AttachRepository struct {
	store Store
}

type attachRemovalStore interface {
	Range(context.Context, RangeRequest) (*RangeResult, error)
	GetMany(context.Context, GetManyRequest) (*GetManyResult, error)
}

func NewAttachRepository(store Store) (*AttachRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach repository store is required")
	}
	return &AttachRepository{store: store}, nil
}

func (repository *AttachRepository) CreateAttachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	record AttachRecord,
	facts *AttachEncryptedFacts,
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateAttachCreateScope(ctx, scope, record, facts); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachCreationTask(record, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachTaskRenderInputScope(scope, record, task, renderInput); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if attachCreateWithTaskOperationCount(record, facts != nil) > maximumTransactionOperations {
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
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	recordValue, err := encodeAttachRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(recordValue)
	renderInputValue, err := encodeAttachTaskRenderInput(renderInput)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(renderInputValue)
	environmentValue, err := encodeEnvironment(scope.Environment.Record)
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

	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: attachKey(record.ID)},
		{Key: attachNameKey(record.EnvironmentID, record.Name)},
		{Key: attachOwnerKey(record.EnvironmentID, record.ID)},
		{Key: attachBackingServiceKey(record.BackingServiceID, record.ID)},
		{Key: attachBackingProjectKey(record.BackingProjectID, record.ID)},
		{Key: environmentKey(scope.Environment.Record.ID), ModRevision: scope.Environment.Revision},
		{Key: projectKey(scope.Project.Record.ID), ModRevision: scope.Project.Revision},
		{Key: environmentKey(scope.BackingEnvironment.Record.ID), ModRevision: scope.BackingEnvironment.Revision},
		{Key: projectKey(scope.BackingProject.Record.ID), ModRevision: scope.BackingProject.Revision},
		{Key: serviceKey(scope.BackingService.Record.Desired.ID), ModRevision: scope.BackingService.Revision},
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
		{Key: tenantKey(scope.Tenant.Record.ID), ModRevision: scope.Tenant.Revision},
		{
			Key:         environmentBlueprintManifestKey(record.EnvironmentID, renderInput.BlueprintRevisionID),
			ModRevision: scope.BlueprintRevision.Revision,
		},
		{
			Key:         environmentComposeProjectionKey(record.EnvironmentID),
			ModRevision: scope.ComposeProjection.Revision,
		},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: taskReference},
		{Type: MutationPut, Key: attachKey(record.ID), Value: recordValue},
		{Type: MutationPut, Key: attachNameKey(record.EnvironmentID, record.Name), Value: []byte(record.ID)},
		{Type: MutationPut, Key: attachOwnerKey(record.EnvironmentID, record.ID), Value: []byte(record.ID)},
		{Type: MutationPut, Key: attachBackingServiceKey(record.BackingServiceID, record.ID), Value: []byte(record.ID)},
		{Type: MutationPut, Key: attachBackingProjectKey(record.BackingProjectID, record.ID), Value: []byte(record.ID)},
		{Type: MutationPut, Key: attachTaskRenderInputKey(task.PlanID), Value: renderInputValue},
		{Type: MutationPut, Key: planReferenceKey, Value: planReferenceValue},
		{Type: MutationPut, Key: environmentKey(scope.Environment.Record.ID), Value: environmentValue},
	}
	for _, service := range scope.Services {
		serviceID := service.Record.Desired.ID
		conditions = append(conditions,
			Condition{Key: serviceKey(serviceID), ModRevision: service.Revision},
			Condition{Key: attachServiceKey(serviceID, record.ID)},
			Condition{Key: deletionTombstoneKey("service", serviceID)},
		)
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: attachServiceKey(serviceID, record.ID), Value: []byte(record.ID),
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
		grantValue, encodeErr := encodeAttachRecord(grant.Record)
		if encodeErr != nil {
			return IdempotencyTransactionResult{}, encodeErr
		}
		grantValues = append(grantValues, grantValue)
		conditions = append(conditions,
			Condition{Key: attachKey(grantID), ModRevision: grant.Revision},
			Condition{Key: attachGrantedByKey(grantID, record.ID)},
			Condition{Key: deletionTombstoneKey("attach", grantID)},
		)
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: attachGrantedByKey(grantID, record.ID), Value: []byte(record.ID)},
			Mutation{Type: MutationPut, Key: attachKey(grantID), Value: grantValue},
		)
	}
	var factValue []byte
	if facts != nil {
		factValue, err = encodeAttachEncryptedFacts(*facts)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(factValue)
		mutations = append(mutations, Mutation{Type: MutationPut, Key: attachFactsKey(record.ID), Value: factValue})
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, scope.Project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, scope.Project, scope.Environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyAttachTaskCreateConflict,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *AttachRepository) BeginAttachDetachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	current Versioned[AttachRecord],
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.beginAttachDetachWithTask(ctx, scope, current, renderInput, task, marker, nil)
}

func (repository *AttachRepository) BeginAttachDetachWithTaskInitiation(
	ctx context.Context,
	scope AttachCreateScope,
	current Versioned[AttachRecord],
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
	initiation TaskInitiation,
) (IdempotencyTransactionResult, error) {
	return repository.beginAttachDetachWithTask(ctx, scope, current, renderInput, task, marker, &initiation)
}

func (repository *AttachRepository) beginAttachDetachWithTask(
	ctx context.Context,
	scope AttachCreateScope,
	current Versioned[AttachRecord],
	renderInput AttachTaskRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
	provided *TaskInitiation,
) (IdempotencyTransactionResult, error) {
	if err := validateAttachDetachScope(ctx, scope, current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	detaching, err := BeginAttachDetaching(current.Record, task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachDetachTask(current.Record, detaching, task, marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachTaskRenderInputScope(scope, detaching, task, renderInput); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if attachDetachWithTaskOperationCount(detaching) > maximumTransactionOperations {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Attach detach combination exceeds the atomic transaction limit",
		)
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	dependents, err := repository.store.Range(ctx, RangeRequest{
		Prefix: attachGrantedByPrefix(current.Record.ID), Limit: 1, Revision: revision,
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

	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	attachValue, err := encodeAttachRecord(detaching)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(attachValue)
	renderInputValue, err := encodeAttachTaskRenderInput(renderInput)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(renderInputValue)
	environmentValue, err := encodeEnvironment(scope.Environment.Record)
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

	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: attachKey(current.Record.ID), ModRevision: current.Revision},
		{Key: environmentKey(scope.Environment.Record.ID), ModRevision: scope.Environment.Revision},
		{Key: projectKey(scope.Project.Record.ID), ModRevision: scope.Project.Revision},
		{Key: environmentKey(scope.BackingEnvironment.Record.ID), ModRevision: scope.BackingEnvironment.Revision},
		{Key: projectKey(scope.BackingProject.Record.ID), ModRevision: scope.BackingProject.Revision},
		{Key: serviceKey(scope.BackingService.Record.Desired.ID), ModRevision: scope.BackingService.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
		{Key: deletionTombstoneKey("environment", current.Record.EnvironmentID)},
		{Key: deletionTombstoneKey("project", scope.Project.Record.ID)},
		{Key: deletionTombstoneKey("tenant", scope.Project.Record.TenantID)},
		{Key: deletionTombstoneKey("environment", current.Record.BackingEnvironmentID)},
		{Key: deletionTombstoneKey("project", current.Record.BackingProjectID)},
		{Key: deletionTombstoneKey("service", current.Record.BackingServiceID)},
		{Key: attachTaskRenderInputKey(task.PlanID)},
		{Key: planReferenceKey},
		{Key: tenantKey(scope.Tenant.Record.ID), ModRevision: scope.Tenant.Revision},
		{
			Key:         environmentBlueprintManifestKey(current.Record.EnvironmentID, renderInput.BlueprintRevisionID),
			ModRevision: scope.BlueprintRevision.Revision,
		},
		{
			Key:         environmentComposeProjectionKey(current.Record.EnvironmentID),
			ModRevision: scope.ComposeProjection.Revision,
		},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: taskReference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: taskReference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: taskReference},
		{Type: MutationPut, Key: attachKey(detaching.ID), Value: attachValue},
		{Type: MutationPut, Key: attachTaskRenderInputKey(task.PlanID), Value: renderInputValue},
		{Type: MutationPut, Key: planReferenceKey, Value: planReferenceValue},
		{Type: MutationPut, Key: environmentKey(scope.Environment.Record.ID), Value: environmentValue},
	}
	for _, service := range scope.Services {
		serviceID := service.Record.Desired.ID
		conditions = append(conditions,
			Condition{Key: serviceKey(serviceID), ModRevision: service.Revision},
			Condition{Key: deletionTombstoneKey("service", serviceID)},
		)
	}
	for _, grant := range scope.Grants {
		conditions = append(conditions,
			Condition{Key: attachKey(grant.Record.ID), ModRevision: grant.Revision},
			Condition{Key: deletionTombstoneKey("attach", grant.Record.ID)},
		)
	}
	initiation := TaskInitiation{}
	if provided == nil {
		taskTenant, tenantErr := loadTaskInitiationTenant(ctx, repository.store, scope.Project)
		if tenantErr != nil {
			return IdempotencyTransactionResult{}, tenantErr
		}
		initiation, err = newEnvironmentTaskInitiation(
			taskTenant, scope.Project, scope.Environment, TaskActorOperator,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
	} else {
		initiation = *provided
		conditions = append(conditions, initiation.fences...)
		if attachDetachWithTaskOperationCount(current.Record)+len(initiation.fences) > maximumTransactionOperations {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"attach detach initiation exceeds the atomic transaction limit",
			)
		}
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyAttachDetachTaskConflict,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *AttachRepository) GetAttach(ctx context.Context, id string) (Versioned[AttachRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if err := validateID(ids.KindAttach, id); err != nil {
		return Versioned[AttachRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		attachKey(id),
		id,
		errs.KindAttachNotFound,
		decodeAttachRecord,
		func(record AttachRecord) string { return record.ID },
	)
}

func (repository *AttachRepository) ResolveAttach(
	ctx context.Context,
	environmentID string,
	reference string,
) (Versioned[AttachRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if ids.Validate(ids.KindAttach, reference) == nil {
		current, err := repository.GetAttach(ctx, reference)
		if err != nil {
			return Versioned[AttachRecord]{}, err
		}
		if current.Record.EnvironmentID != environmentID {
			return Versioned[AttachRecord]{}, errs.New(
				errs.KindScopeUnauthorized,
				"Attach is outside the Environment scope",
			)
		}
		return current, nil
	}
	if reference == "" {
		return Versioned[AttachRecord]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	index, err := repository.store.Get(ctx, attachNameKey(environmentID, reference))
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if index == nil || index.Entry == nil {
		return Versioned[AttachRecord]{}, errs.New(errs.KindAttachNotFound, "Attach was not found")
	}
	id := string(index.Entry.Value)
	if ids.Validate(ids.KindAttach, id) != nil {
		return Versioned[AttachRecord]{}, corruptAttachRecord()
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{attachKey(id)}, Revision: index.ReadRevision,
	})
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return Versioned[AttachRecord]{}, corruptAttachRecord()
	}
	record, err := decodeAttachRecord(result.Values[0].Value)
	if err != nil || record.ID != id || record.EnvironmentID != environmentID || record.Name != reference {
		return Versioned[AttachRecord]{}, corruptAttachRecord()
	}
	return Versioned[AttachRecord]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

func (repository *AttachRepository) ListAttaches(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[AttachRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[AttachRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"attaches",
		"environment",
		environmentID,
		attachOwnerPrefix(environmentID),
		attachKey,
		ids.KindAttach,
		request,
		decodeAttachRecord,
		func(record AttachRecord) string { return record.ID },
		func(record AttachRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *AttachRepository) GetAttachFacts(
	ctx context.Context,
	current Versioned[AttachRecord],
) (AttachEncryptedFacts, bool, error) {
	if err := validateAttachVersion(current); err != nil {
		return AttachEncryptedFacts{}, false, err
	}
	revision := current.ReadRevision
	if revision == 0 {
		revision = current.Revision
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{attachFactsKey(current.Record.ID)}, Revision: revision,
	})
	if err != nil {
		return AttachEncryptedFacts{}, false, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		if len(current.Record.FactSets) == 0 {
			return AttachEncryptedFacts{}, false, nil
		}
		return AttachEncryptedFacts{}, false, errs.New(errs.KindInternal, "Attach encrypted facts are missing")
	}
	facts, err := decodeAttachEncryptedFacts(result.Values[0].Value)
	if err != nil || facts.AttachID != current.Record.ID {
		clear(facts.Ciphertext)
		return AttachEncryptedFacts{}, false, corruptAttachRecord()
	}
	return facts, true, nil
}

func (repository *AttachRepository) ReplaceLifecycle(
	ctx context.Context,
	current Versioned[AttachRecord],
	replacement AttachRecord,
) (Versioned[AttachRecord], error) {
	if err := validateAttachVersion(current); err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if err := validateAttachRecord(replacement); err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if !validAttachLifecycleReplacement(current.Record, replacement) {
		return Versioned[AttachRecord]{}, errs.New(errs.KindStateConflict, "Attach lifecycle replacement is invalid")
	}
	value, err := encodeAttachRecord(replacement)
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	defer clear(value)
	result, err := repository.store.Transact(ctx, []Condition{
		{Key: attachKey(current.Record.ID), ModRevision: current.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
	}, []Mutation{{Type: MutationPut, Key: attachKey(current.Record.ID), Value: value}})
	if err != nil {
		return Versioned[AttachRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[AttachRecord]{}, errs.New(errs.KindStateConflict, "Attach changed concurrently")
	}
	return Versioned[AttachRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *AttachRepository) RenameAttachIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[AttachRecord],
	name string,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateAttachVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replacement := current.Record
	replacement.Name = name
	if err := validateAttachRecord(replacement); err != nil {
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
	secondary, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			attachNameKey(current.Record.EnvironmentID, current.Record.Name),
			attachOwnerKey(current.Record.EnvironmentID, current.Record.ID),
		},
		Revision: current.ReadRevision,
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
	conditions := []Condition{
		{Key: attachKey(current.Record.ID), ModRevision: current.Revision},
		{
			Key:         attachNameKey(current.Record.EnvironmentID, current.Record.Name),
			ModRevision: secondary.Values[0].ModRevision,
		},
		{
			Key:         attachOwnerKey(current.Record.EnvironmentID, current.Record.ID),
			ModRevision: secondary.Values[1].ModRevision,
		},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	renaming := replacement.Name != current.Record.Name
	newNameIndex := -1
	mutations := []Mutation(nil)
	if renaming {
		newNameIndex = len(conditions)
		conditions = append(conditions, Condition{Key: attachNameKey(current.Record.EnvironmentID, replacement.Name)})
		value, encodeErr := encodeAttachRecord(replacement)
		if encodeErr != nil {
			return IdempotencyTransactionResult{}, encodeErr
		}
		defer clear(value)
		mutations = []Mutation{
			{Type: MutationPut, Key: attachKey(current.Record.ID), Value: value},
			{Type: MutationDelete, Key: attachNameKey(current.Record.EnvironmentID, current.Record.Name)},
			{
				Type:  MutationPut,
				Key:   attachNameKey(current.Record.EnvironmentID, replacement.Name),
				Value: []byte(current.Record.ID),
			},
		}
	}
	plan, err := newIdempotencyMutationPlan(conditions, mutations, func(_ int64, values []*KeyValue) error {
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
	})
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
	current Versioned[AttachRecord],
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
	current Versioned[AttachRecord],
	revision int64,
) ([]Condition, []Mutation, [][]byte, error) {
	if err := validateAttachVersion(current); err != nil {
		return nil, nil, nil, err
	}
	if revision <= 0 {
		return nil, nil, nil, errs.New(errs.KindValidationFailed, "Attach removal revision must be positive")
	}
	dependents, err := store.Range(ctx, RangeRequest{
		Prefix: attachGrantedByPrefix(current.Record.ID), Limit: 1, Revision: revision,
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

	conditions := []Condition{
		{Key: attachKey(current.Record.ID), ModRevision: current.Revision},
		{Key: deletionTombstoneKey("attach", current.Record.ID)},
	}
	mutations := []Mutation{
		{Type: MutationDelete, Key: attachKey(current.Record.ID)},
		{Type: MutationDelete, Key: attachNameKey(current.Record.EnvironmentID, current.Record.Name)},
		{Type: MutationDelete, Key: attachOwnerKey(current.Record.EnvironmentID, current.Record.ID)},
		{Type: MutationDelete, Key: attachBackingServiceKey(current.Record.BackingServiceID, current.Record.ID)},
		{Type: MutationDelete, Key: attachBackingProjectKey(current.Record.BackingProjectID, current.Record.ID)},
		{Type: MutationDelete, Key: attachFactsKey(current.Record.ID)},
	}
	for _, serviceID := range current.Record.ServiceIDs {
		mutations = append(mutations, Mutation{
			Type: MutationDelete, Key: attachServiceKey(serviceID, current.Record.ID),
		})
	}
	values := make([][]byte, 0, len(current.Record.GrantAttachIDs))
	if len(current.Record.GrantAttachIDs) == 0 {
		return conditions, mutations, values, nil
	}
	keys := make([]string, 0, len(current.Record.GrantAttachIDs)*2)
	for _, grantID := range current.Record.GrantAttachIDs {
		keys = append(keys, attachKey(grantID), attachGrantedByKey(grantID, current.Record.ID))
	}
	result, err := store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, nil, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return nil, nil, nil, corruptAttachRecord()
	}
	for index, grantID := range current.Record.GrantAttachIDs {
		target := result.Values[index*2]
		reverse := result.Values[index*2+1]
		if target == nil || reverse == nil || string(reverse.Value) != current.Record.ID {
			return nil, nil, values, corruptAttachRecord()
		}
		targetRecord, decodeErr := decodeAttachRecord(target.Value)
		if decodeErr != nil || targetRecord.ID != grantID {
			return nil, nil, values, corruptAttachRecord()
		}
		targetValue, encodeErr := encodeAttachRecord(targetRecord)
		if encodeErr != nil {
			return nil, nil, values, encodeErr
		}
		values = append(values, targetValue)
		conditions = append(conditions,
			Condition{Key: attachKey(grantID), ModRevision: target.ModRevision},
			Condition{Key: attachGrantedByKey(grantID, current.Record.ID), ModRevision: reverse.ModRevision},
		)
		mutations = append(mutations,
			Mutation{Type: MutationDelete, Key: attachGrantedByKey(grantID, current.Record.ID)},
			Mutation{Type: MutationPut, Key: attachKey(grantID), Value: targetValue},
		)
	}
	return conditions, mutations, values, nil
}

func validateAttachCreateScope(
	ctx context.Context,
	scope AttachCreateScope,
	record AttachRecord,
	facts *AttachEncryptedFacts,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateAttachRecord(record); err != nil {
		return err
	}
	if scope.Tenant.Revision <= 0 || scope.Project.Revision <= 0 || scope.Environment.Revision <= 0 ||
		scope.BlueprintRevision.Revision <= 0 || scope.ComposeProjection.Revision <= 0 ||
		scope.BackingProject.Revision <= 0 ||
		scope.BackingEnvironment.Revision <= 0 || scope.BackingService.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach scope records must be versioned")
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID || scope.Project.Record.Kind != ProjectKindTenant ||
		scope.Project.Record.TenantID == "" ||
		scope.Environment.Record.ProjectID != scope.Project.Record.ID ||
		record.EnvironmentID != scope.Environment.Record.ID {
		return errs.New(errs.KindScopeUnauthorized, "Attach consumer hierarchy is invalid")
	}
	if scope.BackingProject.Record.Kind != ProjectKindBacking || scope.BackingProject.Record.TenantID != "" ||
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
	if !slices.Equal(serviceIDs, record.ServiceIDs) {
		return errs.New(errs.KindValidationFailed, "Attach service_ids do not match the resolved Services")
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
	manual := scope.BackingService.Record.Desired.Adapter == "manual"
	if manual {
		if facts != nil || len(record.FactSets) != 0 || len(record.GrantAttachIDs) != 0 {
			return errs.New(
				errs.KindAdapterManualOnly,
				"Manual Attach is network-only and cannot publish facts or grants",
			)
		}
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
	return validateAttachEncryptedFacts(*facts)
}

func validateAttachCreationTask(record AttachRecord, task TaskRecord, marker IdempotencyMarker) error {
	pendingAttachOwned := record.Status == core.AttachPending && record.Operation == AttachOperationProvision &&
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
	current Versioned[AttachRecord],
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateAttachVersion(current); err != nil {
		return err
	}
	record := current.Record
	if record.Status != core.AttachReady &&
		(record.Status != core.AttachFailed || record.Operation != AttachOperationProvision) {
		return errs.New(errs.KindStateConflict, "Attach is not eligible for initial detach")
	}
	if scope.Tenant.Revision <= 0 || scope.Project.Revision <= 0 || scope.Environment.Revision <= 0 ||
		scope.BlueprintRevision.Revision <= 0 || scope.ComposeProjection.Revision <= 0 ||
		scope.BackingProject.Revision <= 0 || scope.BackingEnvironment.Revision <= 0 ||
		scope.BackingService.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach detach scope records must be versioned")
	}
	if scope.Tenant.Record.ID != scope.Project.Record.TenantID || scope.Project.Record.Kind != ProjectKindTenant ||
		scope.Project.Record.TenantID == "" || scope.Environment.Record.ProjectID != scope.Project.Record.ID ||
		record.EnvironmentID != scope.Environment.Record.ID {
		return errs.New(errs.KindScopeUnauthorized, "Attach detach consumer hierarchy is invalid")
	}
	if scope.BackingProject.Record.Kind != ProjectKindBacking || scope.BackingProject.Record.TenantID != "" ||
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
	if !slices.Equal(serviceIDs, record.ServiceIDs) {
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
	return nil
}

func validateAttachDetachTask(
	current AttachRecord,
	detaching AttachRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) error {
	validOwnership := detaching.Status == core.AttachDetaching && detaching.Operation == AttachOperationDetach &&
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

func attachCreateWithTaskOperationCount(record AttachRecord, hasFacts bool) int {
	operations := 45 + (4 * len(record.ServiceIDs)) + (5 * len(record.GrantAttachIDs))
	if hasFacts {
		operations++
	}
	return operations
}

func attachDetachWithTaskOperationCount(record AttachRecord) int {
	return 38 + (2 * len(record.ServiceIDs)) + (2 * len(record.GrantAttachIDs))
}

func validAttachLifecycleReplacement(current AttachRecord, replacement AttachRecord) bool {
	if !attachImmutableEqual(current, replacement) {
		return false
	}
	switch {
	case current.Status == core.AttachPending && replacement.Status == core.AttachProvisioning:
		return current.Operation == AttachOperationProvision && replacement.Operation == current.Operation &&
			replacement.TaskID == current.TaskID
	case current.Status == core.AttachProvisioning &&
		(replacement.Status == core.AttachReady || replacement.Status == core.AttachFailed):
		return replacement.Operation == current.Operation && replacement.TaskID == current.TaskID
	case current.Status == core.AttachFailed && current.Operation == AttachOperationProvision &&
		replacement.Status == core.AttachPending:
		return replacement.Operation == current.Operation && replacement.TaskID != current.TaskID
	case (current.Status == core.AttachReady || current.Status == core.AttachFailed) &&
		replacement.Status == core.AttachDetaching:
		return replacement.Operation == AttachOperationDetach && replacement.TaskID != current.TaskID
	case current.Status == core.AttachDetaching &&
		(replacement.Status == core.AttachDetached || replacement.Status == core.AttachFailed):
		return replacement.Operation == current.Operation && replacement.TaskID == current.TaskID
	case current.Status == core.AttachFailed && current.Operation == AttachOperationDetach &&
		replacement.Status == core.AttachDetaching:
		return replacement.Operation == current.Operation && replacement.TaskID != current.TaskID
	default:
		return false
	}
}

func attachImmutableEqual(left AttachRecord, right AttachRecord) bool {
	if left.ID != right.ID || left.EnvironmentID != right.EnvironmentID || left.Name != right.Name ||
		left.BackingProjectID != right.BackingProjectID || left.BackingEnvironmentID != right.BackingEnvironmentID ||
		left.BackingServiceID != right.BackingServiceID || left.BackingNetworkID != right.BackingNetworkID ||
		!left.CreatedAt.Equal(right.CreatedAt) ||
		!slices.Equal(left.ServiceIDs, right.ServiceIDs) || !slices.Equal(left.GrantAttachIDs, right.GrantAttachIDs) ||
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

func validateAttachVersion(current Versioned[AttachRecord]) error {
	if current.Revision <= 0 {
		return errs.New(errs.KindValidationFailed, "Attach record revision must be positive")
	}
	return validateAttachRecord(current.Record)
}

func classifyAttachCreateConflict(reads []*KeyValue) error {
	if len(reads) > 1 && reads[1] != nil {
		return errs.New(errs.KindNameConflict, "Attach name already exists")
	}
	return errs.New(errs.KindStateConflict, "Attach hierarchy changed concurrently")
}

func classifyAttachTaskCreateConflict(_ int64, reads []*KeyValue) error {
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

func classifyAttachDetachTaskConflict(_ int64, reads []*KeyValue) error {
	if len(reads) < 5 {
		return errs.New(errs.KindInternal, "Attach detach Task conflict evidence is incomplete")
	}
	for _, read := range reads[:4] {
		if read != nil {
			return errs.New(errs.KindStateConflict, "Attach detach Task operation is already active")
		}
	}
	return errs.New(errs.KindStateConflict, "Attach detach scope changed concurrently")
}

func encodeAttachRecord(record AttachRecord) ([]byte, error) {
	if err := validateAttachRecord(record); err != nil {
		return nil, err
	}
	value, err := json.Marshal(record)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func decodeAttachRecord(value []byte) (AttachRecord, error) {
	var record AttachRecord
	if err := json.Unmarshal(value, &record); err != nil {
		return AttachRecord{}, corruptAttachRecord()
	}
	if err := validateAttachRecord(record); err != nil {
		return AttachRecord{}, corruptAttachRecord()
	}
	return record, nil
}

func encodeAttachEncryptedFacts(facts AttachEncryptedFacts) ([]byte, error) {
	if err := validateAttachEncryptedFacts(facts); err != nil {
		return nil, err
	}
	value, err := json.Marshal(facts)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return value, nil
}

func decodeAttachEncryptedFacts(value []byte) (AttachEncryptedFacts, error) {
	var facts AttachEncryptedFacts
	if err := json.Unmarshal(value, &facts); err != nil {
		return AttachEncryptedFacts{}, corruptAttachRecord()
	}
	if err := validateAttachEncryptedFacts(facts); err != nil {
		clear(facts.Ciphertext)
		return AttachEncryptedFacts{}, corruptAttachRecord()
	}
	return facts, nil
}

func corruptAttachRecord() error {
	return errs.New(errs.KindInternal, "Attach durable state is corrupt")
}

func attachKey(id string) string {
	return attachRecordPrefix + id
}

func attachOwnerPrefix(environmentID string) string {
	return "/v1/indexes/attaches/by-owner/environment/" + environmentID + "/"
}

func attachOwnerKey(environmentID string, attachID string) string {
	return attachOwnerPrefix(environmentID) + attachID
}

func attachNameKey(environmentID string, name string) string {
	return "/v1/indexes/attaches/by-name/environment/" + environmentID + "/" + encodeDynamicSegment(name)
}

func attachServiceKey(serviceID string, attachID string) string {
	return "/v1/indexes/attaches/by-service/service/" + serviceID + "/" + attachID
}

func attachBackingServiceKey(serviceID string, attachID string) string {
	return "/v1/indexes/attaches/by-backing-service/service/" + serviceID + "/" + attachID
}

func attachBackingProjectKey(projectID string, attachID string) string {
	return "/v1/indexes/attaches/by-backing-project/project/" + projectID + "/" + attachID
}

func attachGrantedByPrefix(attachID string) string {
	return "/v1/indexes/attaches/by-granted-attach/attach/" + attachID + "/"
}

func attachGrantedByKey(grantAttachID string, attachID string) string {
	return attachGrantedByPrefix(grantAttachID) + attachID
}

func attachFactsKey(attachID string) string {
	return "/v1/secret-values/attach-facts/" + attachID
}
