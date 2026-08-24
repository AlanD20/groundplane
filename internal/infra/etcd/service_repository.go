package etcd

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceRepository owns the flat Service primary, scoped-name uniqueness,
// Environment membership, fixed-revision pagination, and revision-fenced
// desired/runtime updates.
type ServiceRepository struct {
	store hierarchyStore
}

type ordinaryEnvironmentMutationContext struct {
	environmentID string
	readRevision  int64
	fence         environmentMutationFenceEvidence
}

type ordinaryEnvironmentMutationBinding struct {
	context                *ordinaryEnvironmentMutationContext
	conditions             []Condition
	mutations              []Mutation
	originalConditionCount int
	fenceIndexes           []int
	preparedReads          []*KeyValue
}

func loadOrdinaryEnvironmentMutationContext(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	anchorKey string,
	projectID string,
	tenantID string,
) (*ordinaryEnvironmentMutationContext, error) {
	anchor, err := store.Get(ctx, anchorKey)
	if err != nil {
		return nil, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 {
		return nil, errs.New(errs.KindInternal, "environment mutation anchor read is invalid")
	}
	evidence, err := loadOrdinaryEnvironmentMutationFence(ctx, store, environmentID, anchor.ReadRevision)
	if err != nil {
		return nil, err
	}
	mutationContext := &ordinaryEnvironmentMutationContext{
		environmentID: environmentID,
		readRevision:  anchor.ReadRevision,
		fence:         evidence,
	}
	if _, ok := mutationContext.revisionForKey(environmentKey(environmentID)); !ok {
		return nil, errs.New(errs.KindInternal, "environment mutation fence omitted the environment")
	}
	if _, ok := mutationContext.revisionForKey(projectKey(projectID)); !ok {
		return nil, errs.New(errs.KindScopeUnauthorized, "environment mutation project scope is invalid")
	}
	if tenantID != "" {
		if _, ok := mutationContext.revisionForKey(tenantKey(tenantID)); !ok {
			return nil, errs.New(errs.KindScopeUnauthorized, "environment mutation tenant scope is invalid")
		}
	}
	return mutationContext, nil
}

func (mutationContext *ordinaryEnvironmentMutationContext) revisionForKey(key string) (int64, bool) {
	if mutationContext == nil {
		return 0, false
	}
	for _, condition := range mutationContext.fence.transactionConditions() {
		if condition.Key == key && condition.ModRevision > 0 && !condition.Prefix {
			return condition.ModRevision, true
		}
	}
	return 0, false
}

func (mutationContext *ordinaryEnvironmentMutationContext) versionHierarchy(
	tenant *Versioned[TenantRecord],
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
) (*Versioned[TenantRecord], Versioned[ProjectRecord], Versioned[EnvironmentRecord], error) {
	projectRevision, ok := mutationContext.revisionForKey(projectKey(project.Record.ID))
	if !ok {
		return nil, Versioned[ProjectRecord]{}, Versioned[EnvironmentRecord]{}, errs.New(
			errs.KindScopeUnauthorized,
			"environment mutation project scope is invalid",
		)
	}
	environmentRevision, ok := mutationContext.revisionForKey(environmentKey(environment.Record.ID))
	if !ok {
		return nil, Versioned[ProjectRecord]{}, Versioned[EnvironmentRecord]{}, errs.New(
			errs.KindScopeUnauthorized,
			"environment mutation environment scope is invalid",
		)
	}
	project.Revision = projectRevision
	project.ReadRevision = mutationContext.readRevision
	environment.Revision = environmentRevision
	environment.ReadRevision = mutationContext.readRevision
	if project.Record.TenantID == "" {
		if tenant != nil {
			return nil, Versioned[ProjectRecord]{}, Versioned[EnvironmentRecord]{}, errs.New(
				errs.KindScopeUnauthorized,
				"backing project environment mutation cannot contain a tenant",
			)
		}
		return nil, project, environment, nil
	}
	if tenant == nil || tenant.Record.ID != project.Record.TenantID {
		return nil, Versioned[ProjectRecord]{}, Versioned[EnvironmentRecord]{}, errs.New(
			errs.KindScopeUnauthorized,
			"environment mutation tenant scope is invalid",
		)
	}
	tenantRevision, ok := mutationContext.revisionForKey(tenantKey(tenant.Record.ID))
	if !ok {
		return nil, Versioned[ProjectRecord]{}, Versioned[EnvironmentRecord]{}, errs.New(
			errs.KindScopeUnauthorized,
			"environment mutation tenant scope is invalid",
		)
	}
	versionedTenant := *tenant
	versionedTenant.Revision = tenantRevision
	versionedTenant.ReadRevision = mutationContext.readRevision
	return &versionedTenant, project, environment, nil
}

func (mutationContext *ordinaryEnvironmentMutationContext) bind(
	ctx context.Context,
	store hierarchyStore,
	conditions []Condition,
	mutations []Mutation,
	advanceEpoch bool,
) (*ordinaryEnvironmentMutationBinding, error) {
	if mutationContext == nil || mutationContext.readRevision <= 0 {
		return nil, errs.New(errs.KindInternal, "environment mutation context is invalid")
	}
	originalConditionCount := len(conditions)
	conditions = append([]Condition(nil), conditions...)
	fenceConditions := mutationContext.fence.transactionConditions()
	fenceIndexes := make([]int, len(fenceConditions))
	conditionIndexes := make(map[string]int, len(conditions)+len(fenceConditions))
	for index, condition := range conditions {
		if condition.Key == "" {
			return nil, errs.New(errs.KindInternal, "environment mutation compare key is invalid")
		}
		if _, duplicate := conditionIndexes[condition.Key]; duplicate {
			return nil, errs.New(errs.KindInternal, "environment mutation contains a duplicate compare key")
		}
		conditionIndexes[condition.Key] = index
	}
	for index, condition := range fenceConditions {
		if existing, ok := conditionIndexes[condition.Key]; ok {
			conditions[existing] = condition
			fenceIndexes[index] = existing
			continue
		}
		fenceIndexes[index] = len(conditions)
		conditionIndexes[condition.Key] = len(conditions)
		conditions = append(conditions, condition)
	}
	mutations = append([]Mutation(nil), mutations...)
	filteredMutations := mutations[:0]
	for _, mutation := range mutations {
		if mutation.Type == MutationPut && mutation.Key == environmentKey(mutationContext.environmentID) {
			continue
		}
		filteredMutations = append(filteredMutations, mutation)
	}
	mutations = filteredMutations
	if advanceEpoch {
		epochMutation, err := mutationContext.fence.epochRewriteMutation()
		if err != nil {
			return nil, err
		}
		mutations = append(mutations, epochMutation)
	}
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		clearMutationValues(mutations)
		return nil, errs.New(errs.KindValidationFailed, "environment mutation exceeds the atomic transaction limit")
	}
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		keys[index] = condition.Key
	}
	prepared, err := store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: mutationContext.readRevision})
	if err != nil {
		clearMutationValues(mutations)
		return nil, err
	}
	if prepared == nil || prepared.ReadRevision != mutationContext.readRevision ||
		len(prepared.Values) != len(conditions) {
		clearMutationValues(mutations)
		return nil, errs.New(errs.KindInternal, "environment mutation domain read is invalid")
	}
	return &ordinaryEnvironmentMutationBinding{
		context: mutationContext, conditions: conditions, mutations: mutations,
		originalConditionCount: originalConditionCount,
		fenceIndexes:           fenceIndexes,
		preparedReads:          prepared.Values,
	}, nil
}

func (binding *ordinaryEnvironmentMutationBinding) clear() {
	if binding == nil {
		return
	}
	clearKeyValues(binding.preparedReads)
	binding.preparedReads = nil
}

func conditionMatchesRead(condition Condition, value *KeyValue) bool {
	if condition.Prefix {
		return false
	}
	if condition.ModRevision == 0 {
		return value == nil
	}
	return value != nil && value.Key == condition.Key && value.ModRevision == condition.ModRevision
}

func (binding *ordinaryEnvironmentMutationBinding) classify(
	revision int64,
	reads []*KeyValue,
	fallback idempotencyPlanClassifier,
) error {
	if binding == nil || binding.context == nil || fallback == nil ||
		len(reads) != len(binding.conditions) {
		return errs.New(errs.KindInternal, "environment mutation compare evidence is incomplete")
	}
	for index := range binding.originalConditionCount {
		if !conditionMatchesRead(binding.conditions[index], reads[index]) {
			return fallback(revision, reads[:binding.originalConditionCount])
		}
	}
	fenceReads := make([]*KeyValue, len(binding.fenceIndexes))
	for index, conditionIndex := range binding.fenceIndexes {
		fenceReads[index] = reads[conditionIndex]
	}
	return binding.context.fence.classifyCAS(fenceReads)
}

func (binding *ordinaryEnvironmentMutationBinding) preparedConflict(
	fallback idempotencyPlanClassifier,
) error {
	if binding == nil || len(binding.preparedReads) != len(binding.conditions) {
		return errs.New(errs.KindInternal, "environment mutation prepared evidence is incomplete")
	}
	for index, condition := range binding.conditions {
		if !conditionMatchesRead(condition, binding.preparedReads[index]) {
			return binding.classify(binding.context.readRevision, binding.preparedReads, fallback)
		}
	}
	return nil
}

func (binding *ordinaryEnvironmentMutationBinding) preparedConditionsMatch() (bool, error) {
	if binding == nil || len(binding.preparedReads) != len(binding.conditions) {
		return false, errs.New(errs.KindInternal, "environment mutation prepared evidence is incomplete")
	}
	for index, condition := range binding.conditions {
		if !conditionMatchesRead(condition, binding.preparedReads[index]) {
			return false, nil
		}
	}
	return true, nil
}

func existingIdempotencyTransaction(
	ctx context.Context,
	store hierarchyStore,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, bool, error) {
	repository, err := newIdempotencyRepository(store)
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	evidence, err := repository.Read(ctx, marker.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	if evidence == nil {
		return IdempotencyTransactionResult{}, false, nil
	}
	existing, err := evidence.Marker()
	if err != nil {
		return IdempotencyTransactionResult{}, false, err
	}
	return IdempotencyTransactionResult{
		kind: idempotencyTransactionExisting, revision: evidence.modRevision, marker: existing,
	}, true, nil
}

func NewServiceRepository(store Store) (*ServiceRepository, error) {
	return newServiceRepository(store)
}

func newServiceRepository(store hierarchyStore) (*ServiceRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Service store is required")
	}
	return &ServiceRepository{store: store}, nil
}

func (repository *ServiceRepository) CreateService(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
) (Versioned[ServiceRecord], error) {
	conditions, mutations, classify, err := repository.prepareServiceCreation(
		ctx, environment, project, record, ServiceMutationReferences{}, false,
	)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	defer clearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ServiceRecord]{}, classify(result.Revision, result.FailureReads)
	}
	return Versioned[ServiceRecord]{
		Record: record, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

// ServiceMutationReferences are every live Zone and dependency Service used by
// a direct desired mutation. Their revisions and deletion fences join the same
// transaction as the Service and replay marker.
type ServiceMutationReferences struct {
	Zones        []Versioned[ZoneRecord]
	Dependencies []Versioned[ServiceRecord]
}

func (repository *ServiceRepository) CreateServiceIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
	references ServiceMutationReferences,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateServiceMutationMarker(marker, record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	conditions, mutations, classify, err := repository.prepareServiceCreation(
		ctx, environment, project, record, references, true,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	plan, err := newIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ServiceRepository) prepareServiceCreation(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
	references ServiceMutationReferences,
	enforceReferences bool,
) ([]Condition, []Mutation, idempotencyPlanClassifier, error) {
	if err := validateServiceHierarchy(ctx, environment, project, record); err != nil {
		return nil, nil, nil, err
	}
	mutationContext, err := loadOrdinaryEnvironmentMutationContext(
		ctx,
		repository.store,
		record.EnvironmentID,
		serviceKey(record.Desired.ID),
		project.Record.ID,
		project.Record.TenantID,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	value, err := encodeServiceRecord(record)
	if err != nil {
		return nil, nil, nil, err
	}
	conditions := []Condition{
		{Key: serviceKey(record.Desired.ID)},
		{Key: serviceNameKey(record.EnvironmentID, record.Desired.Name)},
		{Key: serviceOwnerKey(record.EnvironmentID, record.Desired.ID)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("service", record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	baseCount := len(conditions)
	if enforceReferences {
		referenceConditions, referenceErr := serviceMutationReferenceConditions(record, references)
		if referenceErr != nil {
			clear(value)
			return nil, nil, nil, referenceErr
		}
		conditions = append(conditions, referenceConditions...)
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: serviceKey(record.Desired.ID), Value: value},
		{
			Type:  MutationPut,
			Key:   serviceNameKey(record.EnvironmentID, record.Desired.Name),
			Value: []byte(record.Desired.ID),
		},
		{
			Type:  MutationPut,
			Key:   serviceOwnerKey(record.EnvironmentID, record.Desired.ID),
			Value: []byte(record.Desired.ID),
		},
	}
	classify := func(_ int64, values []*KeyValue) error {
		if enforceReferences {
			if err := classifyServiceMutationReferenceConflict(values[baseCount:], references); err != nil {
				return err
			}
		}
		return classifyServiceWriteConflict(values[:baseCount], environment, project, record, 0)
	}
	binding, err := mutationContext.bind(ctx, repository.store, conditions, mutations, true)
	if err != nil {
		clear(value)
		return nil, nil, nil, err
	}
	binding.clear()
	originalClassify := classify
	classify = func(revision int64, values []*KeyValue) error {
		return binding.classify(revision, values, originalClassify)
	}
	return binding.conditions, binding.mutations, classify, nil
}

func (repository *ServiceRepository) GetService(
	ctx context.Context,
	id string,
) (Versioned[ServiceRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if err := validateID(ids.KindService, id); err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	return getRecord(
		ctx,
		repository.store,
		serviceKey(id),
		id,
		errs.KindServiceNotFound,
		decodeServiceRecord,
		func(record ServiceRecord) string { return record.Desired.ID },
	)
}

func (repository *ServiceRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[ServiceRecord], error) {
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Page[ServiceRecord]{}, err
	}
	return listIndexPage(
		ctx,
		repository.store,
		"services",
		"environment",
		environmentID,
		serviceOwnerPrefix(environmentID),
		serviceKey,
		ids.KindService,
		request,
		decodeServiceRecord,
		func(record ServiceRecord) string { return record.Desired.ID },
		func(record ServiceRecord) bool { return record.EnvironmentID == environmentID },
	)
}

func (repository *ServiceRepository) ReplaceDesired(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	desired core.Service,
) (Versioned[ServiceRecord], error) {
	replacement, err := ReplaceServiceDesired(current.Record, desired)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	return repository.updateService(ctx, environment, project, current, replacement, ServiceMutationReferences{}, false)
}

func (repository *ServiceRepository) ReplaceDesiredIdempotent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	desired core.Service,
	references ServiceMutationReferences,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateServiceMutationMarker(marker, current.Record.EnvironmentID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	replacement, err := ReplaceServiceDesired(current.Record, desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions, mutations, classify, binding, err := repository.prepareServiceUpdate(
		ctx, environment, project, current, replacement, references, true,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer binding.clear()
	defer clearMutationValues(mutations)
	plan, err := newIdempotencyMutationPlan(conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func (repository *ServiceRepository) SetRuntimeIntent(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	intent core.ServiceRuntimeIntent,
) (Versioned[ServiceRecord], error) {
	replacement, err := SetServiceRuntimeIntent(current.Record, intent)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	return repository.updateService(ctx, environment, project, current, replacement, ServiceMutationReferences{}, false)
}

func (repository *ServiceRepository) updateService(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	replacement ServiceRecord,
	references ServiceMutationReferences,
	enforceReferences bool,
) (Versioned[ServiceRecord], error) {
	conditions, mutations, classify, binding, err := repository.prepareServiceUpdate(
		ctx, environment, project, current, replacement, references, enforceReferences,
	)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	defer binding.clear()
	defer clearMutationValues(mutations)
	if len(mutations) == 0 {
		matches, matchErr := binding.preparedConditionsMatch()
		if matchErr != nil {
			return Versioned[ServiceRecord]{}, matchErr
		}
		if !matches {
			return Versioned[ServiceRecord]{}, classify(binding.context.readRevision, binding.preparedReads)
		}
		return current, nil
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return Versioned[ServiceRecord]{}, err
	}
	if !result.Succeeded {
		return Versioned[ServiceRecord]{}, classify(result.Revision, result.FailureReads)
	}
	return Versioned[ServiceRecord]{
		Record: replacement, Revision: result.Revision, ReadRevision: result.Revision,
	}, nil
}

func (repository *ServiceRepository) prepareServiceUpdate(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	current Versioned[ServiceRecord],
	replacement ServiceRecord,
	references ServiceMutationReferences,
	enforceReferences bool,
) ([]Condition, []Mutation, idempotencyPlanClassifier, *ordinaryEnvironmentMutationBinding, error) {
	if err := validateServiceHierarchy(ctx, environment, project, replacement); err != nil {
		return nil, nil, nil, nil, err
	}
	if err := validateServiceVersion(current); err != nil {
		return nil, nil, nil, nil, err
	}
	if current.Record.EnvironmentID != replacement.EnvironmentID ||
		current.Record.Desired.ID != replacement.Desired.ID ||
		current.Record.Desired.Name != replacement.Desired.Name {
		return nil, nil, nil, nil, errs.New(
			errs.KindValidationFailed,
			"Service update changed immutable identity or ownership",
		)
	}
	mutationContext, err := loadOrdinaryEnvironmentMutationContext(
		ctx,
		repository.store,
		current.Record.EnvironmentID,
		serviceKey(current.Record.Desired.ID),
		project.Record.ID,
		project.Record.TenantID,
	)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			serviceNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
			serviceOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
		},
		Revision: mutationContext.readRevision,
	})
	if err != nil {
		return nil, nil, nil, nil, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return nil, nil, nil, nil, errs.New(errs.KindInternal, "Service indexes are missing or corrupt")
	}
	value, err := encodeServiceRecord(replacement)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	currentValue, err := encodeServiceRecord(current.Record)
	if err != nil {
		clear(value)
		return nil, nil, nil, nil, err
	}
	noOp := bytes.Equal(currentValue, value)
	clear(currentValue)
	conditions := []Condition{
		{Key: serviceKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{
			Key:         serviceNameKey(current.Record.EnvironmentID, current.Record.Desired.Name),
			ModRevision: indexes.Values[0].ModRevision,
		},
		{
			Key:         serviceOwnerKey(current.Record.EnvironmentID, current.Record.Desired.ID),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("service", current.Record.Desired.ID)},
		{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
	}
	if project.Record.TenantID != "" {
		conditions = append(conditions, Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)})
	}
	baseCount := len(conditions)
	if enforceReferences {
		referenceConditions, referenceErr := serviceMutationReferenceConditions(replacement, references)
		if referenceErr != nil {
			clear(value)
			return nil, nil, nil, nil, referenceErr
		}
		conditions = append(conditions, referenceConditions...)
	}
	mutations := []Mutation(nil)
	if !noOp {
		mutations = []Mutation{{Type: MutationPut, Key: serviceKey(replacement.Desired.ID), Value: value}}
	} else {
		clear(value)
	}
	classify := func(_ int64, values []*KeyValue) error {
		if enforceReferences {
			if err := classifyServiceMutationReferenceConflict(values[baseCount:], references); err != nil {
				return err
			}
		}
		return classifyServiceWriteConflict(values[:baseCount], environment, project, current.Record, current.Revision)
	}
	binding, err := mutationContext.bind(ctx, repository.store, conditions, mutations, !noOp)
	if err != nil {
		clearMutationValues(mutations)
		return nil, nil, nil, nil, err
	}
	originalClassify := classify
	classify = func(revision int64, values []*KeyValue) error {
		return binding.classify(revision, values, originalClassify)
	}
	return binding.conditions, binding.mutations, classify, binding, nil
}

func serviceMutationReferenceConditions(
	record ServiceRecord,
	references ServiceMutationReferences,
) ([]Condition, error) {
	wantZones := make(map[string]struct{}, len(record.Desired.Zones))
	for _, name := range record.Desired.Zones {
		wantZones[name] = struct{}{}
	}
	if len(wantZones) != len(references.Zones) || len(record.Desired.DependsOn) != len(references.Dependencies) {
		return nil, errs.New(errs.KindValidationFailed, "Service mutation references are incomplete")
	}
	conditions := make([]Condition, 0, 2*(len(references.Zones)+len(references.Dependencies)))
	for _, zone := range references.Zones {
		if err := validateZoneRecord(zone.Record); err != nil || zone.Revision <= 0 ||
			zone.ReadRevision < zone.Revision || zone.Record.EnvironmentID != record.EnvironmentID {
			return nil, errs.New(errs.KindValidationFailed, "Service Zone reference is invalid")
		}
		if _, ok := wantZones[zone.Record.Desired.Name]; !ok {
			return nil, errs.New(errs.KindValidationFailed, "Service Zone reference is not desired")
		}
		delete(wantZones, zone.Record.Desired.Name)
		conditions = append(conditions,
			Condition{Key: zoneKey(zone.Record.Desired.ID), ModRevision: zone.Revision},
			Condition{Key: deletionTombstoneKey("zone", zone.Record.Desired.ID)},
		)
	}
	wantDependencies := make(map[string]struct{}, len(record.Desired.DependsOn))
	for name := range record.Desired.DependsOn {
		wantDependencies[name] = struct{}{}
	}
	for _, dependency := range references.Dependencies {
		if err := validateServiceVersion(
			dependency,
		); err != nil ||
			dependency.Record.EnvironmentID != record.EnvironmentID ||
			dependency.Record.Desired.ID == record.Desired.ID {
			return nil, errs.New(errs.KindValidationFailed, "Service dependency reference is invalid")
		}
		if _, ok := wantDependencies[dependency.Record.Desired.Name]; !ok {
			return nil, errs.New(errs.KindValidationFailed, "Service dependency reference is not desired")
		}
		delete(wantDependencies, dependency.Record.Desired.Name)
		conditions = append(conditions,
			Condition{Key: serviceKey(dependency.Record.Desired.ID), ModRevision: dependency.Revision},
			Condition{Key: deletionTombstoneKey("service", dependency.Record.Desired.ID)},
		)
	}
	if len(wantZones) != 0 || len(wantDependencies) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Service mutation references are incomplete")
	}
	return conditions, nil
}

func classifyServiceMutationReferenceConflict(values []*KeyValue, references ServiceMutationReferences) error {
	if len(values) != 2*(len(references.Zones)+len(references.Dependencies)) {
		return errs.New(errs.KindInternal, "Service reference compare evidence is incomplete")
	}
	offset := 0
	for _, zone := range references.Zones {
		if values[offset] == nil || values[offset].ModRevision != zone.Revision {
			return errs.New(errs.KindStateConflict, "Service Zone reference changed")
		}
		if values[offset+1] != nil {
			return errs.New(errs.KindResourceInUse, "Service Zone removal is in progress")
		}
		offset += 2
	}
	for _, dependency := range references.Dependencies {
		if values[offset] == nil || values[offset].ModRevision != dependency.Revision {
			return errs.New(errs.KindStateConflict, "Service dependency changed")
		}
		if values[offset+1] != nil {
			return errs.New(errs.KindResourceInUse, "Service dependency removal is in progress")
		}
		offset += 2
	}
	return nil
}

func validateServiceMutationMarker(marker IdempotencyMarker, environmentID string) error {
	if marker.Kind != IdempotencyMarkerDirect || marker.State != IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return errs.New(
			errs.KindValidationFailed,
			"Service mutation marker must be a completed Environment-scoped direct mutation",
		)
	}
	return validateIdempotencyMarker(marker)
}

func validateServiceHierarchy(
	ctx context.Context,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := validateProject(project.Record); err != nil {
		return err
	}
	if err := validateServiceRecord(record); err != nil {
		return err
	}
	if environment.Revision <= 0 || environment.ReadRevision < environment.Revision || project.Revision <= 0 ||
		project.ReadRevision < project.Revision || record.EnvironmentID != environment.Record.ID ||
		environment.Record.ProjectID != project.Record.ID {
		return errs.New(errs.KindValidationFailed, "Service hierarchy ownership or revisions are invalid")
	}
	return nil
}

func validateServiceVersion(current Versioned[ServiceRecord]) error {
	if err := validateServiceRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Service version metadata is invalid")
	}
	return nil
}

func classifyServiceWriteConflict(
	values []*KeyValue,
	environment Versioned[EnvironmentRecord],
	project Versioned[ProjectRecord],
	record ServiceRecord,
	expectedServiceRevision int64,
) error {
	expected := 8
	if project.Record.TenantID != "" {
		expected++
	}
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Service write compare evidence is incomplete")
	}
	if expectedServiceRevision == 0 {
		if values[0] != nil || values[2] != nil {
			return errs.New(errs.KindStateConflict, "Service stable identity is already in use")
		}
		if values[1] != nil {
			return errs.New(errs.KindNameConflict, "Service name is already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindServiceNotFound, "Service was not found")
		}
		if values[0].ModRevision != expectedServiceRevision {
			return stateConflict("service", record.Desired.ID)
		}
		for _, index := range []int{1, 2} {
			if values[index] == nil || string(values[index].Value) != record.Desired.ID {
				return errs.New(errs.KindInternal, "Service index changed or is corrupt")
			}
		}
	}
	if values[3] == nil {
		return errs.New(errs.KindEnvironmentNotFound, "Environment was not found")
	}
	if values[3].ModRevision != environment.Revision {
		return stateConflict("environment", environment.Record.ID)
	}
	if values[4] == nil {
		return errs.New(errs.KindProjectNotFound, "Project was not found")
	}
	if values[4].ModRevision != project.Revision {
		return stateConflict("project", project.Record.ID)
	}
	for _, index := range []int{5, 6, 7} {
		if values[index] != nil {
			return errs.New(errs.KindResourceInUse, "Service hierarchy deletion is in progress")
		}
	}
	if expected == 9 && values[8] != nil {
		return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
	}
	return stateConflict("service", record.Desired.ID)
}
