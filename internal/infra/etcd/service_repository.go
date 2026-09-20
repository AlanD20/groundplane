package etcd

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ServiceRepository projects head-selected desired Services and joins their
// independently mutable runtime sidecars.
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
	conditions             []etcdstore.Condition
	mutations              []etcdstore.Mutation
	originalConditionCount int
	fenceIndexes           []int
	preparedReads          []*etcdstore.KeyValue
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
	if _, ok := mutationContext.revisionForKey(hierarchyrecord.EnvironmentKey(environmentID)); !ok {
		return nil, errs.New(errs.KindInternal, "environment mutation fence omitted the environment")
	}
	if _, ok := mutationContext.revisionForKey(hierarchyrecord.ProjectKey(projectID)); !ok {
		return nil, errs.New(errs.KindScopeUnauthorized, "environment mutation project scope is invalid")
	}
	if tenantID != "" {
		if _, ok := mutationContext.revisionForKey(hierarchyrecord.TenantKey(tenantID)); !ok {
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
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
) (*etcdstore.Versioned[hierarchyrecord.TenantRecord], etcdstore.Versioned[hierarchyrecord.ProjectRecord], etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	projectRevision, ok := mutationContext.revisionForKey(hierarchyrecord.ProjectKey(project.Record.ID))
	if !ok {
		return nil, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
			errs.KindScopeUnauthorized,
			"environment mutation project scope is invalid",
		)
	}
	environmentRevision, ok := mutationContext.revisionForKey(hierarchyrecord.EnvironmentKey(environment.Record.ID))
	if !ok {
		return nil, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
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
			return nil, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
				errs.KindScopeUnauthorized,
				"backing project environment mutation cannot contain a tenant",
			)
		}
		return nil, project, environment, nil
	}
	if tenant == nil || tenant.Record.ID != project.Record.TenantID {
		return nil, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
			errs.KindScopeUnauthorized,
			"environment mutation tenant scope is invalid",
		)
	}
	tenantRevision, ok := mutationContext.revisionForKey(hierarchyrecord.TenantKey(tenant.Record.ID))
	if !ok {
		return nil, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
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
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	advanceEpoch bool,
) (*ordinaryEnvironmentMutationBinding, error) {
	binding, err := mutationContext.prepareBinding(ctx, store, conditions, mutations, advanceEpoch)
	if err != nil {
		return nil, err
	}
	if err := validateEnvironmentMutationTransactionBudget(binding.conditions, binding.mutations); err != nil {
		binding.clear()
		return nil, err
	}
	return binding, nil
}

func (mutationContext *ordinaryEnvironmentMutationContext) prepareBinding(
	ctx context.Context,
	store hierarchyStore,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	advanceEpoch bool,
) (*ordinaryEnvironmentMutationBinding, error) {
	if mutationContext == nil || mutationContext.readRevision <= 0 {
		return nil, errs.New(errs.KindInternal, "environment mutation context is invalid")
	}
	originalConditionCount := len(conditions)
	conditions = append([]etcdstore.Condition(nil), conditions...)
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
	mutations = append([]etcdstore.Mutation(nil), mutations...)
	filteredMutations := mutations[:0]
	for _, mutation := range mutations {
		if mutation.Type == etcdstore.MutationPut && mutation.Key == hierarchyrecord.EnvironmentKey(mutationContext.environmentID) {
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
	keys := make([]string, len(conditions))
	for index, condition := range conditions {
		keys[index] = condition.Key
	}
	prepared, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: mutationContext.readRevision})
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

func conditionMatchesRead(condition etcdstore.Condition, value *etcdstore.KeyValue) bool {
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
	reads []*etcdstore.KeyValue,
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
	fenceReads := make([]*etcdstore.KeyValue, len(binding.fenceIndexes))
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

func NewServiceRepository(store etcdstore.Store) (*ServiceRepository, error) {
	return newServiceRepository(store)
}

func newServiceRepository(store hierarchyStore) (*ServiceRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "Service store is required")
	}
	return &ServiceRepository{store: store}, nil
}

// ServiceMutationReferences are the live desired resources used to construct
// one complete candidate Environment revision.
type ServiceMutationReferences struct {
	Zones        []etcdstore.Versioned[zonerecord.Record]
	Dependencies []etcdstore.Versioned[ServiceRecord]
}

func serviceMutationReferenceConditions(
	record ServiceRecord,
	references ServiceMutationReferences,
) ([]etcdstore.Condition, error) {
	wantZones := make(map[string]struct{}, len(record.Desired.Zones))
	for _, name := range record.Desired.Zones {
		wantZones[name] = struct{}{}
	}
	if len(wantZones) != len(references.Zones) || len(record.Desired.DependsOn) != len(references.Dependencies) {
		return nil, errs.New(errs.KindValidationFailed, "Service mutation references are incomplete")
	}
	conditions := make([]etcdstore.Condition, 0, len(references.Zones)+2*len(references.Dependencies))
	for _, zone := range references.Zones {
		if err := zonerecord.ValidateRecord(zone.Record); err != nil || zone.Revision <= 0 ||
			zone.ReadRevision < zone.Revision || zone.Record.EnvironmentID != record.EnvironmentID {
			return nil, errs.New(errs.KindValidationFailed, "Service Zone reference is invalid")
		}
		if _, ok := wantZones[zone.Record.Desired.Name]; !ok {
			return nil, errs.New(errs.KindValidationFailed, "Service Zone reference is not desired")
		}
		delete(wantZones, zone.Record.Desired.Name)
		conditions = append(conditions, etcdstore.Condition{Key: deletionTombstoneKey("zone", zone.Record.Desired.ID)})
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
			serviceDesiredCondition(dependency),
			etcdstore.Condition{Key: deletionTombstoneKey("service", dependency.Record.Desired.ID)},
		)
	}
	if len(wantZones) != 0 || len(wantDependencies) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Service mutation references are incomplete")
	}
	return conditions, nil
}

func classifyServiceMutationReferenceConflict(values []*etcdstore.KeyValue, references ServiceMutationReferences) error {
	if len(values) != len(references.Zones)+2*len(references.Dependencies) {
		return errs.New(errs.KindInternal, "Service reference compare evidence is incomplete")
	}
	offset := 0
	for range references.Zones {
		if values[offset] != nil {
			return errs.New(errs.KindResourceInUse, "Service Zone removal is in progress")
		}
		offset++
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
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record ServiceRecord,
) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
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

func validateServiceVersion(current etcdstore.Versioned[ServiceRecord]) error {
	if err := validateServiceRecord(current.Record); err != nil {
		return err
	}
	if current.Revision <= 0 || current.ReadRevision < current.Revision {
		return errs.New(errs.KindValidationFailed, "Service version metadata is invalid")
	}
	return nil
}

func classifyServiceWriteConflict(
	values []*etcdstore.KeyValue,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
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
