package environmentfence

import (
	"context"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type mutationContextStore interface {
	Get(context.Context, string) (*etcdstore.KeyValue, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

// MutationContext binds domain writes to one captured Environment fence.
type MutationContext struct {
	environmentID string
	readRevision  int64
	fence         Evidence
}

// MutationBinding retains fixed-revision compare evidence for publication.
type MutationBinding struct {
	context                *MutationContext
	conditions             []etcdstore.Condition
	mutations              []etcdstore.Mutation
	originalConditionCount int
	fenceIndexes           []int
	preparedReads          []*etcdstore.KeyValue
}

// MutationContext retains this evidence without exposing its private authority.
func (evidence Evidence) MutationContext() *MutationContext {
	return &MutationContext{environmentID: evidence.environmentID, readRevision: evidence.readRevision, fence: evidence}
}

func (mutationContext *MutationContext) ReadRevision() int64 {
	return mutationContext.readRevision
}

// Conditions returns the borrowed compares used by the publishing transaction.
func (binding *MutationBinding) Conditions() []etcdstore.Condition {
	return binding.conditions
}

// Mutations returns borrowed operations; the publisher owns clearing their bytes.
func (binding *MutationBinding) Mutations() []etcdstore.Mutation {
	return binding.mutations
}

func LoadMutationContext(
	ctx context.Context,
	store mutationContextStore,
	environmentID string,
	anchorKey string,
	projectID string,
	tenantID string,
) (*MutationContext, error) {
	anchor, err := store.Get(ctx, anchorKey)
	if err != nil {
		return nil, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 {
		return nil, errs.New(errs.KindInternal, "environment mutation anchor read is invalid")
	}
	evidence, err := LoadOrdinary(ctx, store, environmentID, anchor.ReadRevision)
	if err != nil {
		return nil, err
	}
	mutationContext := &MutationContext{
		environmentID: environmentID,
		readRevision:  anchor.ReadRevision,
		fence:         evidence,
	}
	if _, ok := mutationContext.RevisionForKey(hierarchyrecord.EnvironmentKey(environmentID)); !ok {
		return nil, errs.New(errs.KindInternal, "environment mutation fence omitted the environment")
	}
	if _, ok := mutationContext.RevisionForKey(hierarchyrecord.ProjectKey(projectID)); !ok {
		return nil, errs.New(errs.KindScopeUnauthorized, "environment mutation project scope is invalid")
	}
	if tenantID != "" {
		if _, ok := mutationContext.RevisionForKey(hierarchyrecord.TenantKey(tenantID)); !ok {
			return nil, errs.New(errs.KindScopeUnauthorized, "environment mutation tenant scope is invalid")
		}
	}
	return mutationContext, nil
}

func (mutationContext *MutationContext) RevisionForKey(key string) (int64, bool) {
	if mutationContext == nil {
		return 0, false
	}
	for _, condition := range mutationContext.fence.TransactionConditions() {
		if condition.Key == key && condition.ModRevision > 0 && !condition.Prefix {
			return condition.ModRevision, true
		}
	}
	return 0, false
}

func (mutationContext *MutationContext) VersionHierarchy(
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
) (*etcdstore.Versioned[hierarchyrecord.TenantRecord], etcdstore.Versioned[hierarchyrecord.ProjectRecord], etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	projectRevision, ok := mutationContext.RevisionForKey(hierarchyrecord.ProjectKey(project.Record.ID))
	if !ok {
		return nil, etcdstore.Versioned[hierarchyrecord.ProjectRecord]{}, etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]{}, errs.New(
			errs.KindScopeUnauthorized,
			"environment mutation project scope is invalid",
		)
	}
	environmentRevision, ok := mutationContext.RevisionForKey(hierarchyrecord.EnvironmentKey(environment.Record.ID))
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
	tenantRevision, ok := mutationContext.RevisionForKey(hierarchyrecord.TenantKey(tenant.Record.ID))
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

func (mutationContext *MutationContext) Bind(
	ctx context.Context,
	store environmentMutationFenceStore,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	advanceEpoch bool,
) (*MutationBinding, error) {
	binding, err := mutationContext.PrepareBinding(ctx, store, conditions, mutations, advanceEpoch)
	if err != nil {
		return nil, err
	}
	if err := ValidateTransactionBudget(binding.conditions, binding.mutations); err != nil {
		binding.Clear()
		return nil, err
	}
	return binding, nil
}

// PrepareBinding assembles fence evidence without choosing a transaction budget.
// Closed Task terminal publishers apply their own budget before publication.
func (mutationContext *MutationContext) PrepareBinding(
	ctx context.Context,
	store environmentMutationFenceStore,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
	advanceEpoch bool,
) (*MutationBinding, error) {
	if mutationContext == nil || mutationContext.readRevision <= 0 {
		return nil, errs.New(errs.KindInternal, "environment mutation context is invalid")
	}
	originalConditionCount := len(conditions)
	conditions = append([]etcdstore.Condition(nil), conditions...)
	fenceConditions := mutationContext.fence.TransactionConditions()
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
		epochMutation, err := mutationContext.fence.EpochRewriteMutation()
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
		etcdstore.ClearMutationValues(mutations)
		return nil, err
	}
	if prepared == nil || prepared.ReadRevision != mutationContext.readRevision ||
		len(prepared.Values) != len(conditions) {
		etcdstore.ClearMutationValues(mutations)
		return nil, errs.New(errs.KindInternal, "environment mutation domain read is invalid")
	}
	return &MutationBinding{
		context: mutationContext, conditions: conditions, mutations: mutations,
		originalConditionCount: originalConditionCount,
		fenceIndexes:           fenceIndexes,
		preparedReads:          prepared.Values,
	}, nil
}

func (binding *MutationBinding) Clear() {
	if binding == nil {
		return
	}
	etcdstore.ClearValues(binding.preparedReads)
	binding.preparedReads = nil
}

func (binding *MutationBinding) ClassifyConflict(
	revision int64,
	reads []*etcdstore.KeyValue,
	fallback func(int64, []*etcdstore.KeyValue) error,
) error {
	if binding == nil || binding.context == nil || fallback == nil ||
		len(reads) != len(binding.conditions) {
		return errs.New(errs.KindInternal, "environment mutation compare evidence is incomplete")
	}
	for index := range binding.originalConditionCount {
		if !etcdstore.ConditionMatchesRead(binding.conditions[index], reads[index]) {
			return fallback(revision, reads[:binding.originalConditionCount])
		}
	}
	fenceReads := make([]*etcdstore.KeyValue, len(binding.fenceIndexes))
	for index, conditionIndex := range binding.fenceIndexes {
		fenceReads[index] = reads[conditionIndex]
	}
	return binding.context.fence.ClassifyConflict(fenceReads)
}

func (binding *MutationBinding) PreparedConflict(
	fallback func(int64, []*etcdstore.KeyValue) error,
) error {
	if binding == nil || len(binding.preparedReads) != len(binding.conditions) {
		return errs.New(errs.KindInternal, "environment mutation prepared evidence is incomplete")
	}
	for index, condition := range binding.conditions {
		if !etcdstore.ConditionMatchesRead(condition, binding.preparedReads[index]) {
			return binding.ClassifyConflict(binding.context.readRevision, binding.preparedReads, fallback)
		}
	}
	return nil
}

func (binding *MutationBinding) preparedConditionsMatch() (bool, error) {
	if binding == nil || len(binding.preparedReads) != len(binding.conditions) {
		return false, errs.New(errs.KindInternal, "environment mutation prepared evidence is incomplete")
	}
	for index, condition := range binding.conditions {
		if !etcdstore.ConditionMatchesRead(condition, binding.preparedReads[index]) {
			return false, nil
		}
	}
	return true, nil
}
