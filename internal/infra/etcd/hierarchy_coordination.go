package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyCoordinationStore interface {
	GetMany(context.Context, GetManyRequest) (*GetManyResult, error)
}

type HierarchyMutationScope struct {
	TenantID  string
	ProjectID string
}

type hierarchyMutationBinding struct {
	conditions []Condition
	mutations  []Mutation
	values     [][]byte
}

// bindHierarchyMutation is the only mutation-plan seam for the ADR 0053
// ancestry epoch. Callers append their ordinary conditions and mutations,
// then this binder compares and increments each applicable ancestor epoch.
func bindHierarchyMutation(
	ctx context.Context,
	store hierarchyCoordinationStore,
	revision int64,
	scope HierarchyMutationScope,
	conditions []Condition,
	mutations []Mutation,
) (hierarchyMutationBinding, error) {
	if err := validateContext(ctx); err != nil {
		return hierarchyMutationBinding{}, err
	}
	if store == nil || revision <= 0 || scope.ProjectID == "" && scope.TenantID == "" {
		return hierarchyMutationBinding{}, errs.New(errs.KindValidationFailed, "hierarchy mutation scope is invalid")
	}
	keys := make([]string, 0, 2)
	if scope.TenantID != "" {
		keys = append(keys, HierarchyCoordinationKey(string(HierarchyDeletionTargetTenant), scope.TenantID))
	}
	if scope.ProjectID != "" {
		keys = append(keys, HierarchyCoordinationKey(string(HierarchyDeletionTargetProject), scope.ProjectID))
	}
	result, err := store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return hierarchyMutationBinding{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return hierarchyMutationBinding{}, corruptHierarchyDeletion()
	}
	binding := hierarchyMutationBinding{
		conditions: append([]Condition(nil), conditions...),
		mutations:  cloneMutations(mutations),
		values:     make([][]byte, 0, len(keys)),
	}
	for index, key := range keys {
		value := result.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 {
			binding.clear()
			return hierarchyMutationBinding{}, corruptHierarchyDeletion()
		}
		record, decodeErr := decodeHierarchyCoordination(value.Value)
		if decodeErr != nil {
			binding.clear()
			return hierarchyMutationBinding{}, decodeErr
		}
		record.MutationEpoch++
		encoded, encodeErr := encodeHierarchyCoordination(record)
		if encodeErr != nil {
			binding.clear()
			return hierarchyMutationBinding{}, encodeErr
		}
		binding.values = append(binding.values, encoded)
		binding.conditions = append(binding.conditions, Condition{Key: key, ModRevision: value.ModRevision})
		binding.mutations = append(binding.mutations, Mutation{Type: MutationPut, Key: key, Value: encoded})
	}
	if err := validateHierarchyDeletionTransaction(binding.conditions, binding.mutations, maximumTransactionOperations); err != nil {
		binding.clear()
		return hierarchyMutationBinding{}, err
	}
	return binding, nil
}

func (binding *hierarchyMutationBinding) clear() {
	if binding == nil {
		return
	}
	clearMutationValues(binding.mutations)
	for _, value := range binding.values {
		clear(value)
	}
	binding.conditions = nil
	binding.mutations = nil
	binding.values = nil
}
