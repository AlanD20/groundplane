package etcd

import (
	"context"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyCoordinationStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

type HierarchyMutationScope struct {
	TenantID  string
	ProjectID string
}

type hierarchyMutationBinding struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func encodeInitialHierarchyCoordination(
	targetKind hierarchydeletion.HierarchyDeletionTargetKind,
	targetID string,
) ([]byte, error) {
	return hierarchydeletion.EncodeHierarchyCoordination(hierarchydeletion.HierarchyCoordinationRecord{
		Schema:        1,
		TargetKind:    targetKind,
		TargetID:      targetID,
		MutationEpoch: 1,
	})
}

// bindHierarchyMutation is the only mutation-plan seam for the ADR 0053
// ancestry epoch. Callers append their ordinary conditions and mutations,
// then this binder compares and increments each applicable ancestor epoch.
func bindHierarchyMutation(
	ctx context.Context,
	store hierarchyCoordinationStore,
	revision int64,
	scope HierarchyMutationScope,
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (hierarchyMutationBinding, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return hierarchyMutationBinding{}, err
	}
	if store == nil || revision <= 0 || scope.ProjectID == "" && scope.TenantID == "" {
		return hierarchyMutationBinding{}, errs.New(errs.KindValidationFailed, "hierarchy mutation scope is invalid")
	}
	keys := make([]string, 0, 2)
	if scope.TenantID != "" {
		keys = append(keys, hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetTenant), scope.TenantID))
	}
	if scope.ProjectID != "" {
		keys = append(keys, hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetProject), scope.ProjectID))
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return hierarchyMutationBinding{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return hierarchyMutationBinding{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	binding := hierarchyMutationBinding{
		conditions: append([]etcdstore.Condition(nil), conditions...),
		mutations:  cloneMutations(mutations),
		values:     make([][]byte, 0, len(keys)),
	}
	for index, key := range keys {
		value := result.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 {
			binding.clear()
			return hierarchyMutationBinding{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record, decodeErr := hierarchydeletion.DecodeHierarchyCoordination(value.Value)
		if decodeErr != nil {
			binding.clear()
			return hierarchyMutationBinding{}, decodeErr
		}
		if hierarchydeletion.HierarchyCoordinationKey(string(record.TargetKind), record.TargetID) != key {
			binding.clear()
			return hierarchyMutationBinding{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		record.MutationEpoch++
		encoded, encodeErr := hierarchydeletion.EncodeHierarchyCoordination(record)
		if encodeErr != nil {
			binding.clear()
			return hierarchyMutationBinding{}, encodeErr
		}
		binding.values = append(binding.values, encoded)
		binding.conditions = append(binding.conditions, etcdstore.Condition{Key: key, ModRevision: value.ModRevision})
		binding.mutations = append(binding.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded})
	}
	if err := hierarchydeletion.ValidateHierarchyDeletionTransaction(binding.conditions, binding.mutations, etcdstore.MaximumOperations); err != nil {
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
