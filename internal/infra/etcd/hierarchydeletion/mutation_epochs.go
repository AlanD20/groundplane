package hierarchydeletion

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyCoordinationStore interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

type MutationScope struct {
	TenantID  string
	ProjectID string
}

type hierarchyMutationBinding struct {
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

func (binding hierarchyMutationBinding) Conditions() []etcdstore.Condition {
	return append([]etcdstore.Condition(nil), binding.conditions...)
}

// Mutations returns an independent slice whose value bytes remain borrowed.
// The caller must finish publication before Clear, or transfer byte ownership
// to the combined publication that clears them.
func (binding hierarchyMutationBinding) Mutations() []etcdstore.Mutation {
	return append([]etcdstore.Mutation(nil), binding.mutations...)
}

func EncodeInitialCoordination(
	targetKind HierarchyDeletionTargetKind,
	targetID string,
) ([]byte, error) {
	return EncodeHierarchyCoordination(HierarchyCoordinationRecord{
		Schema:        1,
		TargetKind:    targetKind,
		TargetID:      targetID,
		MutationEpoch: 1,
	})
}

// bindHierarchyMutation is the only mutation-plan seam for the ADR 0053
// ancestry epoch. Callers append their ordinary conditions and mutations,
// then this binder compares and increments each applicable ancestor epoch.
func BindMutationEpochs(
	ctx context.Context,
	store hierarchyCoordinationStore,
	revision int64,
	scope MutationScope,
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
		keys = append(keys, HierarchyCoordinationKey(string(HierarchyDeletionTargetTenant), scope.TenantID))
	}
	if scope.ProjectID != "" {
		keys = append(keys, HierarchyCoordinationKey(string(HierarchyDeletionTargetProject), scope.ProjectID))
	}
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return hierarchyMutationBinding{}, err
	}
	if result == nil || result.ReadRevision != revision || len(result.Values) != len(keys) {
		return hierarchyMutationBinding{}, CorruptHierarchyDeletion()
	}
	binding := hierarchyMutationBinding{
		conditions: append([]etcdstore.Condition(nil), conditions...),
		mutations:  etcdstore.CloneMutations(mutations),
		values:     make([][]byte, 0, len(keys)),
	}
	for index, key := range keys {
		value := result.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 {
			binding.Clear()
			return hierarchyMutationBinding{}, CorruptHierarchyDeletion()
		}
		record, decodeErr := DecodeHierarchyCoordination(value.Value)
		if decodeErr != nil {
			binding.Clear()
			return hierarchyMutationBinding{}, decodeErr
		}
		if HierarchyCoordinationKey(string(record.TargetKind), record.TargetID) != key {
			binding.Clear()
			return hierarchyMutationBinding{}, CorruptHierarchyDeletion()
		}
		record.MutationEpoch++
		encoded, encodeErr := EncodeHierarchyCoordination(record)
		if encodeErr != nil {
			binding.Clear()
			return hierarchyMutationBinding{}, encodeErr
		}
		binding.values = append(binding.values, encoded)
		binding.conditions = append(binding.conditions, etcdstore.Condition{Key: key, ModRevision: value.ModRevision})
		binding.mutations = append(binding.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded})
	}
	if err := ValidateHierarchyDeletionTransaction(binding.conditions, binding.mutations, etcdstore.MaximumOperations); err != nil {
		binding.Clear()
		return hierarchyMutationBinding{}, err
	}
	return binding, nil
}

func (binding *hierarchyMutationBinding) Clear() {
	if binding == nil {
		return
	}
	etcdstore.ClearMutationValues(binding.mutations)
	for _, value := range binding.values {
		clear(value)
	}
	binding.conditions = nil
	binding.mutations = nil
	binding.values = nil
}
