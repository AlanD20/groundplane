package hierarchydeletionfinalization

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

// OWN-05/SVC-05: hierarchy deletion must retire only the selected Service's
// applied runtime in the same commit as its runtime metadata. An unrelated
// Service and immutable Release history are independent retention owners.
// Earlier workload-removal proof is supplied; this checks metadata settlement.
func TestServiceFinalizerRemovesOnlyOwnedRuntimeAndPreservesHistory(t *testing.T) {
	for _, raced := range []bool{false, true} {
		t.Run(map[bool]string{false: "removed", true: "lifecycle raced"}[raced], func(t *testing.T) {
			serviceID, otherID := ids.New(ids.KindService), ids.New(ids.KindService)
			environmentID, releaseID := ids.New(ids.KindEnvironment), ids.New(ids.KindDeployment)
			runtimeKey, appliedKey := services.ServiceRuntimeKey(serviceID), serviceruntimerecord.Key(serviceID)
			otherKey, historyKey := serviceruntimerecord.Key(otherID), releases.ReleaseTerminalKey(releaseID)
			value, err := services.EncodeServiceRuntimeRecord(services.ServiceRuntimeRecord{
				EnvironmentID: environmentID, ServiceID: serviceID,
				Runtime: core.ServiceRuntime{ServiceID: serviceID, RuntimeIntent: core.ServiceRuntimeIntentRunning}})
			if err != nil {
				t.Fatal(err)
			}
			store := &serviceFinalizationStore{values: map[string]*keyvalue.KeyValue{}}
			for key, raw := range map[string][]byte{runtimeKey: value, appliedKey: []byte("captured selected runtime"),
				otherKey: []byte("unrelated runtime"), historyKey: []byte("immutable successful Release")} {
				store.values[key] = &keyvalue.KeyValue{Key: key, Value: slices.Clone(raw), ModRevision: 1}
			}
			effects, err := NewPreparer(store).Prepare(t.Context(), hierarchydeletion.HierarchyDeletionOperation{},
				hierarchydeletion.HierarchyDeletionAction{TargetID: serviceID, TargetRevision: 1,
					ActionKind: hierarchydeletion.HierarchyDeletionServiceRemove})
			if err != nil {
				t.Fatal(err)
			}
			defer keyvalue.ClearByteSlices(effects.Values())
			if raced {
				key := services.ServiceLifecycleActiveKey(serviceID)
				store.values[key] = &keyvalue.KeyValue{Key: key, Value: []byte("new operation"), ModRevision: 2}
			}
			result, err := store.Transact(t.Context(), effects.Conditions(), effects.Mutations())
			if err != nil || result.Succeeded == raced {
				t.Fatalf("finalizer commit=%v error=%v", result.Succeeded, err)
			}
			for _, key := range []string{runtimeKey, appliedKey} {
				if (store.values[key] != nil) != raced {
					t.Fatal("runtime metadata was only partially retired")
				}
			}
			if !slices.Equal(store.values[otherKey].Value, []byte("unrelated runtime")) ||
				!slices.Equal(store.values[historyKey].Value, []byte("immutable successful Release")) {
				t.Fatal("finalizer changed unrelated runtime or successful history")
			}
		})
	}
}

type serviceFinalizationStore struct{ values map[string]*keyvalue.KeyValue }

func (store *serviceFinalizationStore) Get(_ context.Context, key string) (*keyvalue.GetResult, error) {
	result := &keyvalue.GetResult{}
	if value := store.values[key]; value != nil {
		cloned := *value
		cloned.Value = slices.Clone(value.Value)
		result.Entry = &cloned
	}
	return result, nil
}

func (store *serviceFinalizationStore) GetMany(
	ctx context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if len(request.Keys) == 0 {
		return nil, errors.New("empty multi-get")
	}
	result := &keyvalue.GetManyResult{ReadRevision: 1, Values: make([]*keyvalue.KeyValue, len(request.Keys))}
	for index, key := range request.Keys {
		value, err := store.Get(ctx, key)
		if err != nil {
			return nil, err
		}
		result.Values[index] = value.Entry
	}
	return result, nil
}

func (store *serviceFinalizationStore) Range(
	_ context.Context,
	request keyvalue.RangeRequest,
) (*keyvalue.RangeResult, error) {
	result := &keyvalue.RangeResult{ReadRevision: 1}
	for key, value := range store.values {
		if strings.HasPrefix(key, request.Prefix) {
			cloned := *value
			cloned.Value = slices.Clone(value.Value)
			result.Values = append(result.Values, cloned)
		}
	}
	return result, nil
}

func (store *serviceFinalizationStore) Transact(
	_ context.Context,
	conditions []keyvalue.Condition,
	mutations []keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	for _, condition := range conditions {
		var revision int64
		if condition.Prefix {
			for key := range store.values {
				if strings.HasPrefix(key, condition.Key) {
					revision = 1
					break
				}
			}
		} else if value := store.values[condition.Key]; value != nil {
			revision = value.ModRevision
		}
		if revision != condition.ModRevision {
			return keyvalue.TransactionResult{}, nil
		}
	}
	for _, mutation := range mutations {
		if mutation.Type == keyvalue.MutationDelete {
			delete(store.values, mutation.Key)
		} else {
			store.values[mutation.Key] = &keyvalue.KeyValue{Key: mutation.Key, Value: slices.Clone(mutation.Value), ModRevision: 3}
		}
	}
	return keyvalue.TransactionResult{Succeeded: true, Revision: 3}, nil
}
