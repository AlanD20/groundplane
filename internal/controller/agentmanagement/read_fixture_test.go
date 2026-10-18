package agentmanagement

import (
	"bytes"
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func agentReadFixture(t *testing.T, slots int) (*agentReadStore, *workqueue.Repository) {
	t.Helper()
	store := &agentReadStore{values: map[string]keyvalue.KeyValue{}}
	binding := hostbindings.Binding{HostID: ids.New(ids.KindHost), AgentID: runtimeAdapterAgentID}
	change, err := hostbindings.PrepareCreation(binding)
	if err != nil {
		t.Fatal(err)
	}
	for _, mutation := range change.Mutations {
		store.values[mutation.Key] = keyvalue.KeyValue{Key: mutation.Key, ModRevision: 1, Value: mutation.Value}
	}
	for index := 0; index < slots; index++ {
		key := workqueue.SlotKey(
			workexecution.Identity{Executor: workexecution.Agent, HostID: binding.HostID},
			uint16(index),
		)
		raw, err := recordcodec.Encode("execution_slot", workqueue.Slot{
			TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem), AttemptID: ids.New(ids.KindAttempt),
		})
		if err != nil {
			t.Fatal(err)
		}
		store.values[key] = keyvalue.KeyValue{Key: key, ModRevision: 1, Value: raw}
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	return store, queue
}

type agentReadStore struct{ values map[string]keyvalue.KeyValue }

func (store *agentReadStore) GetMany(
	_ context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	if request.Revision != 0 && request.Revision != 1 {
		return nil, errs.New(errs.KindStateConflict, "unexpected read revision")
	}
	result := &keyvalue.GetManyResult{
		ReadRevision:     1,
		ResponseRevision: 1,
		Values:           make([]*keyvalue.KeyValue, len(request.Keys)),
	}
	for index, key := range request.Keys {
		if value, found := store.values[key]; found {
			value.Value = bytes.Clone(value.Value)
			result.Values[index] = &value
		}
	}
	return result, nil
}

func (store *agentReadStore) Range(_ context.Context, request keyvalue.RangeRequest) (*keyvalue.RangeResult, error) {
	if request.Revision != 1 {
		return nil, errs.New(errs.KindStateConflict, "capacity was not read at host-binding revision")
	}
	result := &keyvalue.RangeResult{ReadRevision: 1, ResponseRevision: 1}
	for key, value := range store.values {
		if strings.HasPrefix(key, request.Prefix) {
			value.Value = bytes.Clone(value.Value)
			result.Values = append(result.Values, value)
		}
	}
	slices.SortFunc(result.Values, func(a, b keyvalue.KeyValue) int { return strings.Compare(a.Key, b.Key) })
	return result, nil
}

func (*agentReadStore) MeasureTransaction(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionBudget, error) {
	return keyvalue.TransactionBudget{}, errs.New(errs.KindInternal, "unexpected write in read-only Agent fixture")
}

func (*agentReadStore) Transact(
	context.Context,
	[]keyvalue.Condition,
	[]keyvalue.Mutation,
) (keyvalue.TransactionResult, error) {
	return keyvalue.TransactionResult{}, errs.New(errs.KindInternal, "unexpected write in read-only Agent fixture")
}
