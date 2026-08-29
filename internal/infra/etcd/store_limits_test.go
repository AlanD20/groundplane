package etcd

import (
	"context"
	"fmt"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Rationale: Store.Transact is the final guard against requests exceeding the
// accepted aggregate operation or physical serialized request ceilings.
func TestStoreTransactEnforcesExactRequestCeilings(t *testing.T) {
	t.Parallel()

	half := maximumTransactionOperations / 2
	conditions := make([]Condition, half)
	mutations := make([]Mutation, half)
	for index := range half {
		conditions[index] = Condition{Key: fmt.Sprintf("/conditions/%02d", index), ModRevision: int64(index + 1)}
		mutations[index] = Mutation{Type: MutationDelete, Key: fmt.Sprintf("/records/%02d", index)}
	}
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 12}, Succeeded: true,
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}
	if _, err := store.Transact(context.Background(), conditions, mutations); err != nil {
		t.Fatalf("Transact(%d operations) error = %v", maximumTransactionOperations, err)
	}
	if len(backend.transaction.otherwise) != len(conditions) {
		t.Fatalf("failure reads = %d, want %d", len(backend.transaction.otherwise), len(conditions))
	}

	backend.transaction = nil
	conditions = append(conditions, Condition{Key: fmt.Sprintf("/conditions/%02d", half), ModRevision: int64(half + 1)})
	if _, err := store.Transact(context.Background(), conditions, mutations); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("Transact(%d operations) error = %v, want validation", maximumTransactionOperations+1, err)
	}
	if backend.transaction != nil {
		t.Fatalf("Transact(%d operations) reached etcd", maximumTransactionOperations+1)
	}

	backend.transaction = nil
	tooLarge := []Mutation{{Type: MutationPut, Key: "/records/large", Value: make([]byte, maximumTransactionBytes)}}
	if _, err := store.Transact(context.Background(), nil, tooLarge); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("Transact(oversize) error = %v, want validation", err)
	}
	if backend.transaction != nil {
		t.Fatal("Transact(oversize) reached etcd")
	}
}

// Rationale: a failed compare must expose exact-key values in condition order
// from the transaction revision, including an explicit nil for absence.
func TestStoreTransactReturnsSameRevisionFailureReads(t *testing.T) {
	t.Parallel()

	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 22},
		Responses: []*etcdserverpb.ResponseOp{
			{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{
				Kvs: []*mvccpb.KeyValue{{
					Key: []byte("/groundplane/records/one"), Value: []byte("value"), ModRevision: 19,
				}},
			}}},
			{Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: &etcdserverpb.RangeResponse{}}},
		},
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}
	result, err := store.Transact(context.Background(), []Condition{
		{Key: "/records/one", ModRevision: 7},
		{Key: "/records/two", ModRevision: 8},
	}, []Mutation{{Type: MutationDelete, Key: "/records/one"}})
	if err != nil {
		t.Fatalf("Transact() error = %v", err)
	}
	if result.Succeeded || result.Revision != 22 || len(result.FailureReads) != 2 ||
		result.FailureReads[0] == nil || result.FailureReads[0].Key != "/records/one" ||
		string(result.FailureReads[0].Value) != "value" || result.FailureReads[0].ModRevision != 19 ||
		result.FailureReads[1] != nil {
		t.Fatalf("Transact() failure result = %#v", result)
	}
}
