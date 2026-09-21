package etcd

import (
	"context"
	"fmt"
	"testing"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

// Rationale: Store.Transact is the final guard against requests exceeding the
// accepted aggregate operation or physical serialized request ceilings.
func TestStoreTransactEnforcesExactRequestCeilings(t *testing.T) {
	t.Parallel()

	half := testkeyvalue.MaximumOperations / 2
	conditions := make([]testkeyvalue.Condition, half)
	mutations := make([]testkeyvalue.Mutation, half)
	for index := range half {
		conditions[index] = testkeyvalue.Condition{
			Key:         fmt.Sprintf("/conditions/%02d", index),
			ModRevision: int64(index + 1),
		}
		mutations[index] = testkeyvalue.Mutation{
			Type: testkeyvalue.MutationDelete,
			Key:  fmt.Sprintf("/records/%02d", index),
		}
	}
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 12}, Succeeded: true,
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}
	if _, err := store.Transact(context.Background(), conditions, mutations); err != nil {
		t.Fatalf("Transact(%d operations) error = %v", testkeyvalue.MaximumOperations, err)
	}
	if len(backend.transaction.otherwise) != len(conditions) {
		t.Fatalf("failure reads = %d, want %d", len(backend.transaction.otherwise), len(conditions))
	}

	backend.transaction = nil
	conditions = append(
		conditions,
		testkeyvalue.Condition{Key: fmt.Sprintf("/conditions/%02d", half), ModRevision: int64(half + 1)},
	)
	if _, err := store.Transact(context.Background(), conditions, mutations); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("Transact(%d operations) error = %v, want validation", testkeyvalue.MaximumOperations+1, err)
	}
	if backend.transaction != nil {
		t.Fatalf("Transact(%d operations) reached etcd", testkeyvalue.MaximumOperations+1)
	}

	backend.transaction = nil
	tooLarge := []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: "/records/large", Value: make([]byte, testkeyvalue.MaximumBytes)},
	}
	if _, err := store.Transact(context.Background(), nil, tooLarge); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("Transact(oversize) error = %v, want validation", err)
	}
	if backend.transaction != nil {
		t.Fatal("Transact(oversize) reached etcd")
	}
}

// Rationale: final authored Blueprints use their dedicated per-arm executor,
// while every ordinary transaction remains subject to the aggregate 96 bound.
func TestEnvironmentBlueprintExecutorUsesUnifiedEnvelopeWithoutWideningStore(t *testing.T) {
	t.Parallel()
	conditions := make([]testkeyvalue.Condition, 143)
	mutations := make([]testkeyvalue.Mutation, 160)
	for index := range conditions {
		conditions[index] = testkeyvalue.Condition{Key: fmt.Sprintf("/blueprint/conditions/%03d", index)}
	}
	for index := range mutations {
		mutations[index] = testkeyvalue.Mutation{
			Type: testkeyvalue.MutationDelete,
			Key:  fmt.Sprintf("/blueprint/mutations/%03d", index),
		}
	}
	backend := &fakeClient{transactionResponse: &clientv3.TxnResponse{
		Header: &etcdserverpb.ResponseHeader{Revision: 33}, Succeeded: true,
	}}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := executeEnvironmentBlueprintTransaction(context.Background(), store, conditions, mutations); err != nil {
		t.Fatalf("Blueprint Transact(143/160/143) error = %v", err)
	}
	backend.transaction = nil
	if _, err := store.Transact(context.Background(), conditions, mutations); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("ordinary Transact(303 operations) error = %v, want validation", err)
	}
	if backend.transaction != nil {
		t.Fatal("ordinary oversized transaction reached etcd")
	}
	tooMany := make([]testkeyvalue.Condition, maximumEnvironmentBlueprintTransactionOperationsPerArm+1)
	for index := range tooMany {
		tooMany[index] = testkeyvalue.Condition{Key: fmt.Sprintf("/blueprint/too-many/%03d", index)}
	}
	if _, err := executeEnvironmentBlueprintTransaction(context.Background(), store, tooMany, mutations[:1]); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("Blueprint Transact(257 comparisons) error = %v, want validation", err)
	}
	maximum := make([]testkeyvalue.Condition, maximumEnvironmentBlueprintTransactionOperationsPerArm)
	for index := range maximum {
		maximum[index] = testkeyvalue.Condition{Key: fmt.Sprintf("/blueprint/maximum/%03d", index)}
	}
	backend.transaction = nil
	if _, err := executeEnvironmentBlueprintTransaction(context.Background(), store, maximum, mutations[:1]); err != nil {
		t.Fatalf("Blueprint Transact(256 comparisons) error = %v", err)
	}
	backend.transaction = nil
	large := []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: "/blueprint/large", Value: make([]byte, testkeyvalue.MaximumBytes)},
	}
	if _, err := executeEnvironmentBlueprintTransaction(context.Background(), store, nil, large); !isKind(
		err,
		errs.KindValidationFailed,
	) {
		t.Fatalf("Blueprint Transact(oversize) error = %v, want validation", err)
	}
	if backend.transaction != nil {
		t.Fatal("oversized Blueprint transaction reached etcd")
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
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{
		{Key: "/records/one", ModRevision: 7},
		{Key: "/records/two", ModRevision: 8},
	}, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationDelete, Key: "/records/one"}})
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
