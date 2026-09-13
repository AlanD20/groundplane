package etcd

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: release batching needs one inclusive predicate for the exact
// ordinary Store operation and serialized-request boundaries.
func TestTransactionBudgetFitsInclusiveOrdinaryLimits(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		budget TransactionBudget
		want   bool
	}{
		{name: "exact limits", budget: TransactionBudget{Operations: 96, Bytes: 1 << 20}, want: true},
		{name: "too many operations", budget: TransactionBudget{Operations: 97, Bytes: 1 << 20}},
		{name: "too many bytes", budget: TransactionBudget{Operations: 96, Bytes: (1 << 20) + 1}},
		{name: "negative operations", budget: TransactionBudget{Operations: -1}},
		{name: "negative bytes", budget: TransactionBudget{Bytes: -1}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := test.budget.Fits(); got != test.want {
				t.Fatalf("TransactionBudget%+v.Fits() = %t, want %t", test.budget, got, test.want)
			}
		})
	}
}

// Rationale: deciding whether the next release member fits must count every
// compare and mutation without creating or committing an etcd transaction.
func TestStoreMeasureTransactionCountsWithoutExecution(t *testing.T) {
	t.Parallel()

	backend := &fakeClient{}
	store, err := newStore(backend, "/groundplane/")
	if err != nil {
		t.Fatalf("newStore() error = %v", err)
	}
	conditions := make([]Condition, maximumTransactionOperations-1)
	for index := range conditions {
		conditions[index] = Condition{Key: fmt.Sprintf("/conditions/%02d", index)}
	}
	mutations := []Mutation{{Type: MutationDelete, Key: "/records/one"}}

	budget, err := store.MeasureTransaction(context.Background(), conditions, mutations)
	if err != nil {
		t.Fatalf("MeasureTransaction(96 operations) error = %v", err)
	}
	if budget.Operations != maximumTransactionOperations || !budget.Fits() {
		t.Fatalf("MeasureTransaction(96 operations) = %+v, want fitting", budget)
	}
	conditions = append(conditions, Condition{Key: "/conditions/overflow"})
	budget, err = store.MeasureTransaction(context.Background(), conditions, mutations)
	if err != nil {
		t.Fatalf("MeasureTransaction(97 operations) error = %v", err)
	}
	if budget.Operations != maximumTransactionOperations+1 || budget.Fits() {
		t.Fatalf("MeasureTransaction(97 operations) = %+v, want non-fitting", budget)
	}
	if backend.transaction != nil {
		t.Fatal("MeasureTransaction() reached etcd")
	}
}

// Rationale: batching must use protobuf bytes for physical prefixed keys, so
// exact-size values fit while one extra byte or a large configured prefix does not.
func TestMeasureTransactionBudgetUsesPhysicalSerializedBytes(t *testing.T) {
	t.Parallel()

	mutation := Mutation{Type: MutationPut, Key: "/records/large", Value: make([]byte, maximumTransactionBytes)}
	budget, err := MeasureTransactionBudget(
		context.Background(),
		"/groundplane/",
		nil,
		[]Mutation{mutation},
	)
	if err != nil {
		t.Fatalf("MeasureTransactionBudget(initial) error = %v", err)
	}
	excess := budget.Bytes - maximumTransactionBytes
	if excess <= 0 || excess >= len(mutation.Value) {
		t.Fatalf("initial serialized excess = %d for budget %+v", excess, budget)
	}
	mutation.Value = mutation.Value[:len(mutation.Value)-excess]
	budget, err = MeasureTransactionBudget(context.Background(), "/groundplane/", nil, []Mutation{mutation})
	if err != nil {
		t.Fatalf("MeasureTransactionBudget(exact) error = %v", err)
	}
	if budget.Bytes != maximumTransactionBytes || !budget.Fits() {
		t.Fatalf("exact serialized budget = %+v, want %d fitting bytes", budget, maximumTransactionBytes)
	}
	mutation.Value = append(mutation.Value, 0)
	budget, err = MeasureTransactionBudget(context.Background(), "/groundplane/", nil, []Mutation{mutation})
	if err != nil {
		t.Fatalf("MeasureTransactionBudget(oversize) error = %v", err)
	}
	if budget.Bytes != maximumTransactionBytes+1 || budget.Fits() {
		t.Fatalf("oversized serialized budget = %+v, want %d non-fitting bytes", budget, maximumTransactionBytes+1)
	}

	largePrefix := "/" + strings.Repeat("p", maximumTransactionBytes) + "/"
	budget, err = MeasureTransactionBudget(context.Background(), largePrefix, nil, []Mutation{{
		Type: MutationDelete,
		Key:  "/records/one",
	}})
	if err != nil {
		t.Fatalf("MeasureTransactionBudget(large prefix) error = %v", err)
	}
	if budget.Bytes <= maximumTransactionBytes || budget.Fits() {
		t.Fatalf("large-prefix serialized budget = %+v, want non-fitting", budget)
	}
}

// Rationale: measurement is a validation boundary and must reject every
// malformed transaction shape that execution rejects before contacting etcd.
func TestMeasureTransactionBudgetRejectsMalformedInputs(t *testing.T) {
	t.Parallel()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	tests := []struct {
		name       string
		ctx        context.Context
		keyPrefix  string
		conditions []Condition
		mutations  []Mutation
	}{
		{
			name:      "nil context",
			keyPrefix: "/groundplane/",
			mutations: []Mutation{{Type: MutationDelete, Key: "/record"}},
		},
		{
			name:      "canceled context",
			ctx:       canceled,
			keyPrefix: "/groundplane/",
			mutations: []Mutation{{Type: MutationDelete, Key: "/record"}},
		},
		{
			name:      "invalid key prefix",
			ctx:       context.Background(),
			keyPrefix: "groundplane/",
			mutations: []Mutation{{Type: MutationDelete, Key: "/record"}},
		},
		{
			name:       "condition key",
			ctx:        context.Background(),
			keyPrefix:  "/groundplane/",
			conditions: []Condition{{Key: "record"}},
		},
		{
			name:       "negative revision",
			ctx:        context.Background(),
			keyPrefix:  "/groundplane/",
			conditions: []Condition{{Key: "/record", ModRevision: -1}},
		},
		{
			name:       "revisioned prefix condition",
			ctx:        context.Background(),
			keyPrefix:  "/groundplane/",
			conditions: []Condition{{Key: "/records/", ModRevision: 1, Prefix: true}},
		},
		{
			name:      "mutation key",
			ctx:       context.Background(),
			keyPrefix: "/groundplane/",
			mutations: []Mutation{{Type: MutationDelete, Key: "record"}},
		},
		{
			name:      "prefix put",
			ctx:       context.Background(),
			keyPrefix: "/groundplane/",
			mutations: []Mutation{{Type: MutationPut, Key: "/records/", Prefix: true}},
		},
		{
			name:      "mutation type",
			ctx:       context.Background(),
			keyPrefix: "/groundplane/",
			mutations: []Mutation{{Key: "/record"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := MeasureTransactionBudget(
				test.ctx,
				test.keyPrefix,
				test.conditions,
				test.mutations,
			); err == nil {
				t.Fatal("MeasureTransactionBudget() error = nil")
			} else if test.name != "canceled context" && test.name != "nil context" &&
				!isKind(err, errs.KindValidationFailed) {
				t.Fatalf("MeasureTransactionBudget() error = %v, want validation", err)
			}
		})
	}
}
