package app

import (
	"bytes"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the app-owned Blueprint fixture must read one coherent historical
// MVCC view while retaining exact-key order and independent byte ownership.
func TestBlueprintTestStoreHistoricalGetManyAndReadCloning(t *testing.T) {
	store := newBlueprintTestStore()
	input := []byte("one-v1")
	firstRevision, err := store.Put(t.Context(), "/items/one", input)
	if err != nil {
		t.Fatal(err)
	}
	input[0] = 'X'
	if _, err := store.Put(t.Context(), "/items/two", []byte("two-v1")); err != nil {
		t.Fatal(err)
	}
	latestRevision, err := store.Put(t.Context(), "/items/one", []byte("one-v2"))
	if err != nil {
		t.Fatal(err)
	}

	historical, err := store.GetMany(t.Context(), etcd.GetManyRequest{
		Keys: []string{"/items/one", "/items/missing", "/items/two"}, Revision: firstRevision,
	})
	if err != nil {
		t.Fatal(err)
	}
	if historical.ReadRevision != firstRevision || historical.ResponseRevision != latestRevision ||
		len(historical.Values) != 3 || historical.Values[0] == nil ||
		string(historical.Values[0].Value) != "one-v1" || historical.Values[1] != nil || historical.Values[2] != nil {
		t.Fatalf("historical GetMany = %#v", historical)
	}
	historical.Values[0].Value[0] = 'Y'
	current, err := store.Get(t.Context(), "/items/one")
	if err != nil {
		t.Fatal(err)
	}
	if current.ReadRevision != latestRevision || current.Entry == nil || string(current.Entry.Value) != "one-v2" {
		t.Fatalf("current Get = %#v", current)
	}
	current.Entry.Value[0] = 'Z'
	again, err := store.Get(t.Context(), "/items/one")
	if err != nil || again.Entry == nil || string(again.Entry.Value) != "one-v2" {
		t.Fatalf("cloned Get = %#v, %v", again, err)
	}
	for _, request := range []etcd.GetManyRequest{{}, {Keys: []string{"/items/one"}, Revision: -1}, {
		Keys: []string{"/items/one"}, Revision: latestRevision + 1,
	}} {
		if _, err := store.GetMany(t.Context(), request); err == nil {
			t.Fatalf("GetMany(%#v) error = nil", request)
		}
	}
}

// Rationale: fixed-revision range pagination is the durable traversal contract;
// ordering, cursors, and More must not drift between ascending and descending reads.
func TestBlueprintTestStoreHistoricalRangeOrderingAndMore(t *testing.T) {
	store := newBlueprintTestStore()
	for _, key := range []string{"/items/c", "/items/a", "/other/x", "/items/b"} {
		if _, err := store.Put(t.Context(), key, []byte(key)); err != nil {
			t.Fatal(err)
		}
	}
	view := store.currentRevision()
	if _, err := store.Delete(t.Context(), "/items/b"); err != nil {
		t.Fatal(err)
	}

	ascending, err := store.Range(t.Context(), etcd.RangeRequest{Prefix: "/items/", Limit: 2, Revision: view})
	if err != nil {
		t.Fatal(err)
	}
	if keysOf(ascending.Values) != "/items/a,/items/b" || !ascending.More ||
		ascending.ReadRevision != view || ascending.ResponseRevision != store.currentRevision() {
		t.Fatalf("ascending Range = %#v", ascending)
	}
	descending, err := store.Range(t.Context(), etcd.RangeRequest{
		Prefix: "/items/", StartExclusive: "/items/c", Limit: 2, Revision: view, Descending: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if keysOf(descending.Values) != "/items/b,/items/a" || descending.More {
		t.Fatalf("descending Range = %#v", descending)
	}
	descending.Values[0].Value[0] = 'X'
	again, err := store.Range(t.Context(), etcd.RangeRequest{Prefix: "/items/", Limit: 3, Revision: view})
	if err != nil || len(again.Values) != 3 || string(again.Values[1].Value) != "/items/b" {
		t.Fatalf("cloned Range = %#v, %v", again, err)
	}
	for _, request := range []etcd.RangeRequest{
		{Prefix: "/items/", Limit: 0},
		{Prefix: "/items/", Limit: 1, Revision: -1},
		{Prefix: "/items/", StartExclusive: "/other/x", Limit: 1},
		{Prefix: "/items/", Limit: 1, Revision: store.currentRevision() + 1},
	} {
		if _, err := store.Range(t.Context(), request); err == nil {
			t.Fatalf("Range(%#v) error = nil", request)
		}
	}
}

// Rationale: compare loss must return condition-aligned evidence from one
// snapshot, including the first lexicographic child for an occupied prefix.
func TestBlueprintTestStoreCASFailureReadsAreAligned(t *testing.T) {
	store := newBlueprintTestStore()
	if _, err := store.Put(t.Context(), "/exact", []byte("exact")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "/prefix/b", []byte("b")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "/prefix/a", []byte("a")); err != nil {
		t.Fatal(err)
	}
	revision := store.currentRevision()
	result, err := store.Transact(t.Context(), []etcd.Condition{
		{Key: "/missing", ModRevision: 0},
		{Key: "/prefix/", ModRevision: 0, Prefix: true},
		{Key: "/exact", ModRevision: 1},
	}, []etcd.Mutation{{Type: etcd.MutationPut, Key: "/should-not-exist", Value: []byte("no")}})
	if err != nil {
		t.Fatal(err)
	}
	if result.Succeeded || result.Revision != revision || len(result.FailureReads) != 3 ||
		result.FailureReads[0] != nil || result.FailureReads[1] == nil ||
		result.FailureReads[1].Key != "/prefix/a" || result.FailureReads[2] == nil ||
		result.FailureReads[2].Key != "/exact" {
		t.Fatalf("failed transaction = %#v", result)
	}
	result.FailureReads[1].Value[0] = 'X'
	read, err := store.Get(t.Context(), "/prefix/a")
	if err != nil || read.Entry == nil || string(read.Entry.Value) != "a" {
		t.Fatalf("failure read was not cloned: %#v, %v", read, err)
	}
	missing, err := store.Get(t.Context(), "/should-not-exist")
	if err != nil || missing.Entry != nil {
		t.Fatalf("compare loss mutated state: %#v, %v", missing, err)
	}
}

// Rationale: a fake persistence transaction must validate its complete write
// set before mutation and assign one MVCC revision to every changed key.
func TestBlueprintTestStoreTransactionsAreAtomic(t *testing.T) {
	store := newBlueprintTestStore()
	before := store.currentRevision()
	if _, err := store.Transact(t.Context(), nil, nil); !isErrorKind(err, errs.KindValidationFailed) {
		t.Fatalf("empty transaction error = %v", err)
	}
	putValue := []byte("put")
	if _, err := store.Transact(t.Context(), nil, []etcd.Mutation{
		{Type: etcd.MutationPut, Key: "/atomic/one", Value: putValue},
		{Type: etcd.MutationType(99), Key: "/atomic/two"},
	}); !isErrorKind(err, errs.KindValidationFailed) {
		t.Fatalf("invalid transaction error = %v", err)
	}
	if store.currentRevision() != before {
		t.Fatalf("invalid transaction revision = %d, want %d", store.currentRevision(), before)
	}
	putValue[0] = 'X'
	result, err := store.Transact(t.Context(), nil, []etcd.Mutation{
		{Type: etcd.MutationPut, Key: "/atomic/one", Value: []byte("one")},
		{Type: etcd.MutationPut, Key: "/atomic/two", Value: []byte("two")},
	})
	if err != nil || !result.Succeeded || result.Revision != before+1 {
		t.Fatalf("valid transaction = %#v, %v", result, err)
	}
	one, _ := store.Get(t.Context(), "/atomic/one")
	two, _ := store.Get(t.Context(), "/atomic/two")
	if one.Entry == nil || two.Entry == nil || one.Entry.ModRevision != two.Entry.ModRevision ||
		one.Entry.ModRevision != result.Revision {
		t.Fatalf("atomic revisions: one=%#v two=%#v", one, two)
	}
}

// Rationale: deletes are idempotent MVCC operations, prefix deletes are atomic,
// and an etcd key's Version restarts after deletion and recreation.
func TestBlueprintTestStoreDeleteAndVersionSemantics(t *testing.T) {
	store := newBlueprintTestStore()
	if _, err := store.Put(t.Context(), "/tree/a", []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "/tree/a", []byte("a2")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Put(t.Context(), "/tree/b", []byte("b")); err != nil {
		t.Fatal(err)
	}
	current, _ := store.Get(t.Context(), "/tree/a")
	if current.Entry == nil || current.Entry.Version != 2 {
		t.Fatalf("consecutive put version = %#v", current)
	}
	deleteRevision, err := store.Delete(t.Context(), "/tree/a")
	if err != nil {
		t.Fatal(err)
	}
	unchanged, err := store.Delete(t.Context(), "/tree/a")
	if err != nil || unchanged != deleteRevision {
		t.Fatalf("missing delete revision = %d, %v; want %d", unchanged, err, deleteRevision)
	}
	if _, err := store.Put(t.Context(), "/tree/a", []byte("a3")); err != nil {
		t.Fatal(err)
	}
	recreated, _ := store.Get(t.Context(), "/tree/a")
	if recreated.Entry == nil || recreated.Entry.Version != 1 {
		t.Fatalf("recreated version = %#v", recreated)
	}
	result, err := store.Transact(t.Context(), nil, []etcd.Mutation{{
		Type: etcd.MutationDelete, Key: "/tree/", Prefix: true,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("prefix delete = %#v, %v", result, err)
	}
	page, err := store.Range(t.Context(), etcd.RangeRequest{Prefix: "/tree/", Limit: 1})
	if err != nil || len(page.Values) != 0 {
		t.Fatalf("range after prefix delete = %#v, %v", page, err)
	}
}

// Rationale: fixture reconnect paths share this Store across workers, so
// concurrent commits must serialize without lost versions or data races.
func TestBlueprintTestStoreConcurrentTransactions(t *testing.T) {
	store := newBlueprintTestStore()
	const workers = 24
	var group sync.WaitGroup
	group.Add(workers)
	for index := range workers {
		go func() {
			defer group.Done()
			if _, err := store.Put(t.Context(), "/concurrent", []byte{byte(index)}); err != nil {
				t.Errorf("Put(%d) error = %v", index, err)
			}
		}()
	}
	group.Wait()
	result, err := store.Get(t.Context(), "/concurrent")
	if err != nil || result.Entry == nil || result.Entry.Version != workers {
		t.Fatalf("concurrent result = %#v, %v", result, err)
	}
}

// Rationale: the three fixture fault modes must be one-shot, observe cloned
// public operations, and distinguish compare loss from a lost commit response.
func TestBlueprintTestStoreFaultsAreBounded(t *testing.T) {
	t.Run("fail nth ordinary before commit", func(t *testing.T) {
		store := newBlueprintTestStore()
		store.setFault(func(conditions []etcd.Condition, mutations []etcd.Mutation) blueprintTestStoreFaultDirective {
			if len(mutations) > 0 {
				mutations[0].Key = "/callback-cannot-change-request"
				mutations[0].Value = []byte("callback")
			}
			return blueprintTestStoreFaultDirective{
				kind: blueprintTestStoreFailOrdinaryBeforeCommit, ordinaryTransaction: 2,
			}
		})
		if _, err := store.Put(t.Context(), "/fault/one", []byte("one")); err != nil {
			t.Fatal(err)
		}
		if _, err := store.Put(t.Context(), "/fault/two", []byte("two")); !isErrorKind(err, errs.KindInternal) {
			t.Fatalf("second Put error = %v", err)
		}
		if _, err := store.Put(t.Context(), "/fault/three", []byte("three")); err != nil {
			t.Fatalf("one-shot third Put error = %v", err)
		}
		one, _ := store.Get(t.Context(), "/fault/one")
		two, _ := store.Get(t.Context(), "/fault/two")
		changed, _ := store.Get(t.Context(), "/callback-cannot-change-request")
		if one.Entry == nil || string(one.Entry.Value) != "one" || two.Entry != nil || changed.Entry != nil {
			t.Fatalf("fault changed public operations: one=%#v two=%#v callback=%#v", one, two, changed)
		}
		if stats := store.stats(); stats.Transactions != 3 {
			t.Fatalf("transaction stats = %#v", stats)
		}
	})

	t.Run("terminal compare loss", func(t *testing.T) {
		store := newBlueprintTestStore()
		if _, err := store.Put(t.Context(), "/fault/compared", []byte("same")); err != nil {
			t.Fatal(err)
		}
		before, _ := store.Get(t.Context(), "/fault/compared")
		store.setFault(func([]etcd.Condition, []etcd.Mutation) blueprintTestStoreFaultDirective {
			return blueprintTestStoreFaultDirective{
				kind: blueprintTestStoreAdvanceComparedValueBeforeTerminal, comparedCondition: 0,
			}
		})
		result, err := store.transact(t.Context(), []etcd.Condition{{
			Key: "/fault/compared", ModRevision: before.Entry.ModRevision,
		}}, []etcd.Mutation{{Type: etcd.MutationPut, Key: "/fault/terminal", Value: []byte("no")}}, true)
		if err != nil || result.Succeeded || len(result.FailureReads) != 1 || result.FailureReads[0] == nil ||
			!bytes.Equal(result.FailureReads[0].Value, []byte("same")) {
			t.Fatalf("terminal compare loss = %#v, %v", result, err)
		}
		stats := store.stats()
		if stats.InjectedRaceRevision == 0 || stats.InjectedRaceRevision != result.FailureReads[0].ModRevision {
			t.Fatalf("injected race stats = %#v", stats)
		}
		retry, err := store.transact(t.Context(), []etcd.Condition{{
			Key: "/fault/compared", ModRevision: result.FailureReads[0].ModRevision,
		}}, []etcd.Mutation{{Type: etcd.MutationPut, Key: "/fault/terminal", Value: []byte("yes")}}, true)
		if err != nil || !retry.Succeeded || store.stats().InjectedRaceRevision != stats.InjectedRaceRevision {
			t.Fatalf("one-shot terminal retry = %#v, %v; stats=%#v", retry, err, store.stats())
		}
	})

	t.Run("terminal response lost after commit", func(t *testing.T) {
		store := newBlueprintTestStore()
		store.setFault(func([]etcd.Condition, []etcd.Mutation) blueprintTestStoreFaultDirective {
			return blueprintTestStoreFaultDirective{kind: blueprintTestStoreErrorAfterTerminalCommit}
		})
		result, err := store.transact(t.Context(), nil, []etcd.Mutation{{
			Type: etcd.MutationPut, Key: "/fault/committed", Value: []byte("yes"),
		}}, true)
		if result.Succeeded || !isErrorKind(err, errs.KindInternal) {
			t.Fatalf("lost response result = %#v, %v", result, err)
		}
		read, readErr := store.Get(t.Context(), "/fault/committed")
		if readErr != nil || read.Entry == nil || string(read.Entry.Value) != "yes" {
			t.Fatalf("committed state = %#v, %v", read, readErr)
		}
		stats := store.stats()
		if stats.CommittedRevision != read.Entry.ModRevision {
			t.Fatalf("committed revision stats = %#v, read=%#v", stats, read)
		}
		if _, err := store.Put(t.Context(), "/fault/after", []byte("ok")); err != nil {
			t.Fatalf("fault was not one-shot: %v", err)
		}
	})

	t.Run("read only counts rejected write", func(t *testing.T) {
		store := newBlueprintTestStore()
		store.readOnly = true
		if _, err := store.Put(t.Context(), "/readonly", []byte("no")); err == nil {
			t.Fatal("read-only Put error = nil")
		}
		if store.writes != 1 || store.currentRevision() != 1 {
			t.Fatalf("read-only state: writes=%d revision=%d", store.writes, store.currentRevision())
		}
	})
}

func keysOf(values []etcd.KeyValue) string {
	var joined []byte
	for index := range values {
		if index != 0 {
			joined = append(joined, ',')
		}
		joined = append(joined, values[index].Key...)
	}
	return string(joined)
}

func isErrorKind(err error, kind errs.Kind) bool {
	actual, ok := errs.KindOf(err)
	return err != nil && ok && actual == kind
}
