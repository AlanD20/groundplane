package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"testing"
)

func releaseLedgerFixture(t *testing.T, store keyvalue.Store) *ReleaseLedger {
	t.Helper()
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := NewReleaseLedger(store, tasks)
	if err != nil {
		t.Fatal(err)
	}
	return ledger
}
