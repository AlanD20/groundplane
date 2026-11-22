package workqueue

import (
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestInputSettlementReadDistinguishesOperationOwnedUnstartedResult(t *testing.T) {
	store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
	view := settledUnstartedResultView(t, store, "entry-metadata-result")
	repository, err := NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := repository.ReadInputSettlement(t.Context(), view)
	if err != nil || settlement != nil {
		t.Fatalf("operation-owned result became input settlement: result=%#v err=%v", settlement, err)
	}
}

func TestInputSettlementReadRejectsMissingDerivedPolicyResult(t *testing.T) {
	store := &assignmentStore{revision: 1, values: make(map[string]keyvalue.KeyValue)}
	view := settledUnstartedResultView(t, store, "input-settlement")
	repository, err := NewRepository(&readAuthorizationStore{assignmentStore: store})
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := repository.ReadInputSettlement(t.Context(), view)
	if settlement != nil || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("missing policy settlement = %#v, %v", settlement, err)
	}
}

func settledUnstartedResultView(t *testing.T, store *assignmentStore, purpose string) View {
	t.Helper()
	fixture := assignmentFixture(t, store, workexecution.EntryConfiguration, nil)
	at := fixture.At
	item := fixture.Item.Item
	item.Input = nil
	item.Phase = workexecution.Settled
	item.Outcome = workexecution.Succeeded
	item.UpdatedAt = at
	item.Result = &workexecution.Input{
		ID: ids.DeriveAt(ids.KindPlan, item.CreatedAt, item.ID, purpose), SHA256: strings.Repeat("b", 64),
	}
	item.Placement = workexecution.Placement{Executor: workexecution.Controller}
	if err := item.Validate(); err != nil {
		t.Fatal(err)
	}
	task := fixture.Task.Task
	task.UpdatedAt = at
	task.Progress = workexecution.Progress{Succeeded: 1}
	if err := task.Validate(); err != nil {
		t.Fatal(err)
	}
	return View{
		Task: TaskSnapshot{Task: task, Revision: 1}, Item: ItemSnapshot{Item: item, Revision: 1}, ReadRevision: 1,
	}
}
