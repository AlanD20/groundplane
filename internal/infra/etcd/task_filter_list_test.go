package etcd

import (
	"errors"
	"testing"
	"time"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: filtering a visible page misses older matches. Server-side filters
// must scan the journal at one revision and cannot reuse another filter's cursor.
func TestTaskFiltersSearchHistoryAndBindCursor(t *testing.T) {
	store := newMemoryTaskStore()
	at := taskJournalTime()
	var wanted []string
	for index := range 7 {
		task := scopedTaskRecord(
			at.Add(time.Duration(index)*time.Second),
			int64(701+index),
			testtaskjournal.PlatformTaskOwner(),
		)
		task.Type = testtaskjournal.TaskStop
		task.Params = map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceRoute}
		if index == 0 || index == 2 {
			task.Type = testtaskjournal.TaskDeploy
			task.Params[testtaskjournal.TaskResourceKindParam] = testtaskjournal.TaskResourceService
			wanted = append([]string{task.ID}, wanted...)
		}
		seedTaskRepositoryTask(t, store, task)
	}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	scope := TaskListScope{
		Kind:         TaskListScopePlatformWorkspace,
		Status:       testtaskjournal.TaskStatusPending,
		Type:         testtaskjournal.TaskDeploy,
		ResourceKind: testtaskjournal.TaskResourceService,
	}
	first, err := repository.ListTasksByScope(t.Context(), scope, testkeyvalue.PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.Items[0].Record.ID != wanted[0] || first.NextCursor == "" {
		t.Fatalf("filtered first page = %#v, %v", first, err)
	}
	late := scopedTaskRecord(at.Add(10*time.Second), 900, testtaskjournal.PlatformTaskOwner())
	late.Params = map[string]string{testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceService}
	seedTaskRepositoryTask(t, store, late)
	next, err := repository.ListTasksByScope(
		t.Context(),
		scope,
		testkeyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor},
	)
	if err != nil || len(next.Items) != 1 || next.Items[0].Record.ID != wanted[1] || next.Revision != first.Revision ||
		next.NextCursor != "" {
		t.Fatalf("filtered next page = %#v, %v", next, err)
	}
	for _, changed := range []TaskListScope{
		{Kind: scope.Kind, Status: testtaskjournal.TaskStatusRunning, Type: scope.Type, ResourceKind: scope.ResourceKind},
		{Kind: scope.Kind, Status: scope.Status, Type: testtaskjournal.TaskStop, ResourceKind: scope.ResourceKind},
		{Kind: scope.Kind, Status: scope.Status, Type: scope.Type, ResourceKind: testtaskjournal.TaskResourceRoute},
	} {
		if _, err := repository.ListTasksByScope(t.Context(), changed, testkeyvalue.PageRequest{Limit: 1, Cursor: first.NextCursor}); !errors.Is(
			err,
			errs.New(errs.KindMalformedRequest, ""),
		) {
			t.Fatalf("changed-filter cursor accepted: %v", err)
		}
	}
}
