package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	api "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-128: immutable owner indexes must isolate workspace history, preserve a
// fixed snapshot across continuation, and keep maximum-page reads bounded.
// These are native publication/listing fragments, not complete operations.
func TestNativeTaskListingScopesSnapshotsAndBoundedReads(t *testing.T) {
	store := &nativeTaskListStore{memoryHierarchyStore: newMemoryHierarchyStore()}
	store.revision = 1
	f := newNativeTaskListFixture(t, store)
	tenant, project, environment := ids.New(ids.KindTenant), ids.New(ids.KindProject), ids.New(ids.KindEnvironment)
	backingProject, backingEnvironment := ids.New(ids.KindProject), ids.New(ids.KindEnvironment)
	wanted := map[string][]string{}
	add := func(scope workexecution.Scope, selected ...string) {
		task := f.publish(t, scope, workexecution.ActionDeploy, ids.New(ids.KindService))
		for _, key := range append([]string{"all"}, selected...) {
			wanted[key] = append(wanted[key], task.ID)
		}
	}
	add(workexecution.Scope{}, "platform")
	add(workexecution.Scope{TenantID: tenant}, tenant)
	add(workexecution.Scope{ProjectID: backingProject, EnvironmentID: backingEnvironment},
		"platform", backingProject, backingEnvironment)
	for range 105 {
		add(workexecution.Scope{TenantID: tenant, ProjectID: project, EnvironmentID: environment},
			tenant, project, environment)
	}
	for scope, expected := range wanted {
		slices.Reverse(expected)
		page, err := f.queue.ListTasks(t.Context(), scope, "", 100, 0)
		if err != nil || len(page.Tasks) != min(100, len(expected)) || page.More != (len(expected) > 100) {
			t.Fatalf("scope %s: %d Tasks, more=%t, error=%v", scope, len(page.Tasks), page.More, err)
		}
		for index, snapshot := range page.Tasks {
			if snapshot.Task.ID != expected[index] {
				t.Fatalf("scope %s returned a foreign or out-of-order Task", scope)
			}
		}
	}
	first, err := f.queue.ListTasks(t.Context(), tenant, "", 100, 0)
	if err != nil {
		t.Fatal(err)
	}
	late := f.publish(t, workexecution.Scope{TenantID: tenant}, workexecution.ActionDeploy, ids.New(ids.KindService))
	second, err := f.queue.ListTasks(t.Context(), tenant, first.LastID, 100, first.ReadRevision)
	if err != nil || second.More || second.ReadRevision != first.ReadRevision || len(second.Tasks) != 6 {
		t.Fatalf("fixed continuation: %d Tasks, more=%t, error=%v", len(second.Tasks), second.More, err)
	}
	for index, snapshot := range second.Tasks {
		if snapshot.Task.ID != wanted[tenant][100+index] {
			t.Fatal("continuation included a later action, skipped history or repeated a Task")
		}
	}
	current, err := f.queue.ListTasks(t.Context(), tenant, "", 1, 0)
	if err != nil || len(current.Tasks) != 1 || current.Tasks[0].Task.ID != late.ID {
		t.Fatalf("new snapshot did not expose later acceptance: %v", err)
	}
	if store.maximumKeys == 0 || store.maximumKeys > keyvalue.MaximumOperations {
		t.Fatalf("Task reads did not respect the bounded storage interface: %d keys", store.maximumKeys)
	}
}

// TASK-128: an index referencing a foreign, missing or differently keyed Task is
// corruption, not empty history. Invalid operator requests fail validation.
func TestNativeTaskListingRejectsCorruptOwnership(t *testing.T) {
	for _, scenario := range []string{"foreign owner", "missing Task", "wrong index identity"} {
		t.Run(scenario, func(t *testing.T) {
			store := &nativeTaskListStore{memoryHierarchyStore: newMemoryHierarchyStore()}
			store.revision = 1
			f := newNativeTaskListFixture(t, store)
			task := f.publish(t, workexecution.Scope{}, workexecution.ActionDeploy, ids.New(ids.KindService))
			scope := ids.New(ids.KindTenant)
			key := "/v1/execution/task-list/" + scope + "/" + task.ID
			if scenario == "missing Task" {
				if _, err := store.Delete(t.Context(), workqueue.TaskKey(task.ID)); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "wrong index identity" {
				key = "/v1/execution/task-list/" + scope + "/" + ids.New(ids.KindTask)
			}
			if _, err := store.Put(t.Context(), key, []byte(task.ID)); err != nil {
				t.Fatal(err)
			}
			if _, err := f.queue.ListTasks(t.Context(), scope, "", 25, 0); !errors.Is(
				err,
				errs.New(errs.KindInternal, ""),
			) {
				t.Fatalf("corrupt %s returned %v", scenario, err)
			}
			for _, query := range []struct {
				scope, after    string
				limit, revision int64
			}{{"invalid", "", 25, 0}, {"all", "invalid", 25, 0}, {"all", "", 101, 0}, {"all", "", 0, 0}, {"all", "", 25, -1}} {
				if _, err := f.queue.ListTasks(t.Context(), query.scope, query.after, query.limit, query.revision); !errors.Is(
					err,
					errs.New(errs.KindValidationFailed, ""),
				) {
					t.Fatalf("invalid list request returned %v", err)
				}
			}
		})
	}
}

// TASK-128: sparse server-side matches must remain discoverable after a bounded
// empty scan. Tasks/Activity continuation shares the same MVCC snapshot and
// excludes a matching action accepted after the first page.
func TestNativeTaskHTTPFilteredHistoryContinuesAfterScanLimit(t *testing.T) {
	f := newNativeTaskInspectionFixture(t, io.Discard)
	publication := newNativeTaskListFixture(t, f.volume.store.memoryHierarchyStore)
	scope := workexecution.Scope{TenantID: ids.New(ids.KindTenant), ProjectID: ids.New(ids.KindProject),
		EnvironmentID: ids.New(ids.KindEnvironment)}
	var expected []string
	for index := range 1008 {
		action, target := workexecution.ActionStop, ids.New(ids.KindRoute)
		if index == 0 || index == 2 {
			action, target = workexecution.ActionDeploy, ids.New(ids.KindService)
		}
		task := publication.publish(t, scope, action, target)
		if index == 0 || index == 2 {
			expected = append(expected, task.ID)
		}
	}
	slices.Reverse(expected)
	query := url.Values{"scope": {scope.EnvironmentID}, "limit": {"1"},
		"action": {"deploy"}, "resource_kind": {"service"}, "status": {"queued"}}
	page := nativeTaskHTTPGet[api.ExecutionPage[api.TaskSummary]](t, f, "/api/v1/tasks?"+query.Encode())
	if len(page.Items) != 0 || !page.More || !page.ScanLimited || page.After == "" || page.Revision <= 0 {
		t.Fatal("bounded empty filter scan did not provide continuation")
	}
	revision := page.Revision
	publication.publish(t, scope, workexecution.ActionDeploy, ids.New(ids.KindService))
	var actual []string
	for index := 0; page.More && index < 4; index++ {
		previous := page.After
		query.Set("after", previous)
		query.Set("revision", fmt.Sprint(revision))
		page = nativeTaskHTTPGet[api.ExecutionPage[api.TaskSummary]](t, f, "/api/v1/activity?"+query.Encode())
		if page.Revision != revision || page.After == "" || page.After >= previous {
			t.Fatal("filtered Activity continuation changed snapshot or failed to advance")
		}
		for _, task := range page.Items {
			actual = append(actual, task.ID)
		}
	}
	if page.More || !slices.Equal(actual, expected) {
		t.Fatalf("filtered history = %v, more=%t; want %v", actual, page.More, expected)
	}
	query.Del("after")
	query.Del("revision")
	latest := nativeTaskHTTPGet[api.ExecutionPage[api.TaskSummary]](t, f, "/api/v1/tasks?"+query.Encode())
	if len(latest.Items) != 1 || slices.Contains(expected, latest.Items[0].ID) {
		t.Fatal("a refreshed filter did not expose the later matching action")
	}
}

type nativeTaskListStore struct {
	*memoryHierarchyStore
	maximumKeys int
}

func (store *nativeTaskListStore) GetMany(
	ctx context.Context,
	request keyvalue.GetManyRequest,
) (*keyvalue.GetManyResult, error) {
	store.maximumKeys = max(store.maximumKeys, len(request.Keys))
	if len(request.Keys) > keyvalue.MaximumOperations {
		return nil, errs.New(errs.KindInternal, "Task history read exceeds the storage operation bound")
	}
	return store.memoryHierarchyStore.GetMany(ctx, request)
}

type nativeTaskListFixture struct {
	queue      *workqueue.Repository
	ciphertext []byte
	sequence   int64
}

func newNativeTaskListFixture(t *testing.T, store workqueue.Store) *nativeTaskListFixture {
	t.Helper()
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	_, ciphertext := manualJourneyEncryptedValue(t, "captured-listing-intent")
	t.Cleanup(func() { clear(ciphertext) })
	return &nativeTaskListFixture{queue: queue, ciphertext: ciphertext}
}

func (f *nativeTaskListFixture) publish(
	t *testing.T,
	scope workexecution.Scope,
	action workexecution.Action,
	target string,
) workexecution.Task {
	t.Helper()
	f.sequence++
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC).Add(time.Duration(f.sequence) * time.Millisecond)
	input := workexecution.Input{ID: ids.NewAt(ids.KindPlan, at, f.sequence), SHA256: strings.Repeat("a", 64)}
	task := workexecution.Task{ID: ids.NewAt(ids.KindTask, at, f.sequence), Scope: scope,
		Actor: workexecution.OperatorActor, Action: action, TargetID: target, Intent: input,
		ExpectedItems: 1, CreatedAt: at, UpdatedAt: at, Deadline: at.Add(time.Minute)}
	item := workexecution.Item{ID: ids.NewAt(ids.KindWorkItem, at, f.sequence), TaskID: task.ID,
		TargetID: target, Operation: workexecution.ResourceDefinition, Input: &input,
		Placement: workexecution.Placement{Executor: workexecution.Controller},
		Claims:    []workexecution.Claim{{ScopeID: target}},
		Phase:     workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: at, UpdatedAt: at, Deadline: task.Deadline}
	body, err := json.Marshal(api.TaskAccepted{TaskID: task.ID})
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(f.ciphertext)
	marker := idempotency.IdempotencyMarker{Kind: idempotency.IdempotencyMarkerTask,
		State: idempotency.IdempotencyMarkerPending, TaskID: task.ID, CreatedAt: at, UpdatedAt: at,
		Locator: idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopePlatform,
			ScopeID: "-", Method: http.MethodPost, Route: "/services/{id}/deploy", Key: task.ID},
		Intent: idempotency.ProtectedIntentRecord{EnvelopeVersion: 1, Cipher: "age-x25519",
			DigestAlgorithm: "sha256", CiphertextDigest: hex.EncodeToString(digest[:]), Ciphertext: f.ciphertext},
		Response: idempotency.IdempotencyResponse{
			Status:      http.StatusAccepted,
			ContentKind: "application/json",
			Body:        body,
		}}
	change, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		t.Fatal(err)
	}
	conditions, mutations, err := idempotency.PreparePublication(marker)
	if err != nil {
		t.Fatal(err)
	}
	result, err := f.queue.Commit(t.Context(), change, workqueue.Change{Conditions: conditions, Mutations: mutations})
	if err != nil || !result.Succeeded {
		t.Fatalf("native list fixture publication committed=%t: %v", result.Succeeded, err)
	}
	return task
}
