package controller

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type fakeTaskQueries struct {
	getTaskID      string
	eventsTaskID   string
	eventsRevision int64
	listCalls      int
	task           etcd.Versioned[etcd.TaskRecord]
	page           etcd.Page[etcd.TaskRecord]
	request        etcd.PageRequest
	scope          etcd.TaskListScope
	events         etcd.TaskEventSnapshot
	list           func(etcd.TaskListScope, etcd.PageRequest) (etcd.Page[etcd.TaskRecord], error)
}

// QA: TASK-01; pure projection only, not stored ownership or HTTP corruption handling.
// Rationale: malformed actor provenance must not be projected as an operator action.
func TestTaskPublicProjectionRejectsMalformedDurableActor(t *testing.T) {
	record := aliasTaskRecord(time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC), 301)
	if _, err := taskListResponse(record); err != nil {
		t.Fatalf("valid actor baseline: %v", err)
	}
	record.Actor = etcd.TaskActor("unknown")
	if _, err := taskListResponse(record); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskListResponse(malformed actor) error = %v, want internal", err)
	}
}

// QA: TASK-01; pure completed-Controller-step projection, not actual execution or persistence.
// Rationale: a Controller step without Agent events must reflect its completed
// Task instead of remaining pending or acquiring a fabricated Script identity.
func TestTaskResponseProjectsControllerStepFromTaskLifecycle(t *testing.T) {
	record := aliasTaskRecord(time.Date(2026, 8, 29, 12, 0, 0, 0, time.UTC), 306)
	record.Executor = etcd.TaskExecutorController
	record.Status = etcd.TaskStatusCompleted
	record.Steps = []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: "delete_tenant"}}

	response, err := taskResponse(record, etcd.TaskEventSnapshot{})
	if err != nil {
		t.Fatalf("taskResponse() error = %v", err)
	}
	if len(response.Steps) != 1 {
		t.Fatalf("len(taskResponse().Steps) = %d, want 1", len(response.Steps))
	}
	if got := response.Steps[0].Status; got != apiTypes.TaskCompleted {
		t.Fatalf("taskResponse().Steps[0].Status = %q, want %q", got, apiTypes.TaskCompleted)
	}
	if step := response.Steps[0]; step.Name != "delete_tenant" || step.Kind != apiTypes.TaskStepOperation ||
		step.ScriptID != "" || step.ScriptSlug != "" {
		t.Fatalf("Controller operation step = %#v", step)
	}
}

// QA: TASK-01; pure projection only, not stored journal validation or HTTP errors.
// Rationale: an unknown durable type must fail instead of widening the public vocabulary.
func TestTaskPublicProjectionRejectsUnknownDurableType(t *testing.T) {
	record := aliasTaskRecord(time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC), 302)
	if _, err := taskListResponse(record); err != nil {
		t.Fatalf("valid type baseline: %v", err)
	}
	record.Type = etcd.TaskType("unknown")
	if _, err := taskListResponse(record); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskListResponse(unknown type) error = %v, want internal", err)
	}
}

// QA: TASK-01, BAK-08; pure provenance projection, not scheduler admission or actual pruning.
// Rationale: internal backup_prune maintenance cannot claim operator provenance.
func TestTaskPublicProjectionRejectsOperatorBackupPrune(t *testing.T) {
	record := aliasTaskRecord(time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC), 303)
	record.Type = etcd.TaskBackupPrune
	record.Actor = etcd.TaskActorSystem
	if _, err := taskListResponse(record); err != nil {
		t.Fatalf("system prune baseline: %v", err)
	}
	record.Actor = etcd.TaskActorOperator
	if _, err := taskListResponse(record); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskListResponse(operator backup_prune) error = %v, want internal", err)
	}
}

func (queries *fakeTaskQueries) GetTask(_ context.Context, taskID string) (etcd.Versioned[etcd.TaskRecord], error) {
	queries.getTaskID = taskID
	if queries.task.Record.ID == "" {
		return etcd.Versioned[etcd.TaskRecord]{}, errs.New(errs.KindTaskNotFound, "missing")
	}
	return queries.task, nil
}

func (queries *fakeTaskQueries) ListTaskEvents(
	_ context.Context, taskID string, revision int64,
) (etcd.TaskEventSnapshot, error) {
	queries.eventsTaskID = taskID
	queries.eventsRevision = revision
	return queries.events, nil
}

func (queries *fakeTaskQueries) ListTasksByScope(
	_ context.Context,
	scope etcd.TaskListScope,
	request etcd.PageRequest,
) (etcd.Page[etcd.TaskRecord], error) {
	queries.listCalls++
	queries.scope = scope
	queries.request = request
	if queries.list != nil {
		return queries.list(scope, request)
	}
	return queries.page, nil
}

// QA: TASK-01/06; HTTP projection and requested read revision only, not real etcd snapshot consistency.
// Rationale: Task detail must request events at the Task's exact read revision
// and preserve Script/ordinary step identity, owner, actor and timeline.
func TestTaskShowReturnsFixedRevisionStepProjection(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, now, 1)
	stepID := ids.NewAt(ids.KindStep, now, 2)
	ordinaryStepID := ids.NewAt(ids.KindStep, now, 5)
	scriptID := ids.NewAt(ids.KindScript, now, 6)
	startedAt := now.Add(time.Second)
	queries := &fakeTaskQueries{
		task: etcd.Versioned[etcd.TaskRecord]{
			Record: etcd.TaskRecord{
				ID: taskID, OperationID: ids.NewAt(ids.KindOperation, now, 3),
				PlanHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Type:     etcd.TaskDeploy, Target: ids.NewAt(ids.KindService, now, 4),
				Status: etcd.TaskStatusRunning, Steps: []etcd.TaskStepRecord{
					{Kind: etcd.TaskStepScript, ID: stepID, ScriptID: scriptID, ScriptSlug: "migrate-schema"},
					{Kind: etcd.TaskStepOperation, ID: ordinaryStepID},
				},
				Owner: etcd.TaskOwner{WorkspaceType: etcd.TaskWorkspacePlatform}, Actor: etcd.TaskActorSystem,
				CreatedAt: now, UpdatedAt: startedAt, StartedAt: &startedAt,
			},
			ReadRevision: 17,
		},
		events: etcd.TaskEventSnapshot{Revision: 17, Events: []etcd.TaskEventRecord{{
			Identity: etcd.TaskEventIdentity{TaskID: taskID, StepID: stepID, Attempt: 1, Ordinal: 1},
			State:    etcd.TaskEventStateRunning,
		}}},
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	server.tasks = queries
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+taskID, nil)
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if queries.getTaskID != taskID || queries.eventsTaskID != taskID || queries.eventsRevision != 17 {
		t.Fatalf("Task/event reads = %q / %q at %d", queries.getTaskID, queries.eventsTaskID, queries.eventsRevision)
	}
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	payload := append([]byte(nil), response.Body.Bytes()...)
	var body apiTypes.Task
	if err := json.Unmarshal(payload, &body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ID != taskID || body.OperationID != queries.task.Record.OperationID ||
		body.PlanHash != "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ||
		body.Type != "deploy" || body.Target != queries.task.Record.Target ||
		body.Status != apiTypes.TaskRunning || len(body.Steps) != 2 ||
		body.Steps[0].Kind != apiTypes.TaskStepScript || body.Steps[1].Kind != apiTypes.TaskStepOperation ||
		body.Steps[0].Name != stepID || body.Steps[0].Status != apiTypes.TaskRunning ||
		body.Steps[0].ScriptID != scriptID || body.Steps[0].ScriptSlug != "migrate-schema" ||
		body.Steps[1].Name != ordinaryStepID || body.Steps[1].Status != apiTypes.TaskPending ||
		body.WorkspaceType != apiTypes.TaskWorkspacePlatform || body.Actor != apiTypes.TaskActorSystem ||
		!body.CreatedAt.Equal(
			now,
		) || !body.UpdatedAt.Equal(startedAt) || body.StartedAt == nil ||
		!body.StartedAt.Equal(startedAt) || body.FinishedAt != nil {
		t.Fatalf("Task response = %#v", body)
	}
	var raw struct {
		Steps []map[string]json.RawMessage `json:"steps"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatalf("decode raw response: %v", err)
	}
	if _, exists := raw.Steps[1]["script_id"]; exists {
		t.Fatalf("ordinary Task step exposed script_id: %s", raw.Steps[1]["script_id"])
	}
	if _, exists := raw.Steps[1]["script_slug"]; exists {
		t.Fatalf("ordinary Task step exposed script_slug: %s", raw.Steps[1]["script_slug"])
	}
}

// QA: TASK-01, UI-03; injected repository miss through HTTP, not retained/deleted Task storage.
// Rationale: the Task-detail boundary must preserve the stable
// task.not_found problem instead of degrading repository misses to HTTP 500.
func TestTaskShowReturnsTaskNotFoundProblem(t *testing.T) {
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	server.tasks = &fakeTaskQueries{}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task_01J00000000000000000000000", nil)
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var problem errs.Problem
	if err := json.NewDecoder(response.Body).Decode(&problem); err != nil {
		t.Fatalf("decode Task miss: %v", err)
	}
	if problem.Type != "about:blank" || problem.Code != "task.not_found" || problem.Status != 404 ||
		response.Header().Get("Content-Type") != "application/problem+json" {
		t.Fatalf("Task miss problem = %#v, content type %q", problem, response.Header().Get("Content-Type"))
	}
}

// QA: TASK-01; route projection over a fake page, not persisted ordering or fixed-revision pagination.
// Rationale: both aliases must forward identical scope/page inputs and expose
// the same Task identity, owner, actor, timeline and continuation cursor.
func TestTaskListAndActivitySharePageProjection(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 9)
	projectID := ids.NewAt(ids.KindProject, now, 8)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 7)
	queries := &fakeTaskQueries{page: etcd.Page[etcd.TaskRecord]{
		Items: []etcd.Versioned[etcd.TaskRecord]{{Record: etcd.TaskRecord{
			ID: ids.NewAt(ids.KindTask, now, 10), OperationID: ids.NewAt(ids.KindOperation, now, 11),
			Type: etcd.TaskBackupPrune, Target: environmentID, Status: etcd.TaskStatusPending,
			Owner: etcd.TaskOwner{
				WorkspaceType: etcd.TaskWorkspaceTenant, TenantID: tenantID,
				ProjectID: projectID, EnvironmentID: environmentID,
			},
			Actor: etcd.TaskActorSystem, CreatedAt: now, UpdatedAt: now,
		}}},
		NextCursor: "next-page",
	}}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	server.tasks = queries
	for _, path := range []string{
		"/api/v1/tasks?limit=7&cursor=current&workspace=" + tenantID,
		"/api/v1/activity?limit=7&cursor=current&workspace=" + tenantID,
	} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, request)
		if response.Code != http.StatusOK {
			t.Fatalf("%s status = %d, body = %s", path, response.Code, response.Body.String())
		}
		var body apiTypes.Page[apiTypes.Task]
		if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
			t.Fatalf("decode %s response: %v", path, err)
		}
		if len(body.Items) != 1 || body.Items[0].ID != queries.page.Items[0].Record.ID ||
			body.Items[0].OperationID != queries.page.Items[0].Record.OperationID ||
			body.Items[0].Status != apiTypes.TaskPending || body.NextCursor != "next-page" ||
			body.Items[0].Type != "backup_prune" || body.Items[0].Target != environmentID ||
			body.Items[0].WorkspaceType != apiTypes.TaskWorkspaceTenant || body.Items[0].TenantID != tenantID ||
			body.Items[0].ProjectID != projectID || body.Items[0].EnvironmentID != environmentID ||
			body.Items[0].Actor != apiTypes.TaskActorSystem || !body.Items[0].CreatedAt.Equal(now) ||
			!body.Items[0].UpdatedAt.Equal(now) ||
			body.Items[0].StartedAt != nil || body.Items[0].FinishedAt != nil {
			t.Fatalf("%s response = %#v", path, body)
		}
		if queries.request != (etcd.PageRequest{Limit: 7, Cursor: "current"}) {
			t.Fatalf("%s request = %#v", path, queries.request)
		}
		if queries.scope != (etcd.TaskListScope{Kind: etcd.TaskListScopeTenantWorkspace, ID: tenantID}) {
			t.Fatalf("%s scope = %#v", path, queries.scope)
		}
	}
}

// QA: TASK-01; fake cursor forwarding only, not real cursor binding, compaction or storage consistency.
// Rationale: Task and Activity are route aliases over one logical collection,
// so a continuation issued by either route must be consumed by the other.
func TestTaskListAndActivityConsumeEachOthersCursors(t *testing.T) {
	now := time.Date(2026, 8, 22, 15, 0, 0, 0, time.UTC)
	records := []etcd.TaskRecord{
		aliasTaskRecord(now, 31),
		aliasTaskRecord(now.Add(time.Second), 32),
	}
	const sharedCursor = "shared-task-cursor"
	queries := &fakeTaskQueries{}
	queries.list = func(
		scope etcd.TaskListScope,
		request etcd.PageRequest,
	) (etcd.Page[etcd.TaskRecord], error) {
		if scope != (etcd.TaskListScope{Kind: etcd.TaskListScopePlatformWorkspace}) || request.Limit != 1 {
			return etcd.Page[etcd.TaskRecord]{}, errs.New(errs.KindInternal, "alias list request changed")
		}
		switch request.Cursor {
		case "":
			return etcd.Page[etcd.TaskRecord]{
				Items:      []etcd.Versioned[etcd.TaskRecord]{{Record: records[0]}},
				NextCursor: sharedCursor,
			}, nil
		case sharedCursor:
			return etcd.Page[etcd.TaskRecord]{
				Items: []etcd.Versioned[etcd.TaskRecord]{{Record: records[1]}},
			}, nil
		default:
			return etcd.Page[etcd.TaskRecord]{}, errs.New(errs.KindMalformedRequest, "cursor changed")
		}
	}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	server.tasks = queries
	for _, routes := range [][2]string{{"tasks", "activity"}, {"activity", "tasks"}} {
		first := requestTaskAliasPage(t, server, routes[0], "")
		if len(first.Items) != 1 || first.Items[0].ID != records[0].ID || first.NextCursor != sharedCursor {
			t.Fatalf("GET /%s first page = %#v", routes[0], first)
		}
		second := requestTaskAliasPage(t, server, routes[1], first.NextCursor)
		if len(second.Items) != 1 || second.Items[0].ID != records[1].ID || second.NextCursor != "" {
			t.Fatalf("GET /%s continuation = %#v", routes[1], second)
		}
	}
}

func aliasTaskRecord(at time.Time, seed int64) etcd.TaskRecord {
	return etcd.TaskRecord{
		ID:          ids.NewAt(ids.KindTask, at, seed),
		OperationID: ids.NewAt(ids.KindOperation, at, seed+100),
		Type:        etcd.TaskUpdate,
		Target:      ids.NewAt(ids.KindComponent, at, seed+200),
		Status:      etcd.TaskStatusPending,
		Owner:       etcd.TaskOwner{WorkspaceType: etcd.TaskWorkspacePlatform},
		Actor:       etcd.TaskActorOperator,
		CreatedAt:   at,
		UpdatedAt:   at,
	}
}

func requestTaskAliasPage(
	t *testing.T,
	server *Server,
	route string,
	cursor string,
) apiTypes.Page[apiTypes.Task] {
	t.Helper()
	path := "/api/v1/" + route + "?limit=1&workspace=platform"
	if cursor != "" {
		path += "&cursor=" + cursor
	}
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	if response.Code != http.StatusOK {
		t.Fatalf("GET /%s status = %d, body = %s", route, response.Code, response.Body.String())
	}
	var page apiTypes.Page[apiTypes.Task]
	if err := json.NewDecoder(response.Body).Decode(&page); err != nil {
		t.Fatalf("decode GET /%s response: %v", route, err)
	}
	return page
}

// Delivery: generated Task schema vocabulary; not Task execution or public read behavior.
// Rationale: generated clients must retain the closed type, owner and actor enums.
func TestTaskOpenAPIConstrainsPublicJournalEnums(t *testing.T) {
	document, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	var contract struct {
		Components struct {
			Schemas map[string]struct {
				Properties map[string]struct {
					Enum []string `json:"enum"`
				} `json:"properties"`
			} `json:"schemas"`
		} `json:"components"`
	}
	if err := json.Unmarshal(document, &contract); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	properties := contract.Components.Schemas["Task"].Properties
	assertTaskOpenAPIEnum(t, properties["type"].Enum,
		"deploy", "rollback", "backup", "backup_prune", "restore", "attach", "detach", "run", "script",
		"provision", "create", "update", "remove", "start", "stop", "destroy", "rotate",
	)
	assertTaskOpenAPIEnum(t, properties["workspace_type"].Enum, "platform", "tenant")
	assertTaskOpenAPIEnum(t, properties["actor"].Enum, "operator", "system")
}

func assertTaskOpenAPIEnum(t *testing.T, got []string, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("OpenAPI enum = %v, want %v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("OpenAPI enum = %v, want %v", got, want)
		}
	}
}

// QA: TASK-01, UI-03; HTTP-to-query dispatch only, not actual ownership indexes or storage isolation.
// Rationale: each stable-id scope must reach its exact query once; conflicting
// scopes must return validation.failed before invoking any repository query.
func TestTaskListDispatchesEveryScopeAndRejectsScopeConflicts(t *testing.T) {
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	queries := &fakeTaskQueries{page: etcd.Page[etcd.TaskRecord]{Items: []etcd.Versioned[etcd.TaskRecord]{}}}
	server.tasks = queries
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	tenantID := ids.NewAt(ids.KindTenant, now, 21)
	projectID := ids.NewAt(ids.KindProject, now, 22)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 23)
	tests := []struct {
		path string
		want etcd.TaskListScope
	}{
		{path: "/api/v1/tasks", want: etcd.TaskListScope{Kind: etcd.TaskListScopeGlobal}},
		{path: "/api/v1/tasks?workspace=platform", want: etcd.TaskListScope{Kind: etcd.TaskListScopePlatformWorkspace}},
		{
			path: "/api/v1/tasks?workspace=" + tenantID,
			want: etcd.TaskListScope{Kind: etcd.TaskListScopeTenantWorkspace, ID: tenantID},
		},
		{
			path: "/api/v1/tasks?project=" + projectID,
			want: etcd.TaskListScope{Kind: etcd.TaskListScopeProject, ID: projectID},
		},
		{
			path: "/api/v1/tasks?environment=" + environmentID,
			want: etcd.TaskListScope{Kind: etcd.TaskListScopeEnvironment, ID: environmentID},
		},
	}
	for _, test := range tests {
		before := queries.listCalls
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, test.path, nil))
		if response.Code != http.StatusOK || queries.scope != test.want || queries.listCalls != before+1 {
			t.Fatalf(
				"%s = status %d, scope %#v, body %s",
				test.path,
				response.Code,
				queries.scope,
				response.Body.String(),
			)
		}
	}
	conflicts := []string{
		"workspace=platform&environment=" + environmentID,
		"workspace=platform&project=" + projectID,
		"project=" + projectID + "&environment=" + environmentID,
	}
	for _, query := range conflicts {
		before := queries.listCalls
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/tasks?"+query, nil))
		if response.Code != http.StatusUnprocessableEntity || queries.listCalls != before {
			t.Fatalf("conflicting scope %q status = %d, body = %s", query, response.Code, response.Body.String())
		}
		var problem struct {
			Code string `json:"code"`
		}
		if err := json.NewDecoder(response.Body).Decode(&problem); err != nil || problem.Code != "validation.failed" {
			t.Fatalf("conflicting scope %q problem = %#v, %v", query, problem, err)
		}
	}
}

// Delivery: code-first operation/media declarations, not working Task/Activity reads or client parity.
// Rationale: generated clients require the exact read operation identities and JSON success media type.
func TestTaskOpenAPIContainsServingReadOperations(t *testing.T) {
	t.Parallel()
	document, err := New(nil, nil, Options{}).OpenAPIDocument()
	if err != nil {
		t.Fatalf("OpenAPIDocument() error = %v", err)
	}
	var contract struct {
		Paths map[string]map[string]struct {
			OperationID string `json:"operationId"`
			Responses   map[string]struct {
				Content map[string]json.RawMessage `json:"content"`
			} `json:"responses"`
		} `json:"paths"`
	}
	if err := json.Unmarshal(document, &contract); err != nil {
		t.Fatalf("decode OpenAPI: %v", err)
	}
	operation := contract.Paths["/tasks/{id}"]["get"]
	if operation.OperationID != "task.show" {
		t.Fatalf("GET /tasks/{id} operationId = %q, want task.show", operation.OperationID)
	}
	if _, exists := operation.Responses["200"].Content["application/json"]; !exists {
		t.Fatalf("task.show responses = %#v, want JSON 200", operation.Responses)
	}
	for path, operationID := range map[string]string{"/tasks": "task.list", "/activity": "activity.list"} {
		operation := contract.Paths[path]["get"]
		if operation.OperationID != operationID {
			t.Fatalf("GET %s operationId = %q, want %s", path, operation.OperationID, operationID)
		}
		if _, exists := operation.Responses["200"].Content["application/json"]; !exists {
			t.Fatalf("%s responses = %#v, want JSON 200", operationID, operation.Responses)
		}
	}
}
