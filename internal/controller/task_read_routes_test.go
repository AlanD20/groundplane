package controller

import (
	"context"
	"encoding/json"
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
	task    etcd.Versioned[etcd.TaskRecord]
	page    etcd.Page[etcd.TaskRecord]
	request etcd.PageRequest
	events  etcd.TaskEventSnapshot
}

func (queries *fakeTaskQueries) GetTask(context.Context, string) (etcd.Versioned[etcd.TaskRecord], error) {
	if queries.task.Record.ID == "" {
		return etcd.Versioned[etcd.TaskRecord]{}, errs.New(errs.KindTaskNotFound, "missing")
	}
	return queries.task, nil
}

func (queries *fakeTaskQueries) ListTaskEvents(context.Context, string, int64) (etcd.TaskEventSnapshot, error) {
	return queries.events, nil
}

func (queries *fakeTaskQueries) ListTasks(_ context.Context, request etcd.PageRequest) (etcd.Page[etcd.TaskRecord], error) {
	queries.request = request
	return queries.page, nil
}

func TestTaskShowReturnsFixedRevisionStepProjection(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	taskID := ids.NewAt(ids.KindTask, now, 1)
	stepID := ids.NewAt(ids.KindStep, now, 2)
	queries := &fakeTaskQueries{
		task: etcd.Versioned[etcd.TaskRecord]{
			Record: etcd.TaskRecord{
				ID: taskID, OperationID: ids.NewAt(ids.KindOperation, now, 3),
				PlanHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				Type:     etcd.TaskDeploy, Target: ids.NewAt(ids.KindService, now, 4),
				Status: etcd.TaskStatusRunning, Steps: []etcd.TaskStepRecord{{ID: stepID}},
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
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var body apiTypes.Task
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.ID != taskID || body.Status != apiTypes.TaskRunning || len(body.Steps) != 1 ||
		body.Steps[0].Name != stepID || body.Steps[0].Status != apiTypes.TaskRunning {
		t.Fatalf("Task response = %#v", body)
	}
}

func TestTaskShowReturnsTaskNotFoundProblem(t *testing.T) {
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	server.tasks = &fakeTaskQueries{}
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/task_01J00000000000000000000000", nil)
	response := httptest.NewRecorder()
	server.Mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func TestTaskListAndActivityShareDurablePage(t *testing.T) {
	now := time.Date(2026, 8, 22, 12, 0, 0, 0, time.UTC)
	queries := &fakeTaskQueries{page: etcd.Page[etcd.TaskRecord]{
		Items: []etcd.Versioned[etcd.TaskRecord]{{Record: etcd.TaskRecord{
			ID: ids.NewAt(ids.KindTask, now, 10), OperationID: ids.NewAt(ids.KindOperation, now, 11),
			Type: etcd.TaskStop, Target: ids.NewAt(ids.KindService, now, 12), Status: etcd.TaskStatusPending,
		}}},
		NextCursor: "next-page",
	}}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	server.tasks = queries
	for _, path := range []string{"/api/v1/tasks?limit=7&cursor=current", "/api/v1/activity?limit=7&cursor=current"} {
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
		if len(body.Items) != 1 || body.Items[0].Status != apiTypes.TaskPending || body.NextCursor != "next-page" {
			t.Fatalf("%s response = %#v", path, body)
		}
		if queries.request != (etcd.PageRequest{Limit: 7, Cursor: "current"}) {
			t.Fatalf("%s request = %#v", path, queries.request)
		}
	}
}
