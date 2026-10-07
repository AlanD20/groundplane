package handlers

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
)

// TASK-01: completed Attach detail must not depend on resolving a provisionable
// current Attach; missing historical metadata must be reported, not invented.
func TestCompletedTaskReadsCapturedDescriptionsWithoutDispatch(t *testing.T) {
	record := aliasTaskRecord(time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC), 600)
	record.Type, record.Executor, record.Status = taskjournal.TaskAttach, taskjournal.TaskExecutorAgent, taskjournal.TaskStatusCompleted
	record.Target, record.TargetName = ids.New(ids.KindAttach), "shared-access"
	record.TimeoutSeconds = 60
	record.Steps = []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep),
		Action: "Grant Backing access", Description: "Grant the selected Attach access.", Target: record.Target, TimeoutSeconds: 45}}
	queries := &fakeTaskQueries{task: keyvalue.Versioned[etcd.TaskRecord]{Record: record, ReadRevision: 17}}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	server.tasks = queries
	for _, historical := range []bool{false, true} {
		if historical {
			queries.task.Record.Steps[0].Action, queries.task.Record.Steps[0].Description = "", ""
			queries.task.Record.Steps[0].Target, queries.task.Record.Steps[0].TimeoutSeconds = "", 0
		}
		response := httptest.NewRecorder()
		server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+record.ID, nil))
		if response.Code != http.StatusOK {
			t.Fatalf("detail returned %d: %s", response.Code, response.Body.String())
		}
		var body apiTypes.Task
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if historical {
			if body.Steps[0].Action != "Descriptions were not recorded" || body.Steps[0].Description != "" {
				t.Fatal("invented historical description")
			}
		} else if body.Steps[0].Action != "Grant Backing access" || body.Steps[0].Description != "Grant the selected Attach access." || body.Steps[0].TimeoutSeconds != 45 || body.TargetName != "shared-access" || body.Executor != "agent" {
			t.Fatalf("lost captured presentation: %#v", body)
		}
	}
}
