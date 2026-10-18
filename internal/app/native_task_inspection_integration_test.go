package app

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	api "github.com/AlanD20/groundplane/pkg/api"
)

// TASK-59: Task reads stay on their requested revision; completed execution
// remains inspectable without dispatch or retained diagnostic text.
func TestNativeTaskHTTPInspectionPreservesExecutionWhenDiagnosticsExpire(t *testing.T) {
	f := newNativeTaskInspectionFixture(t, io.Discard)
	taskID := f.volume.created.TaskID
	path := "/api/v1/tasks/" + taskID
	queued := nativeTaskHTTPGet[api.TaskDetails](t, f, path)
	if queued.Task.Status != "queued" || len(queued.Work.Items) != 1 ||
		queued.Work.Items[0].Phase != workexecution.Ready {
		t.Fatal("accepted Volume action was not visible as queued native work")
	}
	view := f.volume.start(t, taskID)
	current := nativeTaskHTTPGet[api.TaskDetails](t, f, path)
	if current.Task.Status != "running" || current.Task.Counts.Running != 1 ||
		current.Work.Items[0].ActiveAttemptID != view.Attempt.Attempt.ID {
		t.Fatal("Task details did not reflect the actual started attempt")
	}
	fixed := nativeTaskHTTPGet[api.TaskDetails](t, f, fmt.Sprintf("%s?revision=%d", path, queued.Work.Revision))
	if fixed.Task.Status != "queued" || fixed.Work.Items[0].AttemptCount != 0 ||
		fixed.Work.Revision != queued.Work.Revision {
		t.Fatal("fixed-revision details mixed newer execution state into an older snapshot")
	}
	workPath := path + "/work/" + view.Work.Item.Item.ID
	attempts := nativeTaskHTTPGet[api.TaskAttemptPage](t, f, workPath+"/attempts")
	if len(attempts.Items) != 1 || attempts.Items[0].ID != view.Attempt.Attempt.ID || attempts.Items[0].StartedAt == nil ||
		attempts.Items[0].HostID != view.Attempt.Attempt.Executor.HostID ||
		attempts.Items[0].AgentID != view.Attempt.Attempt.Executor.AgentID {
		t.Fatal("attempt inspection lost host/process identity or actual start time")
	}
	stage := workexecution.Stage{
		Key:       "prepare_storage",
		Name:      "Prepare persistent storage",
		State:     workexecution.StageRunning,
		Expected:  "Verify the captured data directory and Docker volume ownership",
		StartedAt: view.Attempt.Attempt.StartedAt,
	}
	if err := f.diagnostics.Report(t.Context(), taskID, view.Work.Item.Item.ID, view.Attempt.Attempt.ID,
		view.Attempt.Attempt.Executor, 1, stage, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	stagesPath := workPath + "/attempts/" + view.Attempt.Attempt.ID + "/stages"
	running := nativeTaskHTTPGet[api.TaskStages](t, f, stagesPath)
	if !running.Available || len(running.Items) != 1 || running.Items[0].Expected != stage.Expected {
		t.Fatal("named stage evidence was lost in HTTP inspection")
	}
	f.finish(t, view)
	completed := nativeTaskHTTPGet[api.TaskDetails](t, f, path)
	if completed.Task.Status != "succeeded" || completed.Task.Counts.Succeeded != 1 ||
		completed.Work.Items[0].Outcome != workexecution.Succeeded {
		t.Fatal("completed native execution was not projected independently of diagnostics")
	}
	stages := nativeTaskHTTPGet[api.TaskStages](t, f, stagesPath)
	if !stages.Incomplete || len(stages.Items) != 1 || stages.Items[0].State != workexecution.StageUnknown ||
		stages.Items[0].FinishedAt != nil {
		t.Fatal("successful attempt fabricated completion of a stage whose observation was never recorded")
	}
	finished := *completed.Task.FinishedAt
	done, err := f.diagnostics.ExpireTask(t.Context(), taskID, finished.Add(2*time.Hour), time.Hour)
	if err != nil || !done {
		t.Fatalf("diagnostic expiry: %v", err)
	}
	expired := nativeTaskHTTPGet[api.TaskStages](t, f, stagesPath)
	if !expired.Expired || expired.Available || len(expired.Items) != 0 {
		t.Fatal("expired diagnostics appeared as current stage evidence")
	}
	retained := nativeTaskHTTPGet[api.TaskDetails](t, f, path)
	if retained.Task.Status != "succeeded" || retained.Task.Counts.Succeeded != 1 || len(retained.Work.Items) != 1 {
		t.Fatal("diagnostic expiry erased execution authority")
	}
	if applied, err := f.volume.inputs.ReadApplied(t.Context(), f.volume.created.Volume.ID, 0); err != nil ||
		applied == nil {
		t.Fatalf("diagnostic expiry removed resource applied state: %v", err)
	}
}

// TASK-59: Activity and Tasks are the same filtered newest-first snapshot.
// Continuing either cursor excludes subsequently accepted actions without gaps.
func TestNativeTaskHTTPActivitySharesFixedRevisionPagination(t *testing.T) {
	f := newNativeTaskInspectionFixture(t, io.Discard)
	for _, slug := range []string{"second", "third"} {
		if _, err := f.volume.mutations.CreateVolume(t.Context(), api.VolumeCreate{EnvironmentID: f.volume.environmentID, Slug: slug}, "native-page-"+slug); err != nil {
			t.Fatal(err)
		}
	}
	query := url.Values{
		"scope":         {f.volume.environmentID},
		"limit":         {"1"},
		"action":        {"create"},
		"resource_kind": {"volume"},
	}
	first := nativeTaskHTTPGet[api.ExecutionPage[api.TaskSummary]](t, f, "/api/v1/tasks?"+query.Encode())
	if len(first.Items) != 1 || !first.More || first.After != first.Items[0].ID || first.Revision == 0 {
		t.Fatal("first native Task page has no usable cursor")
	}
	if _, err := f.volume.mutations.CreateVolume(t.Context(), api.VolumeCreate{EnvironmentID: f.volume.environmentID, Slug: "later"}, "native-page-later"); err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{first.Items[0].ID: true}
	previous := first.Items[0].ID
	page := first
	for index := 0; page.More && index < 4; index++ {
		query.Set("after", page.After)
		query.Set("revision", fmt.Sprint(first.Revision))
		page = nativeTaskHTTPGet[api.ExecutionPage[api.TaskSummary]](t, f, "/api/v1/activity?"+query.Encode())
		if page.Revision != first.Revision {
			t.Fatal("Activity changed the Task snapshot revision")
		}
		for _, task := range page.Items {
			if seen[task.ID] || task.ID >= previous || task.Scope.EnvironmentID != f.volume.environmentID ||
				task.ResourceKind != "volume" {
				t.Fatal("Activity duplicated, reordered or changed the scope of Task history")
			}
			seen[task.ID], previous = true, task.ID
		}
	}
	if page.More || len(seen) != 3 {
		t.Fatalf("snapshot page chain contains %d actions, more=%t; want the original three", len(seen), page.More)
	}
	refreshed := nativeTaskHTTPGet[api.ExecutionPage[api.TaskSummary]](
		t,
		f,
		"/api/v1/tasks?scope="+f.volume.environmentID+"&limit=25",
	)
	if len(refreshed.Items) != 4 {
		t.Fatal("refresh failed to expose the newly accepted action")
	}
	for _, suffix := range []string{"?scope=invalid", "?limit=101", "?status=invented", "?scope=all&scope=platform"} {
		response := nativeTaskHTTPRequest(f, "/api/v1/tasks"+suffix)
		if response.Code < 400 || response.Code >= 500 {
			t.Fatalf("invalid Task query %s returned %d: %s", suffix, response.Code, response.Body.String())
		}
	}
	missing := nativeTaskHTTPRequest(f, "/api/v1/tasks/"+ids.New(ids.KindTask))
	if missing.Code != http.StatusNotFound {
		t.Fatalf("missing Task returned %d: %s", missing.Code, missing.Body.String())
	}
}

func nativeTaskHTTPRequest(f *nativeTaskInspectionFixture, path string) *httptest.ResponseRecorder {
	response := httptest.NewRecorder()
	f.server.Mux.ServeHTTP(response, httptest.NewRequest(http.MethodGet, path, nil))
	return response
}

func nativeTaskHTTPGet[T any](t *testing.T, f *nativeTaskInspectionFixture, path string) T {
	t.Helper()
	var value T
	response := nativeTaskHTTPRequest(f, path)
	if response.Code != http.StatusOK {
		t.Fatalf("GET %s returned %d: %s", path, response.Code, response.Body.String())
	}
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}
