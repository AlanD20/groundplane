package controller

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const nativeAdmissionTaskID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"

type closedMutationAdmission struct {
	calls   int
	failure error
	open    bool
}

func (admission *closedMutationAdmission) CheckMutation(_ context.Context, nativeAbortTaskID string) error {
	admission.calls++
	if admission.failure != nil {
		return admission.failure
	}
	if admission.open || nativeAbortTaskID == nativeAdmissionTaskID {
		return nil
	}
	return errs.New(errs.KindResourceInUse, "native recovery is active")
}

// Rationale: listeners are a qualification prerequisite, not write permission.
// A real Script authoring route must not reach its mutation port during trial.
func TestNativeMutationAdmissionBlocksScriptAuthoring(t *testing.T) {
	mutations, admission := &scriptOrderRouteMutations{}, &closedMutationAdmission{}
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{
		ScriptMutations: mutations, MutationAdmission: admission,
	})
	request := func() *httptest.ResponseRecorder {
		request := httptest.NewRequest(
			http.MethodPatch,
			"/api/v1/scripts/scr_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			strings.NewReader(`{"order":42}`),
		)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set(idempotencyKeyHeader, "trial-script-edit-0001")
		response := httptest.NewRecorder()
		server.HTTPHandler().ServeHTTP(response, request)
		return response
	}
	for index, failure := range []error{nil, errs.New(errs.KindInternal, "journal unavailable"), errors.New("private-journal-path")} {
		admission.failure = failure
		response := request()
		want := http.StatusConflict
		if failure != nil {
			want = http.StatusInternalServerError
		}
		if response.Code != want || mutations.calls != 0 || admission.calls != index+1 ||
			strings.Contains(response.Body.String(), "private-journal-path") {
			t.Fatalf(
				"trial admitted metadata or leaked a failure: status=%d writes=%d checks=%d",
				response.Code,
				mutations.calls,
				admission.calls,
			)
		}
	}
	admission.failure, admission.open = nil, true
	if response := request(); response.Code != http.StatusOK || mutations.calls != 1 {
		t.Fatal("qualified authoring did not resume")
	}
}

// Rationale: read access, native acceptance replay and exact native Abort remain
// usable; adjacent paths, other Tasks and other verbs cannot borrow exceptions.
func TestNativeMutationAdmissionHTTPExceptionsAreExact(t *testing.T) {
	for _, test := range []struct {
		method, path string
		allowed      bool
	}{
		{"GET", "/api/v1/scripts", true},
		{"HEAD", "/api/v1/tasks/" + nativeAdmissionTaskID, true},
		{"POST", "/api/v1/environments/env_x/blueprint/validate", true},
		{"POST", "/api/v1/environments/env_x/export-key", true},
		{"POST", "/api/v1/controller/update", true},
		{"POST", "/api/v1/tasks/" + nativeAdmissionTaskID + "/abort", true},
		{"POST", "/api/v1/tasks/task_01ARZ3NDEKTSV4RRFFQ69G5FAW/abort", false},
		{"POST", "/api/v1/tasks/" + nativeAdmissionTaskID + "/retry", false},
		{"DELETE", "/api/v1/tasks/" + nativeAdmissionTaskID + "/abort", false},
		{"POST", "/api/v1/tasks/" + nativeAdmissionTaskID + "/abort/extra", false},
		{"PATCH", "/api/v1/controller/update", false},
		{"POST", "/api/v1/controller/update/extra", false},
		{"PUT", "/api/v1/environments/env_x/blueprint", false},
		{"POST", "/api/v1/environments/env_x/blueprint/validate/extra", false},
	} {
		t.Run(test.method+test.path, func(t *testing.T) {
			server, calls := idempotencyBoundaryServer()
			server.mutationAdmission = &closedMutationAdmission{}
			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set(idempotencyKeyHeader, "trial-boundary-0001")
			response := httptest.NewRecorder()
			server.HTTPHandler().ServeHTTP(response, request)
			want := http.StatusConflict
			if test.allowed {
				want = http.StatusNoContent
			}
			if response.Code != want || (*calls == 1) != test.allowed {
				t.Fatalf("exception status=%d writes=%d, allowed=%v", response.Code, *calls, test.allowed)
			}
		})
	}
}

type nativeAdmissionBackupSchedules struct{ calls int }

func (schedules *nativeAdmissionBackupSchedules) RunBackupSchedules(context.Context, time.Time) error {
	schedules.calls++
	return nil
}

// Rationale: a trial must not publish due backups or rewrite/prune ordinary Task
// and idempotency state while its predecessor still owns possible recovery.
func TestNativeMutationAdmissionPausesScheduler(t *testing.T) {
	tasks, markers, agents, schedules := &fakeTaskExpiration{}, &fakeIdempotencyPruning{}, &fakeStaleAgentExpiration{}, &nativeAdmissionBackupSchedules{}
	admission := &closedMutationAdmission{}
	scheduler := &Scheduler{
		Server: &Server{mutationAdmission: admission}, tasks: tasks, idempotency: markers, agents: agents,
		backupSchedules: schedules, now: time.Now,
	}
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tasks.calls != 0 || tasks.pruneCalls != 0 || markers.calls != 0 || agents.calls != 0 || schedules.calls != 0 ||
		!scheduler.nextPrune.IsZero() {
		t.Fatal("trial scheduler reached ordinary durable mutation")
	}
	admission.failure = errs.New(errs.KindInternal, "journal unavailable")
	if scheduler.tick(context.Background()) == nil || tasks.calls != 0 {
		t.Fatal("unavailable admission did not fail closed")
	}
	scheduler.Server.mutationAdmission = nil
	if err := scheduler.tick(context.Background()); err != nil {
		t.Fatal(err)
	}
	if tasks.calls != 1 || tasks.pruneCalls != 1 || markers.calls != 1 || agents.calls != 1 || schedules.calls != 1 {
		t.Fatal("scheduler did not resume")
	}
}
