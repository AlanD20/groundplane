package cli

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// QA: GRP-01, UI-01; local CLI request shaping only, not persistence or group execution.
// Rationale: create must materialize the contract default in the exact
// request body rather than leaving default behavior to a handler.
func TestReleaseGroupAddDefaultsOnFailure(t *testing.T) {
	t.Parallel()

	body := `{"environment_id":"env_01J00000000000000000000000","name":"realtime","on_failure":"switch_back","order":null,"service_ids":["svc_01J00000000000000000000000","svc_01J00000000000000000000001"]}`
	server := exactRequestServer(t, http.MethodPost, "/api/v1/release-groups", body, http.StatusCreated, `{}`)
	defer server.Close()

	executeNoun(t, newReleaseGroupCmd(), server.URL, Scope{Environment: "env_01J00000000000000000000000", AsID: true},
		"add", "realtime", "--services", "svc_01J00000000000000000000000,svc_01J00000000000000000000001")
}

// QA: GRP-01, UI-03; local empty-edit validation only, not PATCH persistence or policy preservation.
// Rationale: an edit with no selected field must fail rather than sending an
// empty PATCH whose semantics could diverge between clients and the Controller.
func TestReleaseGroupEditRejectsNoChanges(t *testing.T) {
	t.Parallel()

	command := newReleaseGroupCmd()
	command.SetContext(context.WithValue(context.Background(), appKey{}, &App{Scope: Scope{}}))
	command.SetArgs([]string{"edit", "realtime"})
	if err := command.Execute(); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Execute() error = %v, want validation failure", err)
	}
}

// QA: GRP-01, UI-01; local CLI request shaping only, not stored policy or runtime failure handling.
// Rationale: the CLI and JSON contracts use the same canonical enum value,
// so the body must not translate or alias the operator's input.
func TestReleaseGroupEditSendsChangedOnFailure(t *testing.T) {
	t.Parallel()

	body := `{"on_failure":"leave_active"}`
	groupID := "rg_01J00000000000000000000000"
	server := exactRequestServer(t, http.MethodPatch, "/api/v1/release-groups/"+groupID, body, http.StatusOK, `{}`)
	defer server.Close()

	executeNoun(t, newReleaseGroupCmd(), server.URL, Scope{AsID: true},
		"edit", groupID, "--on-failure", "leave_active")
}

// QA: GRP-01, UI-03; pure enum validation only, not API publication or runtime failure policy.
// Rationale: invalid CLI input must enter the one canonical validation
// taxonomy rather than escaping as an untyped parsing error.
func TestReleaseGroupOnFailureRejectsInvalidValue(t *testing.T) {
	t.Parallel()

	_, err := releaseGroupOnFailure("continue")
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("releaseGroupOnFailure() error = %v, want %q", err, errs.CodeValidationFailed)
	}
}

// QA: GRP-04, UI-01; local request presence semantics only, not source selection or group rollback.
// Rationale: rollback's tag is an operator override for every member, while
// omission must remain a bodyless request that preserves automatic selection.
func TestReleaseGroupRollbackSendsOptionalTag(t *testing.T) {
	t.Parallel()
	groupID := "rg_01J00000000000000000000000"
	for _, test := range []struct {
		name string
		args []string
		body string
	}{
		{name: "omitted", args: []string{"rollback", groupID}},
		{name: "explicit", args: []string{"rollback", groupID, "--tag", "release-2026-09-04"}, body: `{"tag":"release-2026-09-04"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := exactRequestServer(t, http.MethodPost, "/api/v1/release-groups/"+groupID+"/rollback", test.body,
				http.StatusAccepted, `{"task_id":"task_group_rollback"}`)
			defer server.Close()

			executeNoun(t, newReleaseGroupCmd(), server.URL, Scope{AsID: true}, test.args...)
		})
	}
}

// QA: GRP-04, UI-01; local query encoding only, not preview accuracy or rollback execution.
// Rationale: an explicit preview tag must reach the read-only endpoint exactly,
// so the Controller rather than the CLI remains the rollback-source authority.
func TestReleaseGroupRollbackPreviewSendsExactOptionalTag(t *testing.T) {
	t.Parallel()
	groupID := "rg_01J00000000000000000000000"
	server := exactRequestServer(
		t,
		http.MethodGet,
		"/api/v1/release-groups/"+groupID+"/rollback-preview?tag=release-2026-09-04",
		"",
		http.StatusOK,
		`{"release_group_id":"`+groupID+`","revision":"42","sources":[]}`,
	)
	defer server.Close()
	executeNoun(
		t,
		newReleaseGroupCmd(),
		server.URL,
		Scope{AsID: true},
		"rollback-preview",
		groupID,
		"--tag",
		"release-2026-09-04",
	)
}

// QA: GRP-04, UI-03; local CLI validation only, not API rejection or absence of durable effects.
// Rationale: an explicitly supplied blank or padded rollback tag must not
// collapse into omitted-tag automatic selection.
func TestReleaseGroupRollbackRejectsExplicitBlankTag(t *testing.T) {
	t.Parallel()
	command := newReleaseGroupCmd()
	command.SetContext(context.WithValue(context.Background(), appKey{}, &App{Scope: Scope{AsID: true}}))
	command.SetArgs([]string{"rollback", "rg_01J00000000000000000000000", "--tag", " "})
	if err := command.Execute(); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Execute() error = %v, want validation failure", err)
	}
}
