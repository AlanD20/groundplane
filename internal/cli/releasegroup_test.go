package cli

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReleaseGroupAddDefaultsOnFailure(t *testing.T) {
	// Rationale: create must materialize the contract default in the exact
	// request body rather than leaving default behavior to a handler.
	t.Parallel()

	body := `{"environment_id":"env_01J00000000000000000000000","name":"realtime","on_failure":"switch_back","order":null,"service_ids":["svc_01J00000000000000000000000","svc_01J00000000000000000000001"]}`
	server := exactRequestServer(t, http.MethodPost, "/api/v1/release-groups", body, http.StatusCreated, `{}`)
	defer server.Close()

	executeNoun(t, newReleaseGroupCmd(), server.URL, Scope{Environment: "env_01J00000000000000000000000", AsID: true},
		"add", "realtime", "--services", "svc_01J00000000000000000000000,svc_01J00000000000000000000001")
}

func TestReleaseGroupEditOmitsUnchangedOnFailure(t *testing.T) {
	// Rationale: PATCH omission preserves the stored policy; a flag default
	// must never overwrite it when the operator edits another field.
	t.Parallel()

	command := newReleaseGroupCmd()
	command.SetContext(context.WithValue(context.Background(), appKey{}, &App{Scope: Scope{}}))
	command.SetArgs([]string{"edit", "realtime"})
	if err := command.Execute(); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("Execute() error = %v, want validation failure", err)
	}
}

func TestReleaseGroupEditSendsChangedOnFailure(t *testing.T) {
	// Rationale: the CLI and JSON contracts use the same canonical enum value,
	// so the body must not translate or alias the operator's input.
	t.Parallel()

	body := `{"on_failure":"leave_active"}`
	groupID := "rg_01J00000000000000000000000"
	server := exactRequestServer(t, http.MethodPatch, "/api/v1/release-groups/"+groupID, body, http.StatusOK, `{}`)
	defer server.Close()

	executeNoun(t, newReleaseGroupCmd(), server.URL, Scope{AsID: true},
		"edit", groupID, "--on-failure", "leave_active")
}

func TestReleaseGroupOnFailureRejectsInvalidValue(t *testing.T) {
	// Rationale: invalid CLI input must enter the one canonical validation
	// taxonomy rather than escaping as an untyped parsing error.
	t.Parallel()

	_, err := releaseGroupOnFailure("continue")
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("releaseGroupOnFailure() error = %v, want %q", err, errs.CodeValidationFailed)
	}
}
