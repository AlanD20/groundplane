package cli

import (
	"errors"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestReleaseGroupAddDefaultsOnFailure(t *testing.T) {
	// Rationale: create must materialize the contract default in the exact
	// request body rather than leaving default behavior to a handler.
	t.Parallel()

	body := `{"environment":"production","name":"realtime","on_failure":"switch_back","order":null,"services":["api","worker"]}`
	server := exactRequestServer(t, http.MethodPost, "/api/v1/release-groups", body, http.StatusOK, `{}`)
	defer server.Close()

	executeNoun(t, newReleaseGroupCmd(), server.URL, Scope{Environment: "production"},
		"add", "realtime", "--service", "api", "--service", "worker")
}

func TestReleaseGroupEditOmitsUnchangedOnFailure(t *testing.T) {
	// Rationale: PATCH omission preserves the stored policy; a flag default
	// must never overwrite it when the operator edits another field.
	t.Parallel()

	server := exactRequestServer(t, http.MethodPatch, "/api/v1/release-groups/realtime", `{}`, http.StatusOK, `{}`)
	defer server.Close()

	executeNoun(t, newReleaseGroupCmd(), server.URL, Scope{}, "edit", "realtime")
}

func TestReleaseGroupEditSendsChangedOnFailure(t *testing.T) {
	// Rationale: the CLI and JSON contracts use the same canonical enum value,
	// so the body must not translate or alias the operator's input.
	t.Parallel()

	body := `{"on_failure":"leave_active"}`
	server := exactRequestServer(t, http.MethodPatch, "/api/v1/release-groups/realtime", body, http.StatusOK, `{}`)
	defer server.Close()

	executeNoun(t, newReleaseGroupCmd(), server.URL, Scope{},
		"edit", "realtime", "--on-failure", "leave_active")
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
