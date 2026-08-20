package errs

import (
	"errors"
	"fmt"
	"testing"
)

// L0 — pure function tests. Table-driven, no mocks. See
// docs/standards.md, section 13.

func TestNew_DerivesClassAndOpFromCode(t *testing.T) {
	// Rationale: Class and Op must never be set independently of Code —
	// that's the whole point of auto-derivation (one place decides a
	// Code's semantics, not each call site). This locks that contract.
	cases := []struct {
		code      Code
		wantClass Class
		wantOp    string
	}{
		{CodeServiceNotFound, ClassNotFound, "service"},
		{CodeDeployInFlight, ClassConflict, "deploy"},
		{CodeStrategyNotImplemented, ClassValidation, "strategy"},
		{CodeTaskTimedOut, ClassRetryable, "task"},
		{CodeRequestNotFound, ClassNotFound, "request"},
		{CodeRequestMethodNotAllowed, ClassMethodNotAllowed, "request"},
		{CodeRequestNotAcceptable, ClassNotAcceptable, "request"},
		{CodeRequestUnsupportedMediaType, ClassUnsupportedMediaType, "request"},
		{CodeRequestFailed, ClassInternal, "request"},
	}
	for _, c := range cases {
		e := New(c.code, "message")
		if e.Class != c.wantClass {
			t.Errorf("New(%s).Class = %s, want %s", c.code, e.Class, c.wantClass)
		}
		if e.Op != c.wantOp {
			t.Errorf("New(%s).Op = %s, want %s", c.code, e.Op, c.wantOp)
		}
	}
}

func TestHTTPStatus_NotImplementedIsAlways501(t *testing.T) {
	// Rationale: CodeNotImplemented's Class is "internal" (would
	// otherwise map to 500), but its literal HTTP meaning is 501 — this
	// is the one documented special case in HTTPStatus() and needs its
	// own test so a future refactor of the Class switch can't silently
	// regress it back to 500.
	got := New(CodeNotImplemented, "x").HTTPStatus()
	if got != 501 {
		t.Errorf("HTTPStatus() = %d, want 501", got)
	}
}

func TestHTTPStatus_DerivesFromClass(t *testing.T) {
	// Rationale: every Class must map to exactly one status, and that
	// mapping is the ONLY place status codes are decided (the Controller
	// never special-cases a Code directly).
	cases := map[Class]int{
		ClassBadRequest:           400,
		ClassValidation:           422,
		ClassNotFound:             404,
		ClassMethodNotAllowed:     405,
		ClassNotAcceptable:        406,
		ClassConflict:             409,
		ClassNeedsInteraction:     409,
		ClassUnsupportedMediaType: 415,
		ClassRetryable:            503,
		ClassInternal:             500,
	}
	for class, want := range cases {
		e := &Error{Class: class}
		if got := e.HTTPStatus(); got != want {
			t.Errorf("Class %s -> HTTPStatus() = %d, want %d", class, got, want)
		}
	}
}

func TestAcceptedPersistenceErrorsHaveStableStatuses(t *testing.T) {
	// Rationale: storage conflicts and outages drive safe retry behavior;
	// changing their classes would make callers retry conflicts or hide outages.
	cases := []struct {
		code   Code
		status int
	}{
		{CodeStateConflict, 409},
		{CodeResourceInUse, 409},
		{CodeCursorExpired, 409},
		{CodeIdempotencyMismatch, 400},
		{CodeStorageUnavailable, 503},
	}
	for _, test := range cases {
		if got := New(test.code, "test").HTTPStatus(); got != test.status {
			t.Errorf("%s status = %d, want %d", test.code, got, test.status)
		}
	}
}

func TestIs_ComparesByCodeOnly(t *testing.T) {
	// Rationale: errors.Is must match on Code regardless of Message or
	// wrapped cause — that's what lets callers write
	// errors.Is(err, errs.New(SomeCode, "")) without caring about the
	// original message.
	a := New(CodeServiceNotFound, "app-api not found")
	b := New(CodeServiceNotFound, "a completely different message")
	c := New(CodeProjectNotFound, "app-api not found")

	if !errors.Is(a, b) {
		t.Error("expected same-Code errors to match via errors.Is")
	}
	if errors.Is(a, c) {
		t.Error("expected different-Code errors NOT to match via errors.Is")
	}
}

func TestWrap_MessageComesFromCause(t *testing.T) {
	// Rationale: Wrap's two-argument shape (code, err) — no separate
	// message param — only makes sense if the message is derived from
	// err.Error(); this pins that behavior.
	cause := fmt.Errorf("dial tcp: connection refused")
	wrapped := Wrap(CodeInternal, cause)

	if wrapped.Message != cause.Error() {
		t.Errorf("Wrap message = %q, want %q", wrapped.Message, cause.Error())
	}
	if !errors.Is(wrapped, cause) {
		t.Error("expected Wrap to preserve Unwrap() chain to the cause")
	}
}

func TestIsNotFound_IsRetryable(t *testing.T) {
	// Rationale: these helpers are how the rest of the codebase branches
	// on error semantics WITHOUT string-matching messages (a banned
	// pattern) — cover both the positive and negative case for each.
	notFound := New(CodeServiceNotFound, "x")
	retryable := New(CodeTaskTimedOut, "x")
	plain := fmt.Errorf("not a domain error")

	if !IsNotFound(notFound) {
		t.Error("expected IsNotFound(notFound) to be true")
	}
	if IsNotFound(retryable) {
		t.Error("expected IsNotFound(retryable) to be false")
	}
	if IsNotFound(plain) {
		t.Error("expected IsNotFound(plain stdlib error) to be false, not panic")
	}
	if !IsRetryable(retryable) {
		t.Error("expected IsRetryable(retryable) to be true")
	}
}

func TestToProblem_CarriesCodeVerbatim(t *testing.T) {
	// Rationale: the Code string IS the RFC 7807 `code` the API returns,
	// verbatim (docs/standards.md, section 3) — this guards
	// against ToProblem ever normalizing/renaming it.
	e := New(CodeAttachNotFound, "attach att_xyz not found", WithDetails(map[string]any{"attach_id": "att_xyz"}))
	p := e.ToProblem()

	if p.Code != CodeAttachNotFound {
		t.Errorf("Problem.Code = %s, want %s", p.Code, CodeAttachNotFound)
	}
	if p.Type != ProblemType {
		t.Errorf("Problem.Type = %q, want %q", p.Type, ProblemType)
	}
	if p.Status != 404 {
		t.Errorf("Problem.Status = %d, want 404", p.Status)
	}
	if p.Details["attach_id"] != "att_xyz" {
		t.Errorf("Problem.Details[attach_id] = %v, want att_xyz", p.Details["attach_id"])
	}
}

func TestPublicCodesUseCanonicalDotNamespaces(t *testing.T) {
	// Rationale: these code strings are part of the public API and must not
	// regress to the superseded flat spellings.
	if CodeDeployInFlight != "deploy.in_flight" {
		t.Errorf("CodeDeployInFlight = %q, want deploy.in_flight", CodeDeployInFlight)
	}
	if CodeStrategyNotImplemented != "strategy.not_implemented" {
		t.Errorf("CodeStrategyNotImplemented = %q, want strategy.not_implemented", CodeStrategyNotImplemented)
	}
}

func TestValidationProblemUsesUnprocessableEntity(t *testing.T) {
	// Rationale: once transport decoding succeeds, parameter, schema, and
	// semantic validation failures use 422 rather than malformed-input 400.
	problem := New(CodeValidationFailed, "invalid parameter").ToProblem()
	if problem.Status != 422 {
		t.Errorf("Problem.Status = %d, want 422", problem.Status)
	}
}
