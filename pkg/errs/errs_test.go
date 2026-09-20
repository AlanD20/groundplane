package errs

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

func expectedKindCatalog() map[Kind]descriptor {
	return map[Kind]descriptor{
		KindTenantNotFound:                     {"tenant.not_found", "not_found", 404},
		KindProjectNotFound:                    {"project.not_found", "not_found", 404},
		KindEnvironmentNotFound:                {"environment.not_found", "not_found", 404},
		KindServiceNotFound:                    {"service.not_found", "not_found", 404},
		KindBackingServiceNotFound:             {"backing_service.not_found", "not_found", 404},
		KindAttachNotFound:                     {"attach.not_found", "not_found", 404},
		KindTaskNotFound:                       {"task.not_found", "not_found", 404},
		KindTaskTimedOut:                       {"task.timed_out", "retryable", 503},
		KindSecretNotFound:                     {"secret.not_found", "not_found", 404},
		KindConnectorNotFound:                  {"connector.not_found", "not_found", 404},
		KindRunnerNotFound:                     {"runner.not_found", "not_found", 404},
		KindRunnerSlugConflict:                 {"runner.slug_conflict", "conflict", 409},
		KindAgentNotFound:                      {"agent.not_found", "not_found", 404},
		KindReleaseGroupNotFound:               {"release_group.not_found", "not_found", 404},
		KindComponentNotFound:                  {"component.not_found", "not_found", 404},
		KindRecoveryPointNotFound:              {"recovery_point.not_found", "not_found", 404},
		KindDeployInFlight:                     {"deploy.in_flight", "conflict", 409},
		KindTaskNotRetryable:                   {"task.not_retryable", "conflict", 409},
		KindTaskNotAbortable:                   {"task.not_abortable", "conflict", 409},
		KindTaskRetryInFlight:                  {"task.retry_in_flight", "conflict", 409},
		KindSlugConflict:                       {"slug.conflict", "conflict", 409},
		KindNameConflict:                       {"name.conflict", "conflict", 409},
		KindStateConflict:                      {"state.conflict", "conflict", 409},
		KindResourceInUse:                      {"resource.in_use", "conflict", 409},
		KindCursorExpired:                      {"cursor.expired", "conflict", 409},
		KindIdempotencyInProgress:              {"idempotency.in_progress", "conflict", 409},
		KindIdempotencyMismatch:                {"idempotency.mismatch", "bad_request", 400},
		KindStorageUnavailable:                 {"storage.unavailable", "retryable", 503},
		KindWorkloadImageResolutionBusy:        {"workload.image_resolution_busy", "conflict", 409},
		KindWorkloadImageResolutionUnavailable: {"workload.image_resolution_unavailable", "retryable", 503},
		KindZoneNotFound:                       {"zone.not_found", "not_found", 404},
		KindRouteNotFound:                      {"route.not_found", "not_found", 404},
		KindVolumeNotFound:                     {"volume.not_found", "not_found", 404},
		KindEntryNotFound:                      {"entry.not_found", "not_found", 404},
		KindScriptNotFound:                     {"script.not_found", "not_found", 404},
		KindScriptRetryUnsafe:                  {"script.retry_unsafe", "conflict", 409},
		KindReleaseNotFound:                    {"release.not_found", "not_found", 404},
		KindReleaseRecoveryRequired:            {"release.recovery_required", "conflict", 409},
		KindReleaseDeadlineTooShort:            {"release.deadline_too_short", "validation", 422},
		KindReleasePlanTooLarge:                {"release.plan_too_large", "validation", 422},
		KindReleaseGroupTagRequired:            {"release_group.tag_required", "validation", 422},
		KindRollbackNoPreviousRelease:          {"rollback.no_previous_release", "conflict", 409},
		KindRollbackSourceExpired:              {"rollback.source_expired", "conflict", 409},
		KindBackupSourceNotFound:               {"backup_source.not_found", "not_found", 404},
		KindStrategyNotImplemented:             {"strategy.not_implemented", "validation", 422},
		KindRotationNotImplemented:             {"rotation.not_implemented", "validation", 422},
		KindAdapterCustomOnly:                  {"adapter.custom_only", "validation", 422},
		KindValidationFailed:                   {"validation.failed", "validation", 422},
		KindMalformedRequest:                   {"validation.failed", "bad_request", 400},
		KindScopeUnauthorized:                  {"scope.unauthorized", "validation", 422},
		KindConnectorScopeInvalid:              {"connector.scope_invalid", "validation", 422},
		KindRequestNotFound:                    {"request.not_found", "not_found", 404},
		KindRequestMethodNotAllowed:            {"request.method_not_allowed", "method_not_allowed", 405},
		KindRequestNotAcceptable:               {"request.not_acceptable", "not_acceptable", 406},
		KindRequestUnsupportedMediaType:        {"request.unsupported_media_type", "unsupported_media_type", 415},
		KindRequestTooLarge:                    {"request.failed", "bad_request", 413},
		KindRequestUnavailable:                 {"request.failed", "retryable", 503},
		KindRequestFailed:                      {"request.failed", "internal", 500},
		KindNotImplemented:                     {"not_implemented", "internal", 501},
		KindInternal:                           {"internal", "internal", 500},
	}
}

// QA: UI-03, UI-05 - L0 error-catalog proof only; no HTTP framework mapping,
// generated client, Controller handler, or operator-visible failure is exercised.
// Rationale: every closed Kind must retain its independently specified public
// code, control-flow class, and HTTP status without an ambiguous inbound tuple.
func TestKindCatalogIsCompleteAndExact(t *testing.T) {
	expected := expectedKindCatalog()
	if len(expected) != int(kindLimit-1) {
		t.Fatalf("test catalog has %d kinds, runtime catalog has %d", len(expected), kindLimit-1)
	}

	seenTuples := make(map[struct {
		code   Code
		status int
	}]Kind)
	for kind := Kind(1); kind < kindLimit; kind++ {
		got, ok := descriptorFor(kind)
		if !ok {
			t.Fatalf("descriptorFor(%d) is missing", kind)
		}
		want, exists := expected[kind]
		if !exists {
			t.Fatalf("Kind %d is absent from the expected catalog", kind)
		}
		if got != want {
			t.Errorf("Kind %d descriptor = %#v, want %#v", kind, got, want)
		}
		domainError := New(kind, "catalog check")
		if domainError.Kind() != kind || domainError.Code != want.Code ||
			domainError.Class() != want.Class || domainError.HTTPStatus() != want.Status {
			t.Errorf("Kind %d constructor/accessors = %#v, want %#v", kind, domainError, want)
		}
		tuple := struct {
			code   Code
			status int
		}{got.Code, got.Status}
		if previous, duplicate := seenTuples[tuple]; duplicate {
			t.Errorf("Kinds %d and %d share ambiguous problem tuple %#v", previous, kind, tuple)
		}
		seenTuples[tuple] = kind
	}
}

// QA: UI-03, UI-05 - L0 constructor/accessor proof only; no HTTP request,
// framework fallback, log record, or client presentation is exercised.
// Rationale: unrecognized Kinds must fail closed as internal errors rather
// than creating a dynamic public error identity escape hatch.
func TestUnknownKindFailsClosedAsInternal(t *testing.T) {
	err := New(Kind(65535), "programming error")
	if err.Kind() != KindInternal || err.Code != "internal" || err.Class() != "internal" ||
		err.HTTPStatus() != 500 {
		t.Fatalf("unknown Kind produced %#v", err)
	}
}

// QA: UI-03 - L0 taxonomy proof only; no parser, HTTP response body,
// semantic validator, or pre-effect publication assertion is exercised.
// Rationale: malformed request syntax is a transport failure and therefore
// must remain distinct from semantically invalid input.
func TestMalformedAndSemanticValidationRemainDistinct(t *testing.T) {
	malformed := New(KindMalformedRequest, "malformed")
	semantic := New(KindValidationFailed, "invalid")

	if malformed.Code != "validation.failed" || semantic.Code != "validation.failed" {
		t.Fatalf("validation codes = %q and %q", malformed.Code, semantic.Code)
	}
	if malformed.Class() != "bad_request" || malformed.HTTPStatus() != 400 {
		t.Fatalf("malformed descriptor = %s/%d", malformed.Class(), malformed.HTTPStatus())
	}
	if semantic.Class() != "validation" || semantic.HTTPStatus() != 422 {
		t.Fatalf("semantic descriptor = %s/%d", semantic.Class(), semantic.HTTPStatus())
	}
	if errors.Is(malformed, semantic) {
		t.Fatal("different Kinds sharing one Code must not match")
	}
}

// QA: UI-03, TASK-07 - L0 error-chain and projection proof only; no Controller
// log sink, HTTP handler, CLI output, or real storage failure is exercised.
// Rationale: errors.Is must compare closed Kinds while Wrap retains a private
// cause for diagnostics without exposing it in a public problem.
func TestIsComparesKindAndWrapPreservesCause(t *testing.T) {
	first := New(KindServiceNotFound, "first")
	second := New(KindServiceNotFound, "second")
	other := New(KindProjectNotFound, "first")
	if !errors.Is(first, second) {
		t.Error("same Kind did not match")
	}
	if errors.Is(first, other) {
		t.Error("different Kinds matched")
	}

	cause := fmt.Errorf("dial failed")
	wrapped := Wrap(KindStorageUnavailable, cause)
	if !errors.Is(wrapped, cause) {
		t.Fatalf("wrapped error = %#v", wrapped)
	}
	problem := wrapped.ToProblem()
	if problem.Detail != "Service Unavailable" {
		t.Fatalf("detail = %q, want safe status detail", problem.Detail)
	}
	if strings.Contains(problem.Detail, cause.Error()) {
		t.Fatalf("problem detail leaked private cause: %q", problem.Detail)
	}
}

// QA: SVC-12, TASK-07 - L0 joined-error proof only; no failed rollout,
// compensation, durable outcome, HTTP response, or diagnostic log is exercised.
// Rationale: a failed operation remains the diagnostic primary when cleanup
// also fails, while the public boundary receives exactly one domain Error.
func TestWrapJoinedPreservesPrimaryAndCleanupCauses(t *testing.T) {
	primary := errors.New("primary private failure")
	cleanup := errors.New("cleanup private failure")
	wrapped := WrapJoined(KindInternal, primary, nil, cleanup)
	if !errors.Is(wrapped, primary) || !errors.Is(wrapped, cleanup) {
		t.Fatalf("joined error did not preserve causes: %v", wrapped)
	}
	var domainError *Error
	if !errors.As(wrapped, &domainError) || domainError != wrapped {
		t.Fatalf("joined error is not one outer Error: %#v", wrapped)
	}
	diagnostic := wrapped.Error()
	primaryIndex := strings.Index(diagnostic, primary.Error())
	cleanupIndex := strings.Index(diagnostic, cleanup.Error())
	if primaryIndex < 0 || cleanupIndex < 0 || primaryIndex >= cleanupIndex {
		t.Fatalf("diagnostics must contain primary before cleanup: %q", diagnostic)
	}
	if problem := wrapped.ToProblem(); strings.Contains(problem.Detail, "private") {
		t.Fatalf("joined private causes leaked publicly: %#v", problem)
	}
}

// QA: UI-03, UI-05 - L0 classification proof only; no request retry behavior,
// Controller decision, or operator error presentation is exercised.
// Rationale: KindOf and class helpers must inspect wrapped chains without
// inventing a classification for ordinary errors.
func TestKindOfAndClassHelpersInspectChains(t *testing.T) {
	wrapped := fmt.Errorf("outer: %w", New(KindAgentNotFound, "missing"))
	kind, ok := KindOf(wrapped)
	if !ok || kind != KindAgentNotFound {
		t.Fatalf("KindOf = %d, %t", kind, ok)
	}
	if !IsNotFound(wrapped) || IsRetryable(wrapped) {
		t.Fatal("not-found classification is incorrect")
	}
	if !IsRetryable(New(KindStorageUnavailable, "offline")) {
		t.Fatal("storage unavailable must be retryable")
	}
	if _, ok := KindOf(fmt.Errorf("plain")); ok {
		t.Fatal("plain error unexpectedly has a Kind")
	}
}

// QA: UI-03, UI-05 - L0 RFC 7807 serialization only; no content negotiation,
// HTTP headers, generated client, or Controller failure path is exercised.
// Rationale: JSON serialization is a public boundary and must emit exactly
// the canonical RFC problem fields.
func TestProblemJSONContainsOnlyRFC7807Fields(t *testing.T) {
	err := New(
		KindAttachNotFound,
		"attach missing",
	)
	encoded, marshalErr := json.Marshal(err)
	if marshalErr != nil {
		t.Fatalf("marshal error: %v", marshalErr)
	}
	var body map[string]any
	if decodeErr := json.Unmarshal(encoded, &body); decodeErr != nil {
		t.Fatalf("decode error JSON: %v", decodeErr)
	}
	expectedFields := map[string]struct{}{
		"type": {}, "title": {}, "status": {}, "detail": {}, "code": {},
	}
	for field := range body {
		if _, expected := expectedFields[field]; !expected {
			t.Errorf("error JSON exposes unexpected field %q", field)
		}
	}
	if len(body) != len(expectedFields) {
		t.Fatalf("problem JSON fields = %v, want exactly %v", body, expectedFields)
	}
	if body["type"] != "about:blank" || body["title"] != "attach.not_found" ||
		body["code"] != "attach.not_found" || body["status"] != float64(404) || body["detail"] != "attach missing" {
		t.Fatalf("problem JSON = %s", encoded)
	}
}

// QA: UI-03, UI-05 - L0 untrusted-problem decoding only; no network response,
// generated client integration, reconnect behavior, or CLI rendering is exercised.
// Rationale: inbound problem reconstruction is trusted only when type, code,
// class-derived status, and descriptor identity form an exact closed tuple.
func TestFromProblemAcceptsOnlyExactCatalogTuples(t *testing.T) {
	for kind, expected := range expectedKindCatalog() {
		source := Problem{
			Type: "about:blank", Title: "upstream title", Status: expected.Status,
			Detail: "upstream detail", Code: expected.Code,
		}
		got, ok := FromProblem(source)
		if !ok || got.Kind() != kind {
			t.Errorf("FromProblem(%d) = %#v, %t", kind, got, ok)
			continue
		}
		want := source
		if kind == KindInternal || kind == KindRequestFailed {
			want.Title = "Internal Server Error"
			want.Detail = "Internal Server Error"
		}
		if problem := got.ToProblem(); problem != want {
			t.Errorf("FromProblem(%d) projection = %#v, want %#v", kind, problem, want)
		}
	}

	invalid := []Problem{
		{Type: "about:blank", Status: 409, Code: "validation.failed"},
		{Type: "about:blank", Status: 500, Code: "future.error"},
		{Type: "https://example.invalid/problem", Status: 500, Code: "internal"},
	}
	for _, problem := range invalid {
		if got, ok := FromProblem(problem); ok || got != nil {
			t.Errorf("invalid problem was accepted: %#v", problem)
		}
	}
}

// QA: UI-03, UI-05, TASK-07 - L0 canonical-projection proof only; no Huma
// reflection path, HTTP response, secret-bearing incident, or client is exercised.
// Rationale: the embedded Problem exists for framework schema compatibility;
// mutating it must not override canonical identity or any emitted output.
func TestErrorOutputsIgnoreEmbeddedProblemMutation(t *testing.T) {
	cause := errors.New("private storage credential")
	domainError := Wrap(KindStorageUnavailable, cause)
	domainError.Problem = Problem{
		Type:   "https://attacker.invalid/problem",
		Title:  "mutated title",
		Status: 200,
		Detail: "mutated public detail",
		Code:   Code("mutated.code"),
	}

	if domainError.Kind() != KindStorageUnavailable || domainError.Class() != "retryable" ||
		domainError.Op() != "storage" || domainError.HTTPStatus() != 503 {
		t.Fatalf("canonical accessors changed after mutation: %#v", domainError)
	}
	if got := domainError.Error(); !strings.Contains(got, "storage.unavailable") ||
		strings.Contains(got, "mutated") {
		t.Fatalf("Error() used mutable problem identity: %q", got)
	}

	problem := domainError.ToProblem()
	if problem.Type != "about:blank" || problem.Code != "storage.unavailable" || problem.Status != 503 ||
		problem.Detail != "Service Unavailable" {
		t.Fatalf("canonical problem projection = %#v", problem)
	}
	encoded, err := json.Marshal(domainError)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}
	if strings.Contains(string(encoded), "mutated") || strings.Contains(string(encoded), cause.Error()) {
		t.Fatalf("JSON used mutable or private state: %s", encoded)
	}

	domainError.Problem = Problem{}
	zeroed := domainError.ToProblem()
	if zeroed.Type != "about:blank" || zeroed.Code != "storage.unavailable" || zeroed.Status != 503 ||
		zeroed.Detail != "Service Unavailable" || domainError.Class() != "retryable" || domainError.Op() != "storage" {
		t.Fatalf("zeroed schema carrier changed canonical output: %#v", zeroed)
	}
}

// QA: UI-03, UI-05 - L0 invalid-value normalization only; no framework-created
// zero value, HTTP request, handler fallback, or generated client is exercised.
// Rationale: a literal zero or invalid Error can be produced by framework
// reflection; every accessor and projection must fail closed as internal.
func TestZeroErrorFailsClosedAsCanonicalInternal(t *testing.T) {
	domainError := &Error{}
	if domainError.Kind() != KindInternal || domainError.Class() != "internal" ||
		domainError.Op() != "internal" || domainError.HTTPStatus() != 500 {
		t.Fatalf("zero error accessors = kind %d, class %q, op %q, status %d",
			domainError.Kind(), domainError.Class(), domainError.Op(), domainError.HTTPStatus())
	}
	if !errors.Is(domainError, New(KindInternal, "target")) {
		t.Fatal("zero Error did not compare as internal")
	}
	if kind, ok := KindOf(domainError); !ok || kind != KindInternal {
		t.Fatalf("KindOf = %d, %t", kind, ok)
	}
	problem := domainError.ToProblem()
	if problem.Type != "about:blank" || problem.Code != "internal" || problem.Status != 500 ||
		problem.Title != "Internal Server Error" || problem.Detail != "Internal Server Error" {
		t.Fatalf("zero Error problem = %#v", problem)
	}
	if got := domainError.Error(); got != "internal: Internal Server Error" {
		t.Fatalf("zero Error string = %q", got)
	}

	invalid := &Error{kind: Kind(65535)}
	if invalid.Kind() != KindInternal || !errors.Is(invalid, domainError) {
		t.Fatalf("invalid literal did not normalize: kind %d, error %v", invalid.Kind(), invalid)
	}
	if kind, ok := KindOf(invalid); !ok || kind != KindInternal {
		t.Fatalf("invalid KindOf = %d, %t", kind, ok)
	}
	if problem := invalid.ToProblem(); problem.Code != "internal" || problem.Status != 500 ||
		problem.Title != "Internal Server Error" || problem.Detail != "Internal Server Error" {
		t.Fatalf("invalid Error problem = %#v", problem)
	}
}

// QA: TASK-07, UI-03 - L0 public-projection secrecy only; no real secret,
// Controller log record, HTTP response, Task event, or CLI display is exercised.
// Rationale: raw diagnostics passed to opaque 500 constructors are private;
// public problems stay generic while 501 not-implemented remains explicit.
func TestOpaque500MessagesRemainPrivate(t *testing.T) {
	const secret = "database-password=private"
	for _, kind := range []Kind{KindInternal, KindRequestFailed} {
		domainError := New(kind, secret, WithTitle(secret))
		if !strings.Contains(domainError.Error(), secret) {
			t.Fatalf("Kind %d Error() lost private diagnostics: %q", kind, domainError.Error())
		}
		problem := domainError.ToProblem()
		if problem.Title != "Internal Server Error" || problem.Detail != "Internal Server Error" {
			t.Fatalf("Kind %d public problem = %#v", kind, problem)
		}
		encoded, err := json.Marshal(domainError)
		if err != nil {
			t.Fatalf("marshal Kind %d: %v", kind, err)
		}
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("Kind %d JSON leaked diagnostics: %s", kind, encoded)
		}
	}

	notImplemented := New(KindNotImplemented, "rolling strategy is not implemented")
	problem := notImplemented.ToProblem()
	if problem.Status != 501 || problem.Title != "not_implemented" ||
		problem.Detail != "rolling strategy is not implemented" {
		t.Fatalf("not-implemented problem = %#v", problem)
	}
}
