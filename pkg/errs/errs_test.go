package errs

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// Rationale: the Kind catalog is the single closed mapping from internal
// failure semantics to every public error identity field.
func TestKindCatalogIsCompleteAndExact(t *testing.T) {
	expected := map[Kind]descriptor{
		KindTenantNotFound:              {CodeTenantNotFound, ClassNotFound, 404},
		KindProjectNotFound:             {CodeProjectNotFound, ClassNotFound, 404},
		KindEnvironmentNotFound:         {CodeEnvironmentNotFound, ClassNotFound, 404},
		KindServiceNotFound:             {CodeServiceNotFound, ClassNotFound, 404},
		KindBackingServiceNotFound:      {CodeBackingServiceNotFound, ClassNotFound, 404},
		KindAttachNotFound:              {CodeAttachNotFound, ClassNotFound, 404},
		KindTaskNotFound:                {CodeTaskNotFound, ClassNotFound, 404},
		KindTaskTimedOut:                {CodeTaskTimedOut, ClassRetryable, 503},
		KindSecretNotFound:              {CodeSecretNotFound, ClassNotFound, 404},
		KindConnectorNotFound:           {CodeConnectorNotFound, ClassNotFound, 404},
		KindRunnerNotFound:              {CodeRunnerNotFound, ClassNotFound, 404},
		KindRunnerSlugConflict:          {CodeRunnerSlugConflict, ClassConflict, 409},
		KindAgentNotFound:               {CodeAgentNotFound, ClassNotFound, 404},
		KindReleaseGroupNotFound:        {CodeReleaseGroupNotFound, ClassNotFound, 404},
		KindComponentNotFound:           {CodeComponentNotFound, ClassNotFound, 404},
		KindRecoveryPointNotFound:       {CodeRecoveryPointNotFound, ClassNotFound, 404},
		KindDeployInFlight:              {CodeDeployInFlight, ClassConflict, 409},
		KindTaskNotRetryable:            {CodeTaskNotRetryable, ClassConflict, 409},
		KindTaskNotAbortable:            {CodeTaskNotAbortable, ClassConflict, 409},
		KindTaskRetryInFlight:           {CodeTaskRetryInFlight, ClassConflict, 409},
		KindSlugConflict:                {CodeSlugConflict, ClassConflict, 409},
		KindNameConflict:                {CodeNameConflict, ClassConflict, 409},
		KindStateConflict:               {CodeStateConflict, ClassConflict, 409},
		KindResourceInUse:               {CodeResourceInUse, ClassConflict, 409},
		KindCursorExpired:               {CodeCursorExpired, ClassConflict, 409},
		KindIdempotencyInProgress:       {CodeIdempotencyInProgress, ClassConflict, 409},
		KindIdempotencyMismatch:         {CodeIdempotencyMismatch, ClassBadRequest, 400},
		KindStorageUnavailable:          {CodeStorageUnavailable, ClassRetryable, 503},
		KindZoneNotFound:                {CodeZoneNotFound, ClassNotFound, 404},
		KindRouteNotFound:               {CodeRouteNotFound, ClassNotFound, 404},
		KindVolumeNotFound:              {CodeVolumeNotFound, ClassNotFound, 404},
		KindEntryNotFound:               {CodeEntryNotFound, ClassNotFound, 404},
		KindScriptNotFound:              {CodeScriptNotFound, ClassNotFound, 404},
		KindReleaseNotFound:             {CodeReleaseNotFound, ClassNotFound, 404},
		KindReleaseRecoveryRequired:     {CodeReleaseRecoveryRequired, ClassConflict, 409},
		KindReleaseDeadlineTooShort:     {CodeReleaseDeadlineTooShort, ClassValidation, 422},
		KindReleasePlanTooLarge:         {CodeReleasePlanTooLarge, ClassValidation, 422},
		KindReleaseGroupTagRequired:     {CodeReleaseGroupTagRequired, ClassValidation, 422},
		KindRollbackNoPreviousRelease:   {CodeRollbackNoPreviousRelease, ClassConflict, 409},
		KindRollbackSourceExpired:       {CodeRollbackSourceExpired, ClassConflict, 409},
		KindBackupSourceNotFound:        {CodeBackupSourceNotFound, ClassNotFound, 404},
		KindStrategyNotImplemented:      {CodeStrategyNotImplemented, ClassValidation, 422},
		KindRotationNotImplemented:      {CodeRotationNotImplemented, ClassValidation, 422},
		KindAdapterManualOnly:           {CodeAdapterManualOnly, ClassValidation, 422},
		KindValidationFailed:            {CodeValidationFailed, ClassValidation, 422},
		KindMalformedRequest:            {CodeValidationFailed, ClassBadRequest, 400},
		KindScopeUnauthorized:           {CodeScopeUnauthorized, ClassValidation, 422},
		KindConnectorScopeInvalid:       {CodeConnectorScopeInvalid, ClassValidation, 422},
		KindRequestNotFound:             {CodeRequestNotFound, ClassNotFound, 404},
		KindRequestMethodNotAllowed:     {CodeRequestMethodNotAllowed, ClassMethodNotAllowed, 405},
		KindRequestNotAcceptable:        {CodeRequestNotAcceptable, ClassNotAcceptable, 406},
		KindRequestUnsupportedMediaType: {CodeRequestUnsupportedMediaType, ClassUnsupportedMediaType, 415},
		KindRequestTooLarge:             {CodeRequestFailed, ClassBadRequest, 413},
		KindRequestUnavailable:          {CodeRequestFailed, ClassRetryable, 503},
		KindRequestFailed:               {CodeRequestFailed, ClassInternal, 500},
		KindNotImplemented:              {CodeNotImplemented, ClassInternal, 501},
		KindInternal:                    {CodeInternal, ClassInternal, 500},
	}
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

// Rationale: unrecognized Kinds must fail closed as internal errors rather
// than creating a dynamic public error identity escape hatch.
func TestUnknownKindFailsClosedAsInternal(t *testing.T) {
	err := New(Kind(65535), "programming error")
	if err.Kind() != KindInternal || err.Code != CodeInternal || err.Class() != ClassInternal ||
		err.HTTPStatus() != 500 {
		t.Fatalf("unknown Kind produced %#v", err)
	}
}

// Rationale: malformed request syntax is a transport failure and therefore
// must remain distinct from semantically invalid input.
func TestMalformedAndSemanticValidationRemainDistinct(t *testing.T) {
	malformed := New(KindMalformedRequest, "malformed")
	semantic := New(KindValidationFailed, "invalid")

	if malformed.Code != CodeValidationFailed || semantic.Code != CodeValidationFailed {
		t.Fatalf("validation codes = %q and %q", malformed.Code, semantic.Code)
	}
	if malformed.Class() != ClassBadRequest || malformed.HTTPStatus() != 400 {
		t.Fatalf("malformed descriptor = %s/%d", malformed.Class(), malformed.HTTPStatus())
	}
	if semantic.Class() != ClassValidation || semantic.HTTPStatus() != 422 {
		t.Fatalf("semantic descriptor = %s/%d", semantic.Class(), semantic.HTTPStatus())
	}
	if errors.Is(malformed, semantic) {
		t.Fatal("different Kinds sharing one Code must not match")
	}
}

// Rationale: conflict, idempotency, and storage statuses are descriptor-owned
// and cannot be selected independently at a call site.
func TestAcceptedPersistenceAndIdempotencyStatuses(t *testing.T) {
	tests := map[Kind]int{
		KindRunnerSlugConflict:    409,
		KindSlugConflict:          409,
		KindNameConflict:          409,
		KindStateConflict:         409,
		KindResourceInUse:         409,
		KindCursorExpired:         409,
		KindIdempotencyInProgress: 409,
		KindIdempotencyMismatch:   400,
		KindStorageUnavailable:    503,
	}
	for kind, want := range tests {
		if got := New(kind, "test").HTTPStatus(); got != want {
			t.Errorf("Kind %d status = %d, want %d", kind, got, want)
		}
	}
}

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
	if got := wrapped.Error(); strings.Index(got, primary.Error()) > strings.Index(got, cleanup.Error()) {
		t.Fatalf("cleanup preceded primary in diagnostics: %q", got)
	}
	if problem := wrapped.ToProblem(); strings.Contains(problem.Detail, "private") {
		t.Fatalf("joined private causes leaked publicly: %#v", problem)
	}
}

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
	if body["type"] != ProblemType || body["code"] != string(CodeAttachNotFound) ||
		body["status"] != float64(404) || body["detail"] != "attach missing" {
		t.Fatalf("problem JSON = %s", encoded)
	}
}

// Rationale: inbound problem reconstruction is trusted only when type, code,
// class-derived status, and descriptor identity form an exact closed tuple.
func TestFromProblemAcceptsOnlyExactCatalogTuples(t *testing.T) {
	for kind := Kind(1); kind < kindLimit; kind++ {
		source := New(kind, "detail").ToProblem()
		got, ok := FromProblem(source)
		if !ok || got.Kind() != kind {
			t.Errorf("FromProblem(%d) = %#v, %t", kind, got, ok)
		}
	}

	invalid := []Problem{
		{Type: ProblemType, Status: 409, Code: CodeValidationFailed},
		{Type: ProblemType, Status: 500, Code: Code("future.error")},
		{Type: "https://example.invalid/problem", Status: 500, Code: CodeInternal},
	}
	for _, problem := range invalid {
		if got, ok := FromProblem(problem); ok || got != nil {
			t.Errorf("invalid problem was accepted: %#v", problem)
		}
	}
}

// Rationale: exported public code values remain stable even though production
// constructors accept only closed internal Kinds.
func TestPublicCodesUseCanonicalDotNamespaces(t *testing.T) {
	if CodeDeployInFlight != "deploy.in_flight" {
		t.Errorf("CodeDeployInFlight = %q", CodeDeployInFlight)
	}
	if CodeStrategyNotImplemented != "strategy.not_implemented" {
		t.Errorf("CodeStrategyNotImplemented = %q", CodeStrategyNotImplemented)
	}
}

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

	if domainError.Kind() != KindStorageUnavailable || domainError.Class() != ClassRetryable ||
		domainError.Op() != "storage" || domainError.HTTPStatus() != 503 {
		t.Fatalf("canonical accessors changed after mutation: %#v", domainError)
	}
	if got := domainError.Error(); !strings.Contains(got, string(CodeStorageUnavailable)) ||
		strings.Contains(got, "mutated") {
		t.Fatalf("Error() used mutable problem identity: %q", got)
	}

	problem := domainError.ToProblem()
	if problem.Type != ProblemType || problem.Code != CodeStorageUnavailable || problem.Status != 503 ||
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
	if zeroed.Type != ProblemType || zeroed.Code != CodeStorageUnavailable || zeroed.Status != 503 ||
		zeroed.Detail != "Service Unavailable" || domainError.Class() != ClassRetryable || domainError.Op() != "storage" {
		t.Fatalf("zeroed schema carrier changed canonical output: %#v", zeroed)
	}
}

// Rationale: a literal zero or invalid Error can be produced by framework
// reflection; every accessor and projection must fail closed as internal.
func TestZeroErrorFailsClosedAsCanonicalInternal(t *testing.T) {
	domainError := &Error{}
	if domainError.Kind() != KindInternal || domainError.Class() != ClassInternal ||
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
	if problem.Type != ProblemType || problem.Code != CodeInternal || problem.Status != 500 ||
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
	if problem := invalid.ToProblem(); problem.Code != CodeInternal || problem.Status != 500 ||
		problem.Title != "Internal Server Error" || problem.Detail != "Internal Server Error" {
		t.Fatalf("invalid Error problem = %#v", problem)
	}
}

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
	if problem.Status != 501 || problem.Title != string(CodeNotImplemented) ||
		problem.Detail != "rolling strategy is not implemented" {
		t.Fatalf("not-implemented problem = %#v", problem)
	}
}
