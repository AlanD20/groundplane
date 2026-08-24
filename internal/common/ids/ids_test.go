package ids

import (
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

// L0 — pure function tests. See docs/standards.md, section 13.

// Rationale: the locked shape is exactly <kind>_<26-char ULID>, so the
// generator must preserve its prefix and separator contract.
func TestNew_HasKindPrefixAndCorrectShape(t *testing.T) {
	id := New(KindService)
	parts := strings.SplitN(id, "_", 2)
	if len(parts) != 2 {
		t.Fatalf("New(KindService) = %q, want exactly one underscore separator", id)
	}
	if parts[0] != string(KindService) {
		t.Errorf("prefix = %q, want %q", parts[0], KindService)
	}
	if len(parts[1]) != 26 {
		t.Errorf("ULID body length = %d, want 26", len(parts[1]))
	}
	if strings.Contains(parts[1], "-") {
		t.Error("ULID body must not contain hyphens")
	}
}

// Rationale: the shared kind table prevents prefix drift across durable IDs.
func TestKindsHaveCanonicalPrefixesAndShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		kind   Kind
		prefix string
	}{
		{KindTenant, "tnt"},
		{KindProject, "prj"},
		{KindEnvironment, "env"},
		{KindService, "svc"},
		{KindDeployment, "dep"},
		{KindEnvEntry, "ev"},
		{KindVolume, "vol"},
		{KindAttach, "att"},
		{KindRoute, "rte"},
		{KindSecret, "sec"},
		{KindConnector, "con"},
		{KindRunner, "run"},
		{KindScript, "scr"},
		{KindBackupSource, "spt"},
		{KindRecoveryPoint, "rp"},
		{KindTask, "task"},
		{KindAssignment, "asgn"},
		{KindOperation, "op"},
		{KindPlan, "plan"},
		{KindStep, "step"},
		{KindAgent, "agt"},
		{KindNetwork, "net"},
		{KindBackingService, "bks"},
		{KindReleaseGroup, "rg"},
		{KindComponent, "cmp"},
	}

	for _, test := range tests {
		t.Run(test.prefix, func(t *testing.T) {
			if string(test.kind) != test.prefix {
				t.Fatalf("kind prefix = %q, want %q", test.kind, test.prefix)
			}
			id := New(test.kind)
			wantPrefix := test.prefix + "_"
			if !strings.HasPrefix(id, wantPrefix) {
				t.Fatalf("id = %q, want prefix %q", id, wantPrefix)
			}
			body := strings.TrimPrefix(id, wantPrefix)
			if len(body) != 26 {
				t.Fatalf("ULID body length = %d, want 26", len(body))
			}
			if _, err := ulid.ParseStrict(body); err != nil {
				t.Fatalf("ULID body %q is invalid: %v", body, err)
			}
		})
	}
}

// Rationale: a durable claim identity must not be confused with its Task or
// operation when reconnect and retry paths validate stale execution messages.
func TestAssignmentIDsUseTheCanonicalStableShape(t *testing.T) {
	t.Parallel()

	value := NewAt(KindAssignment, time.Date(2026, 8, 24, 12, 0, 0, 0, time.UTC), 12)
	if !strings.HasPrefix(value, "asgn_") {
		t.Fatalf("NewAt(KindAssignment) = %q, want asgn_ prefix", value)
	}
	if err := Validate(KindAssignment, value); err != nil {
		t.Fatalf("Validate(KindAssignment, %q): %v", value, err)
	}
	if err := Validate(KindAssignment, strings.Replace(value, "asgn_", "task_", 1)); err == nil {
		t.Fatal("Validate(KindAssignment) accepted a Task id")
	}
}

// Rationale: task-event deduplication depends on step identity surviving
// reconnects, so a step uses the same canonical stable-ID parser as every
// other referenced entity rather than a free-form procedure label.
func TestStepIDsUseTheCanonicalStableShape(t *testing.T) {
	t.Parallel()

	value := NewAt(KindStep, time.Date(2026, 8, 20, 12, 0, 0, 0, time.UTC), 11)
	if !strings.HasPrefix(value, "step_") {
		t.Fatalf("NewAt(KindStep) = %q, want step_ prefix", value)
	}
	if err := Validate(KindStep, value); err != nil {
		t.Fatalf("Validate(KindStep, %q): %v", value, err)
	}
	if err := Validate(KindStep, strings.Replace(value, "step_", "plan_", 1)); err == nil {
		t.Fatal("Validate(KindStep) accepted a plan id")
	}
}

// Rationale: mutation idempotency keys require the shared generator's raw,
// unprefixed 26-character ULID form.
func TestNewULIDReturnsRawULID(t *testing.T) {
	t.Parallel()

	id := NewULID()
	if len(id) != 26 {
		t.Fatalf("NewULID() length = %d, want 26", len(id))
	}
	if _, err := ulid.ParseStrict(id); err != nil {
		t.Fatalf("NewULID() = %q, want valid ULID: %v", id, err)
	}
}

// Rationale: every boundary must reject a valid ID carrying the wrong kind.
func TestValidateAcceptsOnlyTheRequestedCanonicalKind(t *testing.T) {
	value := "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if err := Validate(KindAgent, value); err != nil {
		t.Fatalf("Validate(KindAgent, %q): %v", value, err)
	}
	if err := Validate(KindService, value); err == nil {
		t.Fatal("Validate accepted an id of the wrong kind")
	}
}

// Rationale: malformed IDs cannot be reliably indexed or round-tripped.
func TestValidateRejectsMalformedIDs(t *testing.T) {
	tests := []string{
		"",
		"agt01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"agt_01ARZ3NDEKTSV4RRFFQ69G5FA",
		"agt_01arz3ndektsv4rrffq69g5fav",
		"agt_01ARZ3NDEKTSV4RRFFQ69G5FA!",
	}
	for _, value := range tests {
		if err := Validate(KindAgent, value); err == nil {
			t.Errorf("Validate(KindAgent, %q) succeeded", value)
		}
	}
}

// Rationale: back-to-back generated IDs must not collide.
func TestNew_IsUnique(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := New(KindTask)
		if seen[id] {
			t.Fatalf("duplicate id generated: %s", id)
		}
		seen[id] = true
	}
}

// Rationale: chronological ULID ordering supports etcd ranges and journal order.
func TestNewAt_IsChronologicallySortable(t *testing.T) {
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	earlier := NewAt(KindDeployment, t1, 42)
	later := NewAt(KindDeployment, t2, 42)

	if earlier >= later {
		t.Errorf("expected earlier id %q to sort before later id %q", earlier, later)
	}
}

// Rationale: fixed timestamp and entropy inputs must keep fixtures reproducible.
func TestNewAt_IsReproducibleForFixtures(t *testing.T) {
	ts := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	a := NewAt(KindEnvironment, ts, 7)
	b := NewAt(KindEnvironment, ts, 7)
	if a != b {
		t.Errorf("NewAt with the same timestamp+seed produced different ids: %s vs %s", a, b)
	}
}
