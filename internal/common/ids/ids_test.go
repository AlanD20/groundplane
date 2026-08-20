package ids

import (
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"
)

// L0 — pure function tests. See docs/standards.md, section 13.

func TestNew_HasKindPrefixAndCorrectShape(t *testing.T) {
	// Rationale: the locked shape is exactly <kind>_<26-char ULID>, no
	// hyphens, no slugs (mvp.md, "Stable identifiers (locked)") — this
	// guards the shape at the one place ids are generated.
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
		{KindOperation, "op"},
		{KindPlan, "plan"},
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

func TestNew_IsUnique(t *testing.T) {
	// Rationale: two ids generated back-to-back must never collide —
	// this is the whole point of the 80 bits of CSPRNG randomness.
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		id := New(KindTask)
		if seen[id] {
			t.Fatalf("duplicate id generated: %s", id)
		}
		seen[id] = true
	}
}

func TestNewAt_IsChronologicallySortable(t *testing.T) {
	// Rationale: the 48-bit millisecond timestamp prefix is what makes
	// etcd ranges and the activity journal orderable for free
	// (architecture.md's "Unified logging"-adjacent ids section) — an
	// earlier NewAt must sort lexicographically before a later one.
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC)

	earlier := NewAt(KindDeployment, t1, 42)
	later := NewAt(KindDeployment, t2, 42)

	if earlier >= later {
		t.Errorf("expected earlier id %q to sort before later id %q", earlier, later)
	}
}

func TestNewAt_IsReproducibleForFixtures(t *testing.T) {
	// Rationale: mock fixtures use static, reproducible ULIDs (mvp.md's
	// locked rule: "fixtures never call time.Now()") — the same
	// timestamp+seed must always produce the same id across test runs.
	ts := time.Date(2026, 8, 16, 12, 0, 0, 0, time.UTC)
	a := NewAt(KindEnvironment, ts, 7)
	b := NewAt(KindEnvironment, ts, 7)
	if a != b {
		t.Errorf("NewAt with the same timestamp+seed produced different ids: %s vs %s", a, b)
	}
}
