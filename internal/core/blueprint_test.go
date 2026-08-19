package core

import "testing"

// L0 — pure function tests. See docs/standards.md, section 13.

func TestServiceValidate_RejectsRollingStrategy(t *testing.T) {
	// Rationale: "rolling" is declared-deferred (mvp.md, "Release
	// strategy rules (locked in)") — Validate must reject it at the
	// type level so the Controller's schema validation and the domain
	// model agree, rather than only being caught by an API-layer check.
	svc := Service{ID: "svc_x", Name: "app-api", Image: "app:latest", Strategy: StrategyRolling}
	if err := svc.Validate(); err == nil {
		t.Error("expected Validate() to reject strategy=rolling, got nil")
	}
}

func TestServiceValidate_AcceptsImplementedStrategies(t *testing.T) {
	// Rationale: the inverse of the above — blue-green, recreate, and
	// the unset (declared-default) case must all pass, or every fixture
	// and every real deploy would fail validation.
	for _, strat := range []Strategy{"", StrategyBlueGreen, StrategyRecreate} {
		svc := Service{ID: "svc_x", Name: "app-api", Image: "app:latest", Strategy: strat}
		if err := svc.Validate(); err != nil {
			t.Errorf("Validate() with strategy=%q: unexpected error: %v", strat, err)
		}
	}
}

func TestServiceValidate_HealthcheckExactlyOneKind(t *testing.T) {
	// Rationale: mvp.md's environment-document mapping table locks
	// healthcheck to exactly one of http/tcp/pgrep — zero or two+ set is
	// ambiguous and must be rejected before it ever reaches a render.
	base := Service{ID: "svc_x", Name: "app-api", Image: "app:latest"}

	none := base
	if err := none.Validate(); err != nil {
		t.Errorf("zero healthcheck kinds should be valid (optional): %v", err)
	}

	both := base
	both.Healthcheck = Healthcheck{HTTP: "/up", TCP: "localhost:5432"}
	if err := both.Validate(); err == nil {
		t.Error("expected Validate() to reject two healthcheck kinds set at once, got nil")
	}
}

func TestEnvEntryValidate_SecretMustNotCarryPlaintextValue(t *testing.T) {
	// Rationale: this is the load-bearing invariant behind "desired
	// state never contains a secret value" (mvp.md) — a secret=true
	// entry with a non-empty literal source would leak a value into the
	// Blueprint YAML, defeating the whole encrypted-secret-store design.
	// See blueprint.md, "x-gp-entry".
	e := EnvEntry{
		ID: "ev_x", Kind: EntryKindEnv, Key: "DB_PASSWORD",
		Source:   EntrySource{Kind: SourceLiteral, Literal: "leaked!"},
		Exposure: []string{"all"},
		Secret:   true,
	}
	if err := e.Validate(); err == nil {
		t.Error("expected Validate() to reject a secret entry with a plaintext literal value, got nil")
	}
}

func TestEnvEntryValidate_ExposureRequiresAtLeastOneTarget(t *testing.T) {
	// Rationale: an entry with no Exposure is unresolvable at
	// materialization time (which file does it land in?) — must be
	// caught here, not discovered as a missing-file bug at render.
	e := EnvEntry{
		ID: "ev_x", Kind: EntryKindEnv, Key: "API_KEY",
		Source: EntrySource{Kind: SourceLiteral, Literal: "x"},
	}
	if err := e.Validate(); err == nil {
		t.Error("expected Validate() to reject an entry with empty Exposure, got nil")
	}
}

func TestEnvEntryValidate_SourceIsExactlyOneKind(t *testing.T) {
	// Rationale: literal, secret_ref, and fact are mutually exclusive
	// (api-cli.md, section 4) — this is the ONE place that invariant is
	// enforced at the type level, so a malformed entry never reaches the
	// renderer with an ambiguous source.
	e := EnvEntry{
		ID: "ev_x", Kind: EntryKindEnv, Key: "DATABASE_URL",
		Source:   EntrySource{Kind: SourceFact, Literal: "also-set", Fact: &FactRef{Attach: "api-db", Key: "pg16_URL"}},
		Exposure: []string{"api"},
	}
	if err := e.Validate(); err == nil {
		t.Error("expected Validate() to reject an entry with two source fields set, got nil")
	}
}

func TestProjectValidate_BackingProjectMustNotHaveTenant(t *testing.T) {
	// Rationale: mvp.md's backing-project model is explicit that backing
	// projects have no owning Tenant — a TenantID slipping onto one
	// would silently break the "backing services are platform-wide"
	// assumption everywhere else in the Controller.
	p := Project{ID: "prj_x", TenantID: "tnt_x", Slug: "shared-postgres", Name: "Shared PostgreSQL", Kind: ProjectKindBacking}
	if err := p.Validate(); err == nil {
		t.Error("expected Validate() to reject a backing project with a tenant_id, got nil")
	}
}
