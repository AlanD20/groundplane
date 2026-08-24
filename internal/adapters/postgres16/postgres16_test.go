package postgres16

import (
	"bytes"
	"testing"

	"github.com/AlanD20/groundplane/internal/adapters"
)

// Rationale: PostgreSQL identity values must compile locally with explicit
// database context and mutable secret input, never unresolved placeholders.
func TestProvisionStepsCompileResolvedIdentity(t *testing.T) {
	steps := (&adapter{}).ProvisionSteps(adapters.ProvisionParams{
		Database: "api_5d3f9a", Role: "api_5d3f9a", Password: []byte("URL_safe-1"),
	})
	if len(steps) != 3 || steps[0].Database != "postgres" ||
		!bytes.Contains(steps[0].Stdin, []byte(`CREATE ROLE "api_5d3f9a" WITH LOGIN PASSWORD 'URL_safe-1'`)) ||
		bytes.Contains(steps[0].Stdin, []byte("<generated>")) {
		t.Fatalf("ProvisionSteps() = %#v", steps)
	}
	secret := steps[0].Stdin
	adapters.ClearSteps(steps)
	for _, character := range secret {
		if character != 0 {
			t.Fatal("ClearSteps() retained PostgreSQL identity input")
		}
	}
}

// Rationale: granted schema changes must run in the granted database, and
// final detach must transfer database ownership before dropping the role.
func TestGrantAndDetachStepsUseCorrectDatabaseContext(t *testing.T) {
	grant := (&adapter{}).GrantSteps(adapters.ProvisionParams{Role: "api_5d3f9a", GrantOn: "other_4a1b2c"})
	detach := (&adapter{}).DetachSteps(adapters.ProvisionParams{Role: "api_5d3f9a", Database: "api_5d3f9a"})
	defer adapters.ClearSteps(grant)
	defer adapters.ClearSteps(detach)
	if len(grant) != 2 || grant[1].Database != "other_4a1b2c" || len(detach) != 3 ||
		!bytes.Contains(detach[1].Stdin, []byte(`ALTER DATABASE "api_5d3f9a" OWNER TO postgres`)) {
		t.Fatalf("GrantSteps()/DetachSteps() = %#v / %#v", grant, detach)
	}
}

// Rationale: service-derived identities may contain hyphens, so every PostgreSQL identifier must be
// delimited without allowing a generated value to change the fixed statement structure.
func TestPostgreSQLStepsQuoteServiceDerivedIdentifiers(t *testing.T) {
	steps := (&adapter{}).ProvisionSteps(adapters.ProvisionParams{
		Database: `api-web_5d3f9a`, Role: `api-web_5d3f9a`, Password: []byte("safe'password"),
	})
	defer adapters.ClearSteps(steps)
	if !bytes.Contains(steps[0].Stdin, []byte(`CREATE ROLE "api-web_5d3f9a"`)) ||
		!bytes.Contains(steps[0].Stdin, []byte(`PASSWORD 'safe''password'`)) ||
		!bytes.Contains(steps[1].Stdin, []byte(`CREATE DATABASE "api-web_5d3f9a" OWNER "api-web_5d3f9a"`)) {
		t.Fatalf("ProvisionSteps() did not safely delimit identity: %#v", steps)
	}
}

// Rationale: registry metadata is operator-visible capability advice and must
// mirror ADR 0047's exact capture and restore procedures without stale flags.
func TestBackupStrategyMatchesPostgres16ArtifactContract(t *testing.T) {
	strategy := (&adapter{}).BackupStrategy()
	wantDump := "pg_dump --format=custom --compress=0 --no-owner --no-acl " +
		"--host=/var/run/postgresql --username=postgres --no-password --role=<role> --dbname=<db>"
	wantRestore := "pg_restore --clean --if-exists --no-owner --no-acl --exit-on-error --single-transaction " +
		"--host=/var/run/postgresql --username=postgres --no-password --role=<role> --dbname=<db>"
	if strategy.Dump != wantDump || strategy.Restore != wantRestore {
		t.Fatalf("BackupStrategy() = %#v, want dump %q restore %q", strategy, wantDump, wantRestore)
	}
}
