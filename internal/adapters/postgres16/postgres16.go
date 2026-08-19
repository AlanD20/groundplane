// Package postgres16 is the "postgres:16" adapter — the MVP's reference
// backing-service kind. Registering a new kind is exactly this shape:
// one package, one Register() call. Register is an explicit function,
// not an init() — docs/standards.md bans init()-based global
// wiring ("everything wired explicitly in internal/app"), so
// internal/app.NewController calls Register() itself rather than
// relying on a blank import's side effect. The "one package + one
// registration line" extensibility promise from architecture.md still
// holds; the registration line just lives in internal/app now.
package postgres16

import "github.com/AlanD20/groundplane/internal/adapters"

// Register adds this adapter to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	adapters.Register(&adapter{})
}

type adapter struct{}

func (a *adapter) Key() string          { return "postgres:16" }
func (a *adapter) Label() string        { return "PostgreSQL 16" }
func (a *adapter) DefaultImage() string { return "postgres:16-alpine" }
func (a *adapter) FactsPrefix() string  { return "pg16_" }
func (a *adapter) URLScheme() string    { return "pgsql://" }
func (a *adapter) Manual() bool         { return false }

func (a *adapter) ProvisionSteps(p adapters.ProvisionParams) []adapters.Step {
	return []adapters.Step{
		{Op: adapters.StepSQL, Params: map[string]string{
			"stmt": "CREATE ROLE <role> WITH LOGIN PASSWORD '<generated>'",
		}},
		{Op: adapters.StepSQL, Params: map[string]string{
			"stmt": "CREATE DATABASE <db> OWNER <role>",
		}},
		{Op: adapters.StepSQL, Params: map[string]string{
			"stmt": "GRANT ALL PRIVILEGES ON DATABASE <db> TO <role>",
		}},
	}
}

func (a *adapter) GrantSteps(p adapters.ProvisionParams) []adapters.Step {
	// p.GrantOn is the OTHER attach's database; the role already exists.
	return []adapters.Step{
		{Op: adapters.StepSQL, Params: map[string]string{
			"stmt": "GRANT CONNECT ON DATABASE <grant_on> TO <role>",
		}},
		{Op: adapters.StepSQL, Params: map[string]string{
			"stmt": "GRANT USAGE ON SCHEMA public TO <role>",
		}},
	}
}

func (a *adapter) DetachSteps(p adapters.ProvisionParams) []adapters.Step {
	return []adapters.Step{
		{Op: adapters.StepSQL, Params: map[string]string{
			"stmt": "REVOKE ALL PRIVILEGES ON DATABASE <db> FROM <role>",
		}},
		{Op: adapters.StepSQL, Params: map[string]string{
			"stmt": "DROP ROLE IF EXISTS <role>",
		}},
		// Dropping <db> itself is a SEPARATE, explicit confirmation in the
		// Console/CLI — detach revokes access; it does not delete data
		// unless the operator explicitly asks for that.
	}
}

func (a *adapter) BackupStrategy() adapters.BackupStrategy {
	return adapters.BackupStrategy{
		Dump:    "pg_dump --format=custom --dbname=<db>",
		Restore: "pg_restore --clean --if-exists -d <db>",
	}
}
