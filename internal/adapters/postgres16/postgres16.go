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

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/adapters"
)

// Register adds this adapter to the registry. Called once, explicitly,
// from internal/app.NewController and NewAgent.
func Register() {
	adapters.Register(&adapter{})
}

type adapter struct{}

func (a *adapter) Key() string          { return "postgres:16" }
func (a *adapter) Label() string        { return "PostgreSQL 16" }
func (a *adapter) DefaultImage() string { return "postgres:16-alpine" }
func (a *adapter) FactsPrefix() string  { return "pg16_" }
func (a *adapter) URLScheme() string    { return "pgsql://" }
func (a *adapter) Port() string         { return "5432" }
func (a *adapter) FactSchema() []adapters.FactDefinition {
	return []adapters.FactDefinition{
		{Field: adapters.FactURL, Secret: true},
		{Field: adapters.FactHost},
		{Field: adapters.FactPort},
		{Field: adapters.FactDatabase},
		{Field: adapters.FactRole},
		{Field: adapters.FactPassword, Secret: true},
	}
}
func (a *adapter) Manual() bool         { return false }
func (a *adapter) SupportsGrants() bool { return true }

func (a *adapter) ProvisionSteps(p adapters.ProvisionParams) []adapters.Step {
	return []adapters.Step{
		{Op: adapters.StepSQL, Database: "postgres", Stdin: passwordSQL(
			"CREATE ROLE "+quoteIdentifier(p.Role)+" WITH LOGIN PASSWORD ", p.Password, "",
		)},
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"CREATE DATABASE ", quoteIdentifier(p.Database), " OWNER ", quoteIdentifier(p.Role),
		)},
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"GRANT ALL PRIVILEGES ON DATABASE ", quoteIdentifier(p.Database), " TO ", quoteIdentifier(p.Role),
		)},
	}
}

func (a *adapter) GrantSteps(p adapters.ProvisionParams) []adapters.Step {
	// p.GrantOn is the OTHER attach's database; the role already exists.
	return []adapters.Step{
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"GRANT CONNECT ON DATABASE ", quoteIdentifier(p.GrantOn), " TO ", quoteIdentifier(p.Role),
		)},
		{Op: adapters.StepSQL, Database: p.GrantOn, Stdin: sql(
			"GRANT USAGE ON SCHEMA public TO ", quoteIdentifier(p.Role),
		)},
	}
}

func (a *adapter) RevokeSteps(p adapters.ProvisionParams) []adapters.Step {
	return []adapters.Step{
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"REVOKE CONNECT ON DATABASE ", quoteIdentifier(p.GrantOn), " FROM ", quoteIdentifier(p.Role),
		)},
		{Op: adapters.StepSQL, Database: p.GrantOn, Stdin: sql(
			"REVOKE USAGE ON SCHEMA public FROM ", quoteIdentifier(p.Role),
		)},
	}
}

func (a *adapter) DetachSteps(p adapters.ProvisionParams) []adapters.Step {
	return []adapters.Step{
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"REVOKE ALL PRIVILEGES ON DATABASE ", quoteIdentifier(p.Database), " FROM ", quoteIdentifier(p.Role),
		)},
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"ALTER DATABASE ", quoteIdentifier(p.Database), " OWNER TO postgres",
		)},
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql("DROP ROLE IF EXISTS ", quoteIdentifier(p.Role))},
		// Dropping <db> itself is a SEPARATE, explicit confirmation in the
		// Console/CLI — detach revokes access; it does not delete data
		// unless the operator explicitly asks for that.
	}
}

func sql(parts ...string) []byte {
	length := 2
	for _, part := range parts {
		length += len(part)
	}
	statement := make([]byte, 0, length)
	for _, part := range parts {
		statement = append(statement, part...)
	}
	return append(statement, ';', '\n')
}

func passwordSQL(prefix string, password []byte, suffix string) []byte {
	statement := make([]byte, 0, len(prefix)+len(password)+len(suffix)+4)
	statement = append(statement, prefix...)
	statement = append(statement, '\'')
	for _, character := range password {
		statement = append(statement, character)
		if character == '\'' {
			statement = append(statement, '\'')
		}
	}
	statement = append(statement, '\'')
	statement = append(statement, suffix...)
	return append(statement, ';', '\n')
}

func quoteIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func (a *adapter) BackupStrategy() adapters.BackupStrategy {
	return adapters.BackupStrategy{
		Dump:    "pg_dump --format=custom --dbname=<db>",
		Restore: "pg_restore --clean --if-exists -d <db>",
	}
}
