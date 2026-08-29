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
	role := quoteIdentifier(p.Role)
	database := quoteIdentifier(p.Database)
	roleStatement := sql(
		"DO $groundplane$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = ",
		quoteLiteral(p.Role), ") THEN CREATE ROLE ", role, " WITH LOGIN; ELSE ALTER ROLE ", role,
		" WITH LOGIN; END IF; END $groundplane$",
	)
	roleStatement = append(roleStatement, passwordSQL("ALTER ROLE "+role+" PASSWORD ", p.Password, "")...)
	createDatabase := "CREATE DATABASE " + database + " OWNER " + role
	databaseStatement := []byte(
		"SELECT " + quoteLiteral(createDatabase) + " WHERE NOT EXISTS " +
			"(SELECT 1 FROM pg_catalog.pg_database WHERE datname = " + quoteLiteral(p.Database) + ")\n\\gexec\n",
	)
	databaseStatement = append(databaseStatement, sql("ALTER DATABASE ", database, " OWNER TO ", role)...)
	return []adapters.Step{
		{Op: adapters.StepSQL, Database: "postgres", Stdin: roleStatement},
		{Op: adapters.StepSQL, Database: "postgres", Stdin: databaseStatement},
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"GRANT ALL PRIVILEGES ON DATABASE ", database, " TO ", role,
		)},
	}
}

func (a *adapter) GrantSteps(p adapters.ProvisionParams) []adapters.Step {
	// p.GrantOn is the OTHER attach's database; the role already exists.
	role := quoteIdentifier(p.Role)
	owner := quoteIdentifier(p.GrantOn)
	privileges := sql("GRANT USAGE ON SCHEMA public TO ", role)
	privileges = append(privileges, sql("GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ", role)...)
	privileges = append(privileges, sql("GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ", role)...)
	privileges = append(privileges, sql("GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO ", role)...)
	privileges = append(privileges, sql(
		"ALTER DEFAULT PRIVILEGES FOR ROLE ", owner,
		" IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO ", role,
	)...)
	privileges = append(privileges, sql(
		"ALTER DEFAULT PRIVILEGES FOR ROLE ", owner,
		" IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO ", role,
	)...)
	return []adapters.Step{
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"GRANT CONNECT ON DATABASE ", owner, " TO ", role,
		)},
		{Op: adapters.StepSQL, Database: p.GrantOn, Stdin: privileges},
	}
}

func (a *adapter) RevokeSteps(p adapters.ProvisionParams) []adapters.Step {
	role := quoteIdentifier(p.Role)
	owner := quoteIdentifier(p.GrantOn)
	privileges := sql(
		"ALTER DEFAULT PRIVILEGES FOR ROLE ", owner,
		" IN SCHEMA public REVOKE ALL PRIVILEGES ON TABLES FROM ", role,
	)
	privileges = append(privileges, sql(
		"ALTER DEFAULT PRIVILEGES FOR ROLE ", owner,
		" IN SCHEMA public REVOKE ALL PRIVILEGES ON SEQUENCES FROM ", role,
	)...)
	privileges = append(privileges, sql("REVOKE ALL PRIVILEGES ON ALL TABLES IN SCHEMA public FROM ", role)...)
	privileges = append(privileges, sql("REVOKE ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public FROM ", role)...)
	privileges = append(privileges, sql("REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public FROM ", role)...)
	privileges = append(privileges, sql("REVOKE USAGE ON SCHEMA public FROM ", role)...)
	return []adapters.Step{
		{Op: adapters.StepSQL, Database: "postgres", Stdin: sql(
			"REVOKE CONNECT ON DATABASE ", owner, " FROM ", role,
		)},
		{Op: adapters.StepSQL, Database: p.GrantOn, Stdin: privileges},
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

func quoteLiteral(value string) string {
	return `'` + strings.ReplaceAll(value, `'`, `''`) + `'`
}

func (a *adapter) BackupStrategy() adapters.BackupStrategy {
	return adapters.BackupStrategy{
		Dump: "pg_dump --format=custom --compress=0 --no-owner --no-acl " +
			"--host=/var/run/postgresql --username=postgres --no-password --role=<role> --dbname=<db>",
		Restore: "pg_restore --clean --if-exists --no-owner --no-acl --exit-on-error --single-transaction " +
			"--host=/var/run/postgresql --username=postgres --no-password --role=<role> --dbname=<db>",
	}
}
