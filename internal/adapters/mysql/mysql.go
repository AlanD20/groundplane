// Package mysql defines the managed MySQL backing-service family.
package mysql

import (
	"strings"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/backingcatalog"
	"github.com/AlanD20/groundplane/internal/core"
)

// Register adds the MySQL family adapter to the registry.
func Register() {
	adapters.Register(&adapter{})
}

type adapter struct{}

func (a *adapter) CredentialIdentityLimit() int { return 32 }

func (a *adapter) Key() string   { return "mysql" }
func (a *adapter) Label() string { return "MySQL" }
func (a *adapter) DefaultImage(version string) (string, error) {
	selected, err := backingcatalog.Resolve(a.Key(), version)
	return selected.Image, err
}
func (a *adapter) FactsPrefix() string               { return "mysql_" }
func (a *adapter) URLScheme() string                 { return "mysql://" }
func (a *adapter) Port() string                      { return "3306" }
func (a *adapter) SupportsAuthenticationModes() bool { return false }
func (a *adapter) FactSchema(core.BackingAuthentication) []adapters.FactDefinition {
	return []adapters.FactDefinition{
		{Field: adapters.FactURL, Secret: true},
		{Field: adapters.FactHost},
		{Field: adapters.FactPort},
		{Field: adapters.FactDatabase},
		{Field: adapters.FactRole},
		{Field: adapters.FactPassword, Secret: true},
	}
}
func (a *adapter) Custom() bool         { return false }
func (a *adapter) SupportsGrants() bool { return true }

func (a *adapter) ProvisionSteps(input adapters.Input) []adapters.Step {
	account := quoteAccount(input.Role)
	database := quoteIdentifier(input.Database)

	// MySQL account and database statements implicitly commit. Keep the account
	// locked until its complete database-scoped grant is durable so interruption
	// cannot publish a usable credential with incomplete access. On retry, the
	// explicit ALTER re-locks an existing account before grants are reapplied.
	createAccount := sql("SET SESSION sql_mode = 'NO_BACKSLASH_ESCAPES'")
	createAccount = append(createAccount, []byte("CREATE USER IF NOT EXISTS "+account+" IDENTIFIED BY '")...)
	createAccount = appendPassword(createAccount, input.Password)
	createAccount = append(createAccount, []byte("' ACCOUNT LOCK;\n")...)
	createAccount = append(createAccount, sql("ALTER USER IF EXISTS ", account, " ACCOUNT LOCK")...)

	grantAndUnlock := sql("GRANT ALL PRIVILEGES ON ", database, ".* TO ", account)
	grantAndUnlock = append(grantAndUnlock, sql("ALTER USER ", account, " ACCOUNT UNLOCK")...)

	return []adapters.Step{
		{Op: adapters.StepMySQLSQL, Database: "mysql", Stdin: sql("CREATE DATABASE IF NOT EXISTS ", database)},
		{Op: adapters.StepMySQLSQL, Database: "mysql", Stdin: createAccount},
		{Op: adapters.StepMySQLSQL, Database: "mysql", Stdin: grantAndUnlock},
	}
}

func (a *adapter) GrantSteps(input adapters.Input) []adapters.Step {
	return []adapters.Step{{
		Op:       adapters.StepMySQLSQL,
		Database: "mysql",
		Stdin: sql(
			"GRANT ALL PRIVILEGES ON ", quoteIdentifier(input.GrantOn), ".* TO ", quoteAccount(input.Role),
		),
	}}
}

func (a *adapter) RevokeSteps(input adapters.Input) []adapters.Step {
	return []adapters.Step{{
		Op:       adapters.StepMySQLSQL,
		Database: "mysql",
		Stdin: sql(
			"REVOKE IF EXISTS ALL PRIVILEGES ON ", quoteIdentifier(input.GrantOn), ".* FROM ",
			quoteAccount(input.Role), " IGNORE UNKNOWN USER",
		),
	}}
}

func (a *adapter) DetachSteps(input adapters.Input) []adapters.Step {
	account := quoteAccount(input.Role)
	return []adapters.Step{
		{Op: adapters.StepMySQLSQL, Database: "mysql", Stdin: sql(
			"ALTER USER IF EXISTS ", account, " ACCOUNT LOCK",
		)},
		{Op: adapters.StepMySQLSQL, Database: "mysql", Stdin: sql(
			"REVOKE IF EXISTS ALL PRIVILEGES, GRANT OPTION FROM ", account, " IGNORE UNKNOWN USER",
		)},
		{Op: adapters.StepMySQLSQL, Database: "mysql", Stdin: sql("DROP USER IF EXISTS ", account)},
		// The database is retained deliberately. Detach removes access, never data.
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

// appendPassword emits a single-quoted value after ProvisionSteps has selected
// NO_BACKSLASH_ESCAPES for the session. Doubling quotes is then independent of
// the server's configured SQL mode and keeps the mutable secret out of argv.
func appendPassword(statement []byte, password []byte) []byte {
	for _, character := range password {
		statement = append(statement, character)
		if character == '\'' {
			statement = append(statement, '\'')
		}
	}
	return statement
}

func quoteAccount(user string) string {
	return quoteLiteral(user) + "@'%'"
}

func quoteIdentifier(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func quoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// MySQL Backup/Restore is integrated through a separate closed execution
// contract; the legacy free-form command strings are intentionally absent.
func (a *adapter) BackupStrategy() adapters.BackupStrategy {
	return adapters.BackupStrategy{}
}
