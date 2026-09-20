// Package valkey9 is the "valkey:9" adapter — cache/queue backing kind.
// See postgres16's package comment for why Register() is an explicit
// function rather than an init().
package valkey9

import (
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/core"
)

// Register adds this adapter to the registry. Called once, explicitly,
// from internal/app.NewController and NewAgent.
func Register() {
	adapters.Register(&adapter{})
}

type adapter struct{}

func (a *adapter) Key() string                       { return "valkey:9" }
func (a *adapter) Label() string                     { return "Valkey 9" }
func (a *adapter) DefaultImage() string              { return "valkey/valkey:9-alpine" }
func (a *adapter) FactsPrefix() string               { return "valkey9_" }
func (a *adapter) URLScheme() string                 { return "redis://" }
func (a *adapter) Port() string                      { return "6379" }
func (a *adapter) SupportsAuthenticationModes() bool { return true }
func (a *adapter) FactSchema(authentication core.BackingAuthentication) []adapters.FactDefinition {
	facts := []adapters.FactDefinition{
		{Field: adapters.FactURL, Secret: authentication != core.BackingAuthenticationNone},
		{Field: adapters.FactHost},
		{Field: adapters.FactPort},
	}
	if authentication == core.BackingAuthenticationNone {
		return facts
	}
	if authentication != core.BackingAuthenticationPassword {
		facts = append(facts, adapters.FactDefinition{Field: adapters.FactRole})
	}
	return append(facts, adapters.FactDefinition{Field: adapters.FactPassword, Secret: true})
}
func (a *adapter) Custom() bool         { return false }
func (a *adapter) SupportsGrants() bool { return false }

func (a *adapter) ProvisionSteps(p adapters.Input) []adapters.Step {
	if p.Authentication == core.BackingAuthenticationNone {
		return nil
	}
	return aclSteps(
		[]string{"ACL", "SETUSER", p.Role, "on", "~*", "&*", "+@all", "-@admin"},
		append([]byte{'>'}, p.Password...),
	)
}

func (a *adapter) GrantSteps(p adapters.Input) []adapters.Step {
	// Valkey has no per-database isolation the way postgres does; a
	// "grant" here is declared-deferred until keyspace-prefix ACLs are
	// designed. Returning no steps keeps the pipeline a no-op rather than
	// a silent partial grant.
	return nil
}

func (a *adapter) RevokeSteps(p adapters.Input) []adapters.Step { return nil }

func (a *adapter) DetachSteps(p adapters.Input) []adapters.Step {
	if p.Authentication == core.BackingAuthenticationNone {
		return nil
	}
	if p.Authentication == core.BackingAuthenticationPassword {
		return aclSteps([]string{"ACL", "SETUSER", "default"}, append([]byte{'<'}, p.Password...))
	}
	return aclSteps([]string{"ACL", "DELUSER", p.Role}, nil)
}

func aclSteps(command []string, secretToken []byte) []adapters.Step {
	args := []string{"--user", "groundplane", "-e"}
	if len(secretToken) != 0 {
		args = append(args, "-x")
	}
	args = append(args, command...)
	return []adapters.Step{
		{Op: adapters.StepExec, Program: "valkey-cli", Args: args, Stdin: secretToken},
		{Op: adapters.StepExec, Program: "valkey-cli", Args: []string{"--user", "groundplane", "-e", "ACL", "SAVE"}},
	}
}

func (a *adapter) BackupStrategy() adapters.BackupStrategy {
	return adapters.BackupStrategy{
		Dump:    "valkey-cli --rdb <dump_path>",
		Restore: "valkey-cli --pipe < <dump_path>", // TODO: real RDB load procedure
	}
}
