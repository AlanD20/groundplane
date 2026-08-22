// Package valkey9 is the "valkey:9" adapter — cache/queue backing kind.
// See postgres16's package comment for why Register() is an explicit
// function rather than an init().
package valkey9

import "github.com/AlanD20/groundplane/internal/adapters"

// Register adds this adapter to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	adapters.Register(&adapter{})
}

type adapter struct{}

func (a *adapter) Key() string          { return "valkey:9" }
func (a *adapter) Label() string        { return "Valkey 9" }
func (a *adapter) DefaultImage() string { return "valkey/valkey:9-alpine" }
func (a *adapter) FactsPrefix() string  { return "valkey9_" }
func (a *adapter) URLScheme() string    { return "redis://" }
func (a *adapter) FactSchema() []adapters.FactDefinition {
	return []adapters.FactDefinition{
		{Field: adapters.FactURL, Secret: true},
		{Field: adapters.FactHost},
		{Field: adapters.FactPort},
		{Field: adapters.FactPassword, Secret: true},
	}
}
func (a *adapter) Manual() bool { return false }

func (a *adapter) ProvisionSteps(p adapters.ProvisionParams) []adapters.Step {
	return []adapters.Step{
		{Op: adapters.StepExec, Params: map[string]string{
			"cmd": "valkey-cli ACL SETUSER <role> on >'<generated>' ~* &* +@all",
		}},
	}
}

func (a *adapter) GrantSteps(p adapters.ProvisionParams) []adapters.Step {
	// Valkey has no per-database isolation the way postgres does; a
	// "grant" here is declared-deferred until keyspace-prefix ACLs are
	// designed. Returning no steps keeps the pipeline a no-op rather than
	// a silent partial grant.
	return nil
}

func (a *adapter) DetachSteps(p adapters.ProvisionParams) []adapters.Step {
	return []adapters.Step{
		{Op: adapters.StepExec, Params: map[string]string{
			"cmd": "valkey-cli ACL DELUSER <role>",
		}},
	}
}

func (a *adapter) BackupStrategy() adapters.BackupStrategy {
	return adapters.BackupStrategy{
		Dump:    "valkey-cli --rdb <dump_path>",
		Restore: "valkey-cli --pipe < <dump_path>", // TODO: real RDB load procedure
	}
}
