// Package custom attaches consumers to a shared Service without built-in
// provisioning or database backups. Optional operator hooks are handled by
// the hook pipeline, not this adapter.
package custom

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

func (a *adapter) CredentialIdentityLimit() int { return 0 }

func (a *adapter) Key() string                                                     { return "custom" }
func (a *adapter) Label() string                                                   { return "Custom" }
func (a *adapter) DefaultImage(string) (string, error)                             { return "", nil }
func (a *adapter) FactsPrefix() string                                             { return "" }
func (a *adapter) URLScheme() string                                               { return "" }
func (a *adapter) Port() string                                                    { return "" }
func (a *adapter) FactSchema(core.BackingAuthentication) []adapters.FactDefinition { return nil }
func (a *adapter) SupportsAuthenticationModes() bool                               { return false }
func (a *adapter) Custom() bool                                                    { return true }
func (a *adapter) SupportsGrants() bool                                            { return false }

func (a *adapter) ProvisionSteps(p adapters.Input) []adapters.Step { return nil }
func (a *adapter) GrantSteps(p adapters.Input) []adapters.Step     { return nil }
func (a *adapter) RevokeSteps(p adapters.Input) []adapters.Step    { return nil }
func (a *adapter) DetachSteps(p adapters.Input) []adapters.Step    { return nil }
func (a *adapter) BackupStrategy() adapters.BackupStrategy         { return adapters.BackupStrategy{} }
