// Package manual is the "manual" adapter — no auto-provisioning.
// Attaching only joins the network: no database/role, no facts, no
// grants, no Groundplane-managed backups. See mvp.md, "The `manual`
// adapter (locked)". See postgres16's package comment for why Register()
// is an explicit function rather than an init().
package manual

import "github.com/sample-tenant/groundplane/internal/adapters"

// Register adds this adapter to the registry. Called once, explicitly,
// from internal/app.NewController.
func Register() {
	adapters.Register(&adapter{})
}

type adapter struct{}

func (a *adapter) Key() string          { return "manual" }
func (a *adapter) Label() string        { return "Manual (network-only)" }
func (a *adapter) DefaultImage() string { return "" }
func (a *adapter) FactsPrefix() string  { return "" }
func (a *adapter) URLScheme() string    { return "" }
func (a *adapter) Manual() bool         { return true }

func (a *adapter) ProvisionSteps(p adapters.ProvisionParams) []adapters.Step { return nil }
func (a *adapter) GrantSteps(p adapters.ProvisionParams) []adapters.Step     { return nil }
func (a *adapter) DetachSteps(p adapters.ProvisionParams) []adapters.Step    { return nil }
func (a *adapter) BackupStrategy() adapters.BackupStrategy                   { return adapters.BackupStrategy{} }
