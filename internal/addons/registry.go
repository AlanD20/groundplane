// Package addons is the ONE registry every environment-addon kind
// registers into — the third extension seam alongside internal/adapters
// (backing services) and core components. Adding a kind is "an addon
// package: config schema, renderer, generated service(s), health/
// reconcile contract + one registration" (architecture.md,
// "Extensibility: adapters and core components"). Like internal/adapters,
// registration is an explicit Register() function called from
// internal/app.NewController — never an init() (standards.md, section 11).
package addons

import (
	"fmt"

	"github.com/sample-tenant/groundplane/internal/core"
)

// Renderer produces this addon's generated Compose service(s) plus
// whatever config file(s) they need (a Caddyfile, cloudflared config,
// …) from the environment's current desired state. Reuses the shared
// service model and the validate-before-reload render path — an addon
// never invents its own apply mechanism (architecture.md, "The
// core-component seam", which this seam mirrors).
type Renderer interface {
	// Render returns the addon's generated Compose service fragment(s)
	// (service name -> core.Service) and any config file content keyed
	// by its materialized path.
	Render(env core.Environment, addon core.Addon) (services map[string]core.Service, files map[string][]byte, err error)
}

// HealthChecker reports whether an enabled addon's generated service(s)
// are healthy — "the component's health decides if the reload took;
// unhealthy -> rollback" (architecture.md).
type HealthChecker interface {
	Healthy(env core.Environment, addon core.Addon) (bool, error)
}

// Addon is the contract every environment-addon kind implements.
type Addon interface {
	Kind() core.AddonKind
	Label() string // display only
	Renderer
	HealthChecker
	// ConfigSchema names the typed config fields this kind accepts in
	// core.Addon.Config / api.Addon.Config — TODO: a real schema type
	// once the Console's addon-config forms are generated from it,
	// mirroring how internal/adapters' ProvisionParams are typed rather
	// than a bag of strings.
	ConfigSchema() []string
}

var registry = map[core.AddonKind]Addon{}

// Register adds a kind to the registry. Called once, explicitly, from
// internal/app.NewController — see internal/adapters' Register()
// pattern, which this mirrors exactly.
func Register(a Addon) {
	if _, exists := registry[a.Kind()]; exists {
		panic(fmt.Sprintf("addons: duplicate registration for kind %q", a.Kind()))
	}
	registry[a.Kind()] = a
}

// Get looks up an addon implementation by kind (core.Addon.Kind).
func Get(kind core.AddonKind) (Addon, bool) {
	a, ok := registry[kind]
	return a, ok
}

// All returns every registered addon kind, for the Console's "enable an
// addon" picker and the CLI's `addon enable --help`.
func All() []Addon {
	out := make([]Addon, 0, len(registry))
	for _, a := range registry {
		out = append(out, a)
	}
	return out
}
