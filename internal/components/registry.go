// Package components is the ONE registry every environment-component kind
// registers into — the third extension seam alongside internal/adapters
// (backing services) and core components. Adding a kind is "an component
// package: config schema, renderer, generated service(s), health/
// reconcile contract + one registration" (architecture.md,
// "Extensibility: adapters and core components"). Like internal/adapters,
// registration is an explicit Register() function called from
// internal/app.NewController — never an init() (standards.md, section 11).
package components

import (
	"fmt"

	"github.com/sample-tenant/groundplane/internal/core"
)

// Renderer produces this component's generated Compose service(s) plus
// whatever config file(s) they need (a Caddyfile, cloudflared config,
// …) from the environment's current desired state. Reuses the shared
// service model and the validate-before-reload render path — an component
// never invents its own apply mechanism (architecture.md, "The
// core-component seam", which this seam mirrors).
type Renderer interface {
	// Render returns the component's generated Compose service fragment(s)
	// (service name -> core.Service) and any config file content keyed
	// by its materialized path.
	Render(env core.Environment, component core.Component) (services map[string]core.Service, files map[string][]byte, err error)
}

// HealthChecker reports whether an enabled component's generated service(s)
// are healthy — "the component's health decides if the reload took;
// unhealthy -> rollback" (architecture.md).
type HealthChecker interface {
	Healthy(env core.Environment, component core.Component) (bool, error)
}

// Component is the contract every environment-component kind implements.
type Component interface {
	Kind() core.ComponentKind
	Label() string // display only
	Renderer
	HealthChecker
	// ConfigSchema names the typed config fields this kind accepts in
	// core.Component.Config / api.Component.Config — TODO: a real schema type
	// once the Console's component-config forms are generated from it,
	// mirroring how internal/adapters' ProvisionParams are typed rather
	// than a bag of strings.
	ConfigSchema() []string
}

var registry = map[core.ComponentKind]Component{}

// Register adds a kind to the registry. Called once, explicitly, from
// internal/app.NewController — see internal/adapters' Register()
// pattern, which this mirrors exactly.
func Register(a Component) {
	if _, exists := registry[a.Kind()]; exists {
		panic(fmt.Sprintf("components: duplicate registration for kind %q", a.Kind()))
	}
	registry[a.Kind()] = a
}

// Get looks up an component implementation by kind (core.Component.Kind).
func Get(kind core.ComponentKind) (Component, bool) {
	a, ok := registry[kind]
	return a, ok
}

// All returns every registered component kind, for the Console's "enable an
// component" picker and the CLI's `component enable --help`.
func All() []Component {
	out := make([]Component, 0, len(registry))
	for _, a := range registry {
		out = append(out, a)
	}
	return out
}
