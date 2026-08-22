// Package components owns the single compiled-in registry for every
// environment- and platform-owned component kind.
package components

import (
	"fmt"
	"sort"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ApplyStrategy declares which shared pipeline applies a component kind.
// Implementations contribute typed knowledge; they never invent an apply
// mechanism outside these strategies.
type ApplyStrategy string

const (
	EnvironmentRender ApplyStrategy = "environment_render"
	PlatformUpdate    ApplyStrategy = "platform_update"
)

// GeneratedMount is a Controller-managed host path mounted into a generated
// component service. Source is relative to the Environment's authorized root.
type GeneratedMount struct {
	Source   string
	Target   string
	ReadOnly bool
}

// GeneratedSecretEnvironment maps one durable secret Entry to the external
// process's required environment-variable name without placing its value in
// Compose, component config, or rendered desired state.
type GeneratedSecretEnvironment struct {
	Name    string
	EntryID string
}

// GeneratedService keeps deployment-only data out of authored Service intent.
// StaticIPv4 is keyed by Zone name; Mounts reference files/directories owned by
// the component render pipeline.
type GeneratedService struct {
	Service           core.Service
	StaticIPv4        map[string]string
	Mounts            []GeneratedMount
	SecretEnvironment []GeneratedSecretEnvironment
}

// Renderer produces an environment component's generated Compose services and
// materialized configuration files.
type Renderer interface {
	Render(
		env core.Environment,
		component core.Component,
	) (services map[string]GeneratedService, files map[string][]byte, err error)
}

// HealthChecker reports whether an enabled environment component is healthy.
type HealthChecker interface {
	Healthy(env core.Environment, component core.Component) (bool, error)
}

// EnvironmentComponent is the strategy implementation required by every
// EnvironmentRender registration.
type EnvironmentComponent interface {
	Renderer
	HealthChecker
}

// Registration is the complete catalog entry for one component kind. Owner
// support and apply strategy are data, so API validation and the task pipeline
// never switch on concrete kinds.
type Registration struct {
	Kind          core.ComponentKind
	Label         string
	AllowedOwners []core.ComponentOwner
	ApplyStrategy ApplyStrategy
	ConfigSchema  []string
	Environment   EnvironmentComponent
}

// Allows reports whether this kind can be created for owner.
func (r Registration) Allows(owner core.ComponentOwner) bool {
	for _, allowed := range r.AllowedOwners {
		if allowed == owner {
			return true
		}
	}
	return false
}

var registry = map[core.ComponentKind]Registration{}

// Register adds a kind to the registry. It is called explicitly from
// internal/app.NewController; packages never self-register through init.
func Register(registration Registration) {
	validateRegistration(registration)
	if _, exists := registry[registration.Kind]; exists {
		panic(fmt.Sprintf("components: duplicate registration for kind %q", registration.Kind))
	}
	registry[registration.Kind] = clone(registration)
}

// Get looks up a component registration by kind.
func Get(kind core.ComponentKind) (Registration, bool) {
	registration, ok := registry[kind]
	return clone(registration), ok
}

// All returns every registration in deterministic kind order.
func All() []Registration {
	out := make([]Registration, 0, len(registry))
	for _, registration := range registry {
		out = append(out, clone(registration))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Kind < out[j].Kind })
	return out
}

// ValidateOwner is the API-boundary guard for a requested kind and owner.
func ValidateOwner(kind core.ComponentKind, owner core.ComponentOwner) error {
	registration, ok := registry[kind]
	if !ok {
		return errs.Newf(errs.KindValidationFailed, "component: unknown kind %q", kind)
	}
	if !registration.Allows(owner) {
		return errs.Newf(errs.KindValidationFailed, "component: kind %q does not allow owner %q", kind, owner)
	}
	return nil
}

func validateRegistration(registration Registration) {
	if registration.Kind == "" || registration.Label == "" {
		panic("components: registration kind and label are required")
	}
	if len(registration.AllowedOwners) == 0 {
		panic(fmt.Sprintf("components: kind %q has no allowed owner", registration.Kind))
	}
	seenOwners := map[core.ComponentOwner]struct{}{}
	for _, owner := range registration.AllowedOwners {
		if owner != core.ComponentOwnerEnvironment && owner != core.ComponentOwnerPlatform {
			panic(fmt.Sprintf("components: kind %q has invalid owner %q", registration.Kind, owner))
		}
		if _, exists := seenOwners[owner]; exists {
			panic(fmt.Sprintf("components: kind %q repeats owner %q", registration.Kind, owner))
		}
		seenOwners[owner] = struct{}{}
	}
	switch registration.ApplyStrategy {
	case EnvironmentRender:
		if len(registration.AllowedOwners) != 1 || registration.AllowedOwners[0] != core.ComponentOwnerEnvironment {
			panic(
				fmt.Sprintf(
					"components: kind %q uses environment_render without exactly the environment owner",
					registration.Kind,
				),
			)
		}
		if registration.Environment == nil {
			panic(fmt.Sprintf("components: kind %q has no environment implementation", registration.Kind))
		}
	case PlatformUpdate:
		if len(registration.AllowedOwners) != 1 || registration.AllowedOwners[0] != core.ComponentOwnerPlatform {
			panic(
				fmt.Sprintf(
					"components: kind %q uses platform_update without exactly the platform owner",
					registration.Kind,
				),
			)
		}
		if registration.Environment != nil {
			panic(fmt.Sprintf("components: platform kind %q carries an environment implementation", registration.Kind))
		}
	default:
		panic(
			fmt.Sprintf(
				"components: kind %q has invalid apply strategy %q",
				registration.Kind,
				registration.ApplyStrategy,
			),
		)
	}
}

func clone(registration Registration) Registration {
	registration.AllowedOwners = append([]core.ComponentOwner(nil), registration.AllowedOwners...)
	registration.ConfigSchema = append([]string(nil), registration.ConfigSchema...)
	return registration
}
