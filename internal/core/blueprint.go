// Package core (continued): the Blueprint contract — desired state as a
// set of schema-validated documents (see envelope.go for the authored
// Compose+x-gp-* shape). Validate() here is the type-level contract
// (required fields, referential shape) for the TYPED internal model
// (model.go); it does not re-parse YAML. Full validation — including the
// cases in blueprint.md's "Required validation" that need external state
// (a running backing service, an existing secret, a valid Compose
// config) — happens in internal/controller at write time, which calls
// these Validate() methods as its first, cheapest check before anything
// touching etcd or the Agent.
package core

import (
	"fmt"
)

// Document is implemented by every top-level Blueprint document.
type Document interface {
	Validate() error
}

func (t Tenant) Validate() error {
	if t.ID == "" || t.Slug == "" || t.Name == "" {
		return fmt.Errorf("tenant: id, slug, and name are required")
	}
	return nil
}

func (p Project) Validate() error {
	if p.ID == "" || p.Slug == "" || p.Name == "" {
		return fmt.Errorf("project: id, slug, and name are required")
	}
	switch p.Kind {
	case ProjectKindTenant:
		if p.TenantID == "" {
			return fmt.Errorf("project %s: tenant projects require tenant_id", p.Slug)
		}
	case ProjectKindBacking:
		if p.TenantID != "" {
			return fmt.Errorf("project %s: backing projects must not have a tenant_id", p.Slug)
		}
	default:
		return fmt.Errorf("project %s: kind must be %q or %q", p.Slug, ProjectKindTenant, ProjectKindBacking)
	}
	return nil
}

func (e Environment) Validate() error {
	if e.ID == "" || e.ProjectID == "" || e.Slug == "" {
		return fmt.Errorf("environment: id, project_id, and slug are required")
	}
	if e.VolumeDir == "" {
		return fmt.Errorf("environment %s: volume_dir is required (must derive from id, never the label)", e.Slug)
	}

	// A service on a zone must reference a zone that exists — see
	// blueprint.md's required-validation case "a service or network
	// reference is invalid".
	for name, svc := range e.Services {
		for _, z := range svc.Zones {
			if _, ok := e.Zones[z]; !ok {
				return fmt.Errorf("service %s: zone %q not declared on this environment", name, z)
			}
		}
		if err := svc.Validate(); err != nil {
			return fmt.Errorf("service %s: %w", name, err)
		}
	}

	for _, entry := range e.Entries {
		if err := entry.Validate(); err != nil {
			return fmt.Errorf("entry: %w", err)
		}
		for _, target := range entry.Exposure {
			if target == "all" {
				if len(entry.Exposure) != 1 {
					return fmt.Errorf("entry %s: exposure %q must be the only target", entry.ID, target)
				}
				continue
			}
			if _, ok := e.Services[target]; !ok {
				return fmt.Errorf("entry %s: exposure target %q is not a declared service", entry.ID, target)
			}
		}
	}

	for _, route := range e.Routes {
		if err := route.Validate(); err != nil {
			return fmt.Errorf("route: %w", err)
		}
		if _, ok := e.Services[route.ServiceName]; !ok {
			return fmt.Errorf("route: target service %q is not declared on this environment", route.ServiceName)
		}
	}

	// Public routes with no enabled ingress component is a WARNING in the
	// Console (mvp.md), not a hard validation failure here — checked by
	// the renderer, which has the component state to compare against.

	return nil
}

func (s Service) Validate() error {
	if s.ID == "" || s.Name == "" || s.Image == "" {
		return fmt.Errorf("id, name, and image are required")
	}
	switch s.Strategy {
	case "", StrategyBlueGreen, StrategyRecreate:
		// ok
	case StrategyRolling:
		// blueprint.md's required-validation case: "a requested release
		// strategy is deferred (rolling in the MVP)".
		return fmt.Errorf("strategy %q is declared-deferred (see errs.CodeStrategyNotImplemented)", s.Strategy)
	default:
		return fmt.Errorf("unknown strategy %q", s.Strategy)
	}
	switch s.OnFailure {
	case "", OnFailureSwitchBack, OnFailureLeaveActive:
		// ok
	default:
		return fmt.Errorf("unknown on_failure %q", s.OnFailure)
	}
	hc := s.Healthcheck
	set := 0
	if hc.HTTP != "" {
		set++
	}
	if hc.TCP != "" {
		set++
	}
	if hc.Pgrep != "" {
		set++
	}
	if set > 1 {
		return fmt.Errorf("healthcheck must set exactly one of http/tcp/pgrep, got %d", set)
	}
	for _, m := range s.Mounts {
		if (m.Volume == "") == (m.File == "") {
			return fmt.Errorf("mount %q must set exactly one of volume or file", m.Mount)
		}
	}
	for _, e := range s.Environment {
		if err := e.Validate(); err != nil {
			return fmt.Errorf("service-scoped entry: %w", err)
		}
	}
	return nil
}

func (c Connector) Validate() error {
	if c.ID == "" || c.Kind == "" {
		return fmt.Errorf("connector: id and kind are required")
	}
	if c.EnvironmentID == "" {
		return fmt.Errorf("connector: environment_id is required")
	}
	return nil
}

// Validate enforces the unified entry model's shape: Kind selects which
// of Key/Path applies, Source is exactly one of literal/secret_ref/fact,
// Exposure is non-empty, and a secret entry must never carry a plaintext
// literal value (mvp.md's original invariant, now generalized to the
// discriminated source model — see blueprint.md, "x-gp-entry").
func (e EnvEntry) Validate() error {
	if e.ID == "" {
		return fmt.Errorf("entry: id is required")
	}
	switch e.Kind {
	case EntryKindEnv:
		if e.Key == "" {
			return fmt.Errorf("entry %s: kind=env requires key", e.ID)
		}
	case EntryKindFile:
		if e.Path == "" {
			return fmt.Errorf("entry %s: kind=file requires path", e.ID)
		}
	default:
		return fmt.Errorf("entry %s: kind must be %q or %q", e.ID, EntryKindEnv, EntryKindFile)
	}

	set := 0
	if e.Source.Literal != "" {
		set++
	}
	if e.Source.SecretRef != "" {
		set++
	}
	if e.Source.Fact != nil {
		set++
	}
	if set != 1 {
		return fmt.Errorf("entry %s: source must set exactly one of literal, secret_ref, or fact (got %d)", e.ID, set)
	}
	switch e.Source.Kind {
	case SourceLiteral:
		if e.Source.Literal == "" {
			return fmt.Errorf("entry %s: source.kind=%q requires literal", e.ID, SourceLiteral)
		}
	case SourceSecretRef:
		if e.Source.SecretRef == "" {
			return fmt.Errorf("entry %s: source.kind=%q requires secret_ref", e.ID, SourceSecretRef)
		}
	case SourceFact:
		if e.Source.Fact == nil {
			return fmt.Errorf("entry %s: source.kind=%q requires fact", e.ID, SourceFact)
		}
	default:
		return fmt.Errorf("entry %s: source.kind must be %q, %q, or %q", e.ID, SourceLiteral, SourceSecretRef, SourceFact)
	}

	if len(e.Exposure) == 0 {
		return fmt.Errorf("entry %s: exposure must list at least one service, or the single sentinel %q", e.ID, "all")
	}
	if e.Secret && e.Source.Kind == SourceLiteral && e.Source.Literal != "" {
		return fmt.Errorf("entry %s: secret entries must not carry a plaintext literal value", e.ID)
	}
	return nil
}

// Validate enforces blueprint.md's required-validation case: "a route
// targets a missing service or uses a hostname as a path, or vice
// versa" — the missing-service-reference half needs the environment's
// service set and is checked in Environment.Validate's caller
// (internal/controller); this method checks the shape it can see alone.
func (r Route) Validate() error {
	if r.ServiceName == "" {
		return fmt.Errorf("route: service is required")
	}
	if r.Host == "" && r.Path == "" {
		return fmt.Errorf("route: at least one of host or path is required")
	}
	switch r.Exposure {
	case "public", "internal":
		// ok
	default:
		return fmt.Errorf("route: exposure must be %q or %q, got %q", "public", "internal", r.Exposure)
	}
	return nil
}

func (a Attach) Validate() error {
	if a.ID == "" || a.Name == "" || a.BackingProjectID == "" {
		return fmt.Errorf("attach: id, name, and backing_project_id are required")
	}
	if len(a.Services) == 0 {
		return fmt.Errorf("attach %s: at least one service is required", a.Name)
	}
	switch a.Status {
	case AttachPending, AttachProvisioning, AttachReady, AttachFailed, AttachDetaching, AttachDetached:
		// ok
	default:
		return fmt.Errorf("attach %s: unknown status %q", a.Name, a.Status)
	}
	return nil
}

func (a Component) Validate() error {
	if a.ID == "" || a.EnvironmentID == "" || a.Kind == "" {
		return fmt.Errorf("component: id, environment_id, and kind are required")
	}
	return nil
}

func (g ReleaseGroup) Validate() error {
	if g.ID == "" || g.Name == "" {
		return fmt.Errorf("release group: id and name are required")
	}
	if len(g.Services) < 2 {
		// A "group" of one service is just a Deploy — coordination is
		// explicit, never inferred (blueprint.md, "x-gp-release-group"),
		// but a single-service group is a modeling error, not a feature.
		return fmt.Errorf("release group %s: at least two services are required", g.Name)
	}
	return nil
}
