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
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/dnsname"
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
		return fmt.Errorf(
			"project %s: kind must be %q or %q",
			p.Slug,
			ProjectKindTenant,
			ProjectKindBacking,
		)
	}
	return nil
}

func (e Environment) Validate() error {
	if e.ID == "" || e.ProjectID == "" || e.Name == "" {
		return fmt.Errorf("environment: id, project_id, and name are required")
	}
	if e.VolumeDir == "" {
		return fmt.Errorf(
			"environment %s: volume_dir is required (must derive from id, never the label)",
			e.Name,
		)
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
					return fmt.Errorf(
						"entry %s: exposure %q must be the only target",
						entry.ID,
						target,
					)
				}
				continue
			}
			if _, ok := e.Services[target]; !ok {
				return fmt.Errorf(
					"entry %s: exposure target %q is not a declared service",
					entry.ID,
					target,
				)
			}
		}
	}

	routeMatches := make(map[string]struct{}, len(e.Routes))
	for _, route := range e.Routes {
		if err := route.Validate(); err != nil {
			return fmt.Errorf("route: %w", err)
		}
		targetFound := false
		for _, service := range e.Services {
			if service.ID == route.TargetServiceID {
				targetFound = true
				break
			}
		}
		if !targetFound {
			return fmt.Errorf(
				"route: target service %q is not declared on this environment",
				route.TargetServiceID,
			)
		}
		match := route.Host + "\x00" + route.Path
		if _, duplicate := routeMatches[match]; duplicate {
			return fmt.Errorf("route: host and path tuple is duplicated")
		}
		routeMatches[match] = struct{}{}
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
		return fmt.Errorf(
			"strategy %q is declared-deferred (see errs.CodeStrategyNotImplemented)",
			s.Strategy,
		)
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
		if m.Mount == "" {
			return fmt.Errorf("mount target is required")
		}
		if (m.Volume == "") == (m.File == "") {
			return fmt.Errorf("mount %q must set exactly one of volume or file", m.Mount)
		}
	}
	for _, e := range s.Environment {
		if err := e.Validate(); err != nil {
			return fmt.Errorf("service-scoped entry: %w", err)
		}
	}
	for name, dependency := range s.DependsOn {
		if name == "" {
			return fmt.Errorf("dependency target is required")
		}
		if err := dependency.Validate(); err != nil {
			return fmt.Errorf("dependency %q: %w", name, err)
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
		if e.Path != "" {
			return fmt.Errorf("entry %s: kind=env must not set path", e.ID)
		}
		if e.UID != nil || e.GID != nil {
			return fmt.Errorf("entry %s: kind=env must not set uid or gid", e.ID)
		}
	case EntryKindFile:
		if e.Path == "" {
			return fmt.Errorf("entry %s: kind=file requires path", e.ID)
		}
		if e.Key != "" {
			return fmt.Errorf("entry %s: kind=file must not set key", e.ID)
		}
		if e.UID == nil || e.GID == nil {
			return fmt.Errorf("entry %s: kind=file requires explicit uid and gid", e.ID)
		}
	default:
		return fmt.Errorf("entry %s: kind must be %q or %q", e.ID, EntryKindEnv, EntryKindFile)
	}

	switch e.Source.Kind {
	case SourceLiteral:
		if e.Source.SecretRef != "" || e.Source.Fact != nil {
			return fmt.Errorf("entry %s: literal source carries another source kind", e.ID)
		}
	case SourceSecretRef:
		if e.Source.SecretRef == "" || e.Source.Literal != "" || e.Source.Fact != nil {
			return fmt.Errorf("entry %s: source.kind=%q requires secret_ref", e.ID, SourceSecretRef)
		}
	case SourceFact:
		if e.Source.Fact == nil || e.Source.Literal != "" || e.Source.SecretRef != "" {
			return fmt.Errorf("entry %s: source.kind=%q requires fact", e.ID, SourceFact)
		}
		if e.Source.Fact.Attach == "" || e.Source.Fact.Key == "" {
			return fmt.Errorf("entry %s: fact source requires attach and key", e.ID)
		}
	default:
		return fmt.Errorf(
			"entry %s: source.kind must be %q, %q, or %q",
			e.ID,
			SourceLiteral,
			SourceSecretRef,
			SourceFact,
		)
	}

	if len(e.Exposure) == 0 {
		return fmt.Errorf(
			"entry %s: exposure must list at least one service, or the single sentinel %q",
			e.ID,
			"all",
		)
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
	if r.TargetServiceID == "" {
		return fmt.Errorf("route: target_service_id is required")
	}
	if r.TargetPort == 0 {
		return fmt.Errorf("route: target_port is required")
	}
	if !validRoutePath(r.Path) {
		return fmt.Errorf("route: path must be a safe absolute ASCII path with only an optional terminal *")
	}
	switch r.Exposure {
	case "public":
		if !dnsname.Valid(r.Host) {
			return fmt.Errorf("route: public exposure requires a lowercase ASCII DNS host")
		}
	case "internal":
		if r.Host != "" && !dnsname.Valid(r.Host) {
			return fmt.Errorf("route: host must be a lowercase ASCII DNS host")
		}
	default:
		return fmt.Errorf(
			"route: exposure must be %q or %q, got %q",
			"public",
			"internal",
			r.Exposure,
		)
	}
	return nil
}

func validRoutePath(path string) bool {
	if path == "" || len(path) > 2048 || path[0] != '/' {
		return false
	}
	for index := 0; index < len(path); index++ {
		character := path[index]
		if character == '*' {
			return index == len(path)-1
		}
		if character == '%' {
			if index+2 >= len(path) || !isHex(path[index+1]) || !isHex(path[index+2]) {
				return false
			}
			index += 2
			continue
		}
		if (character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || strings.ContainsRune("/-._~:@!$&()+,;=", rune(character)) {
			continue
		}
		return false
	}
	return true
}

func isHex(character byte) bool {
	return (character >= '0' && character <= '9') || (character >= 'a' && character <= 'f') ||
		(character >= 'A' && character <= 'F')
}

// ServiceExposesTCPPort reports whether a Compose-style expose declaration
// makes target reachable over TCP. Route persistence and Caddy rendering use
// the same parser so an accepted Route cannot become unreachable by semantic
// drift between the two boundaries.
func ServiceExposesTCPPort(exposures []string, target uint16) bool {
	for _, exposure := range exposures {
		value := strings.TrimSpace(exposure)
		if _, udp := strings.CutSuffix(value, "/udp"); udp {
			continue
		}
		value, _ = strings.CutSuffix(value, "/tcp")
		if colon := strings.LastIndexByte(value, ':'); colon >= 0 {
			value = value[colon+1:]
		}
		startText, endText, ranged := strings.Cut(value, "-")
		start, err := strconv.ParseUint(startText, 10, 16)
		if err != nil {
			continue
		}
		end := start
		if ranged {
			end, err = strconv.ParseUint(endText, 10, 16)
			if err != nil || end < start {
				continue
			}
		}
		if uint64(target) >= start && uint64(target) <= end {
			return true
		}
	}
	return false
}

func (a Attach) Validate() error {
	if a.ID == "" || a.Name == "" || a.BackingProjectID == "" {
		return fmt.Errorf("attach: id, name, and backing_project_id are required")
	}
	if a.Service == "" || a.CredentialAttachID == "" {
		return fmt.Errorf("attach %s: service and credential_attach_id are required", a.Name)
	}
	switch a.Status {
	case AttachPending,
		AttachProvisioning,
		AttachReady,
		AttachFailed,
		AttachDetaching,
		AttachDetached:
		// ok
	default:
		return fmt.Errorf("attach %s: unknown status %q", a.Name, a.Status)
	}
	return nil
}

func (c Component) Validate() error {
	if c.ID == "" || c.Kind == "" {
		return fmt.Errorf("component: id and kind are required")
	}
	switch c.Owner {
	case ComponentOwnerEnvironment:
		if c.OwnerID == "" {
			return fmt.Errorf("component %s: environment owner requires owner_id", c.ID)
		}
	case ComponentOwnerPlatform:
		if c.OwnerID != "" {
			return fmt.Errorf("component %s: platform owner must not set owner_id", c.ID)
		}
	default:
		return fmt.Errorf("component %s: unknown owner %q", c.ID, c.Owner)
	}
	branches := 0
	if c.Config.Caddy != nil { branches++ }
	if c.Config.CloudflareTunnel != nil { branches++ }
	if c.Config.CoreDNS != nil { branches++ }
	if branches > 1 {
		return fmt.Errorf("component %s: desired config has multiple variants", c.ID)
	}
	switch c.Kind {
	case ComponentKindIngressCaddy:
		if c.Owner != ComponentOwnerEnvironment || c.Config.CloudflareTunnel != nil || c.Config.CoreDNS != nil ||
			(c.Enabled && (c.Config.Caddy == nil || c.Config.Caddy.ZoneID == "")) {
			return fmt.Errorf("component %s: Caddy ownership or config is invalid", c.ID)
		}
	case ComponentKindEdgeCloudflare:
		if c.Owner != ComponentOwnerEnvironment || c.Config.Caddy != nil || c.Config.CoreDNS != nil ||
			(c.Enabled && (c.Config.CloudflareTunnel == nil || c.Config.CloudflareTunnel.SecretID == "")) {
			return fmt.Errorf("component %s: Cloudflare Tunnel ownership or config is invalid", c.ID)
		}
	case ComponentKindCoreDNS:
		if c.Owner != ComponentOwnerPlatform || c.Config.Caddy != nil || c.Config.CloudflareTunnel != nil ||
			(c.Enabled && c.Config.CoreDNS == nil) {
			return fmt.Errorf("component %s: CoreDNS ownership or config is invalid", c.ID)
		}
	default:
		return fmt.Errorf("component %s: unknown kind %q", c.ID, c.Kind)
	}
	return nil
}

func (g ReleaseGroupSpec) Validate(name string) error {
	if g.orderPresent && len(g.Order) == 0 {
		return fmt.Errorf("release group %s: explicit order must not be empty or null", name)
	}
	return validateReleaseGroupShape(name, g.Services, g.Order, g.OnFailure)
}

func (g ReleaseGroup) Validate() error {
	if g.ID == "" {
		return fmt.Errorf("release group: id is required")
	}
	return validateReleaseGroupShape(g.Name, g.Services, g.Order, g.OnFailure)
}

func validateReleaseGroupShape(
	name string,
	services []string,
	order []string,
	onFailure OnFailure,
) error {
	if !utf8.ValidString(name) {
		return fmt.Errorf("release group: map key must be valid UTF-8")
	}
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("release group: map key is required")
	}
	if strings.IndexFunc(name, unicode.IsControl) >= 0 {
		return fmt.Errorf("release group %q: map key must not contain control characters", name)
	}
	if len(services) < 2 || len(services) > 32 {
		return fmt.Errorf("release group %s: between 2 and 32 services are required", name)
	}

	members := make(map[string]struct{}, len(services))
	for _, service := range services {
		if strings.TrimSpace(service) == "" {
			return fmt.Errorf("release group %s: services must not contain blank entries", name)
		}
		if _, exists := members[service]; exists {
			return fmt.Errorf("release group %s: service %q is duplicated", name, service)
		}
		members[service] = struct{}{}
	}

	if len(order) > 0 {
		if len(order) != len(services) {
			return fmt.Errorf(
				"release group %s: order must contain every service exactly once",
				name,
			)
		}
		ordered := make(map[string]struct{}, len(order))
		for _, service := range order {
			if strings.TrimSpace(service) == "" {
				return fmt.Errorf("release group %s: order must not contain blank entries", name)
			}
			if _, exists := ordered[service]; exists {
				return fmt.Errorf("release group %s: order service %q is duplicated", name, service)
			}
			if _, exists := members[service]; !exists {
				return fmt.Errorf(
					"release group %s: order service %q is not a member",
					name,
					service,
				)
			}
			ordered[service] = struct{}{}
		}
	}

	switch onFailure {
	case "", OnFailureSwitchBack, OnFailureLeaveActive:
		// Empty resolves to switch_back through OnFailure.WithDefault.
	default:
		return fmt.Errorf("release group %s: unknown on_failure %q", name, onFailure)
	}
	return nil
}
