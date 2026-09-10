package caddy

import (
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// Rationale: the full file must preserve native policy while Route references
// resolve to immutable matches and stable, slot-independent Service upstreams.
func TestPlanRendersFullTemplateFromDeclaredRoutes(t *testing.T) {
	t.Parallel()
	input := routerInput()
	original := append([]component.HTTPRoute(nil), input.Routes...)
	template := `{
	admin localhost:2019
}
http://{gp.route:app.example.com:/:host} {
	header X-Requested-Host {host}
	route {
		respond /internal/* 404
		reverse_proxy {gp.route:app.example.com:/apps/*:path} {gp.route:app.example.com:/apps/*:upstream}
		reverse_proxy {gp.route:app.example.com:/app/*:path} {gp.route:app.example.com:/app/*:upstream}
		reverse_proxy {gp.route:app.example.com:/:path} {gp.route:app.example.com:/:upstream}
	}
}
`
	want := `{
	admin localhost:2019
}
http://app.example.com {
	header X-Requested-Host {host}
	route {
		respond /internal/* 404
		reverse_proxy /apps/* websocket:8080
		reverse_proxy /app/* websocket:8080
		reverse_proxy /* api:8080
	}
}
`
	plan, err := Plan(input, Config{CaddyfileTemplate: template})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if got := string(plan.Files[0].Content); got != want {
		t.Fatalf("rendered = %q, want %q", got, want)
	}
	if !slices.Equal(input.Routes, original) || plan.Services[0].Networks[0].StaticIPv4 != "10.40.0.2" {
		t.Fatal("template rendering changed its input or primary router address")
	}
}

// Rationale: a missing or mistyped Route must not silently create a second
// upstream/hostname authority or omit a declared Route from the template.
func TestFullTemplateRejectsUnresolvedAndUnaccountedRoutes(t *testing.T) {
	t.Parallel()
	for name, body := range map[string]string{
		"legacy aggregate":     "{routes}",
		"duplicate aggregate":  "{gp.routes}\n{gp.routes}",
		"unknown namespace":    "{gp.routes}\n{gp.arbitrary}",
		"unterminated token":   "{gp.routes}\n{gp.route:app.example.com:/:host",
		"missing selector":     "{gp.route:upstream}",
		"unknown host":         "{gp.routes}\n{gp.route:missing.example.com:/:host}",
		"unknown path":         "{gp.routes}\n{gp.route:app.example.com:/missing:upstream}",
		"unknown field":        "{gp.routes}\n{gp.route:app.example.com:/:slot}",
		"unaccounted routes":   "http://app.example.com { respond 404 }",
		"host is not coverage": "{gp.route:app.example.com:/:host}",
		"other paths unserved": "{gp.route:app.example.com:/:upstream}",
		"nul":                  "{gp.routes}\x00",
		"invalid utf8":         "{gp.routes}\xff",
		"oversized":            strings.Repeat("x", maxTemplateBytes+1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Plan(routerInput(), Config{CaddyfileTemplate: body}); err == nil {
				t.Fatal("Plan() accepted an invalid full template")
			}
		})
	}
}

// Rationale: template references use portable immutable matches, including an
// internal catch-all and legal path colons, rather than generated Route ids.
func TestFullTemplateSupportsInternalHostAndColonPath(t *testing.T) {
	t.Parallel()
	input := routerInput()
	input.Routes = []component.HTTPRoute{{
		ID: "rte_internal", Path: "/events:subscribe", BackendServiceID: "svc_api",
		BackendServiceName: "api", TargetPort: 9000, Exposure: component.HTTPRouteExposureInternal,
	}}
	body := "http://{gp.route::/events:subscribe:host} {\n\treverse_proxy " +
		"{gp.route::/events:subscribe:path} {gp.route::/events:subscribe:upstream}\n}\n"
	plan, err := Plan(input, Config{CaddyfileTemplate: body})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(plan.Files[0].Content); got != "http:// {\n\treverse_proxy /events:subscribe api:9000\n}\n" {
		t.Fatalf("rendered = %q", got)
	}
}

// Rationale: the default remains deterministic all-Route rendering, whereas a
// complete native deny-only file is valid when there are no Routes to omit.
func TestFullTemplateAggregateAndEmptyRouteSet(t *testing.T) {
	t.Parallel()
	defaultPlan, err := Plan(routerInput(), Config{})
	if err != nil {
		t.Fatal(err)
	}
	aggregate, err := Plan(routerInput(), Config{CaddyfileTemplate: "{gp.routes}"})
	if err != nil || string(aggregate.Files[0].Content) != string(defaultPlan.Files[0].Content) {
		t.Fatalf("aggregate differs from default: %v", err)
	}
	input := routerInput()
	input.Routes = nil
	plan, err := Plan(input, Config{CaddyfileTemplate: "http:// {\n\trespond 404\n}"})
	if err != nil {
		t.Fatal(err)
	}
	if got := string(plan.Files[0].Content); got != "http:// {\n\trespond 404\n}\n" {
		t.Fatalf("empty Route native policy = %q", got)
	}
}
