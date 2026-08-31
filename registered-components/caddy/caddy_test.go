package caddy

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// Rationale: Caddy must preserve deterministic HTTP host/path ordering and
// ordinary reverse-proxy behavior for HTTP and WebSocket targets.
func TestPlanRendersDeterministicHTTPRoutes(t *testing.T) {
	t.Parallel()
	plan, err := Plan(routerInput(), Config{CaddyfileTemplate: "{routes}"})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.Services) != 1 || plan.Services[0].ID != "svc_caddy" || !sameOCIImage(plan.Services[0].Image, Image) ||
		plan.Services[0].Name != routerInput().Origin.ServiceName || len(plan.Files) != 3 {
		t.Fatalf("Plan() = %#v", plan)
	}
	caddyfile := string(plan.Files[0].Content)
	apps := strings.Index(caddyfile, "handle /apps/*")
	app := strings.Index(caddyfile, "handle /app/*")
	root := strings.Index(caddyfile, "handle /*")
	if apps < 0 || app <= apps || root <= app || !strings.Contains(caddyfile, "reverse_proxy websocket:8080") {
		t.Fatalf("rendered Caddyfile = %q", caddyfile)
	}
}

// Rationale: disabled routers remove their managed runtime resources instead
// of retaining a hidden listener or compatibility configuration.
func TestPlanDisabledRouterProducesNoResources(t *testing.T) {
	t.Parallel()
	plan, err := Plan(component.HTTPRouterInput{ComponentID: "cmp_caddy"}, Config{})
	if err != nil || len(plan.Services) != 0 || len(plan.Files) != 0 {
		t.Fatalf("Plan() = %#v, %v", plan, err)
	}
}

// Rationale: generated configuration must reject input that can escape the
// closed host/path and template grammar.
func TestPlanRejectsRouteAndTemplateInjection(t *testing.T) {
	t.Parallel()
	input := routerInput()
	input.Routes[0].Path = "/ok\n}\nrespond 200\n"
	if _, err := Plan(input, Config{}); err == nil {
		t.Fatal("Plan() accepted a Route directive injection")
	}
	input = routerInput()
	input.Origin.URL = "http://another-router:80"
	if _, err := Plan(input, Config{}); err == nil {
		t.Fatal("Plan() accepted an origin outside the managed Caddy Service")
	}
	input = routerInput()
	if _, err := Plan(input, Config{CaddyfileTemplate: "{routes}\n{routes}"}); err == nil {
		t.Fatal("Plan() accepted duplicate template markers")
	}
	input = routerInput()
	if _, err := Plan(input, Config{CaddyfileTemplate: string([]byte{0xff}) + "{routes}"}); err == nil {
		t.Fatal("Plan() accepted an invalid UTF-8 template")
	}
	input = routerInput()
	if _, err := Plan(
		input,
		Config{CaddyfileTemplate: strings.Repeat("x", maxTemplateBytes) + "{routes}"},
	); err == nil {
		t.Fatal("Plan() accepted an oversized template")
	}
}

func sameOCIImage(left, right component.OCIImage) bool {
	if left.Repository != right.Repository || left.IndexDigest != right.IndexDigest ||
		len(left.Platforms) != len(right.Platforms) {
		return false
	}
	for index, platform := range left.Platforms {
		if platform != right.Platforms[index] {
			return false
		}
	}
	return true
}

func routerInput() component.HTTPRouterInput {
	return component.HTTPRouterInput{
		ComponentID: "cmp_caddy", Enabled: true, GeneratedServiceID: "svc_caddy",
		ZoneID: "net_frontend", ZoneName: "frontend", PinnedIPv4: "10.40.0.2",
		Origin: component.HTTPRouterOrigin{ServiceName: ServiceName, URL: OriginURL},
		Routes: []component.HTTPRoute{
			{
				ID: "rte_root", Host: "app.example.com", Path: "/",
				BackendServiceID: "svc_api", BackendServiceName: "api", TargetPort: 8080,
				Exposure: component.HTTPRouteExposurePublic,
			},
			{
				ID: "rte_app", Host: "app.example.com", Path: "/app/*",
				BackendServiceID: "svc_websocket", BackendServiceName: "websocket", TargetPort: 8080,
				Exposure: component.HTTPRouteExposurePublic,
			},
			{
				ID: "rte_apps", Host: "app.example.com", Path: "/apps/*",
				BackendServiceID: "svc_websocket", BackendServiceName: "websocket", TargetPort: 8080,
				Exposure: component.HTTPRouteExposurePublic,
			},
		},
	}
}
