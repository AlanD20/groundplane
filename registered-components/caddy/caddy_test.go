package caddy

import (
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

// Rationale: Caddy must preserve deterministic HTTP host/path ordering and
// ordinary reverse-proxy behavior for HTTP and WebSocket targets.
func TestPlanRendersDeterministicHTTPRoutes(t *testing.T) {
	t.Parallel()
	plan, err := Plan(routerInput(), Config{CaddyfileTemplate: "{gp.routes}"})
	if err != nil {
		t.Fatalf("Plan() error = %v", err)
	}
	if len(plan.Services) != 1 || plan.Services[0].ID != "svc_caddy" || !sameOCIImage(plan.Services[0].Image, Image) ||
		plan.Services[0].Name != routerInput().Origin.ServiceName || len(plan.Files) != 3 {
		t.Fatalf("Plan() = %#v", plan)
	}
	mount := plan.Services[0].Mounts[0]
	if mount.Source != "components/caddy" || mount.Target != "/etc/caddy" ||
		mount.Kind != component.ManagedMountKindDirectory || !mount.ReadOnly {
		t.Fatal("Caddy must observe atomic configuration replacements through a read-only directory bind")
	}
	if len(plan.Services[0].Networks) != 2 || plan.Services[0].Networks[0].Name != "frontend" ||
		plan.Services[0].Networks[0].StaticIPv4 != "10.40.0.2" ||
		plan.Services[0].Networks[1].Name != "services" || plan.Services[0].Networks[1].StaticIPv4 != "" {
		t.Fatalf("Plan() networks = %#v", plan.Services[0].Networks)
	}
	caddyfile := string(plan.Files[0].Content)
	health := plan.Services[0].Healthcheck
	if health == nil || health.Validate() != nil ||
		!slices.Equal(health.Command, []string{"wget", "-q", "-O", "/dev/null", "http://127.0.0.1:2019/config/"}) {
		t.Fatal("Caddy readiness must observe its running local admin API")
	}
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
	if _, err := Plan(input, Config{CaddyfileTemplate: "{gp.routes}\n{gp.routes}"}); err == nil {
		t.Fatal("Plan() accepted duplicate template markers")
	}
	input = routerInput()
	if _, err := Plan(input, Config{CaddyfileTemplate: string([]byte{0xff}) + "{gp.routes}"}); err == nil {
		t.Fatal("Plan() accepted an invalid UTF-8 template")
	}
	input = routerInput()
	if _, err := Plan(
		input,
		Config{CaddyfileTemplate: strings.Repeat("x", maxTemplateBytes) + "{gp.routes}"},
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
		Zones: []component.HTTPRouterZoneInput{
			{ID: "net_frontend", Name: "frontend", StaticIPv4: "10.40.0.2"},
			{ID: "net_services", Name: "services"},
		},
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
