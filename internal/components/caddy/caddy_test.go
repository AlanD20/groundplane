package caddy

import (
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestRenderBuildsDeterministicReachableRoutes(t *testing.T) {
	// Rationale: Route order, stable aliases, ports, and ordinary reverse_proxy
	// directives are the traffic contract shared by HTTP and WebSocket targets.
	t.Parallel()
	environment, adapter := renderFixture()

	services, files, err := (&component{}).Render(environment, adapter)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	generated, ok := services[caddyServiceName]
	if !ok || generated.Service.ID != adapter.GeneratedServices[0] || generated.Service.Image != caddyImage ||
		generated.StaticIPv4["frontend"] != "10.40.0.2" || len(generated.Mounts) != 3 {
		t.Fatalf("Render() service = %#v", generated)
	}
	caddyfile := string(files[caddyfileName])
	apps := strings.Index(caddyfile, "handle /apps/*")
	app := strings.Index(caddyfile, "handle /app/*")
	root := strings.Index(caddyfile, "handle /*")
	if apps < 0 || app <= apps || root <= app {
		t.Fatalf("Render() route order = %q", caddyfile)
	}
	if !strings.Contains(caddyfile, "reverse_proxy websocket:8080") ||
		!strings.Contains(caddyfile, "http://app.example.com, https://app.example.com") {
		t.Fatalf("Render() Caddyfile = %q", caddyfile)
	}
}

func TestRenderUsesStableRouteIDAfterEqualPathLength(t *testing.T) {
	// Rationale: equal-length path blocks must remain byte-stable by Route id,
	// independent of input order.
	t.Parallel()
	environment, adapter := renderFixture()
	environment.Routes = []core.Route{
		{
			ID:              "rte_b",
			Host:            "app.example.com",
			Path:            "/foo*",
			TargetServiceID: "svc_api",
			TargetPort:      8080,
			Exposure:        "public",
		},
		{
			ID:              "rte_a",
			Host:            "app.example.com",
			Path:            "/bar*",
			TargetServiceID: "svc_websocket",
			TargetPort:      8080,
			Exposure:        "public",
		},
	}

	_, files, err := (&component{}).Render(environment, adapter)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	caddyfile := string(files[caddyfileName])
	if strings.Index(caddyfile, "handle /bar*") >= strings.Index(caddyfile, "handle /foo*") {
		t.Fatalf("Render() equal-length order = %q", caddyfile)
	}
}

func TestRenderAppliesInternalCAOnlyToExactInternalHosts(t *testing.T) {
	// Rationale: internal named routes use the Environment CA while hostless
	// routes remain HTTP catch-alls that do not invent a TLS identity.
	t.Parallel()
	environment, adapter := renderFixture()
	environment.Routes = []core.Route{
		{
			ID:              "rte_internal",
			Host:            "admin.internal",
			Path:            "/",
			TargetServiceID: "svc_api",
			TargetPort:      8080,
			Exposure:        "internal",
		},
		{ID: "rte_hostless", Path: "/jobs/*", TargetServiceID: "svc_api", TargetPort: 8080, Exposure: "internal"},
	}

	_, files, err := (&component{}).Render(environment, adapter)
	if err != nil {
		t.Fatalf("Render() error = %v", err)
	}
	caddyfile := string(files[caddyfileName])
	if strings.Count(caddyfile, "tls internal") != 1 || !strings.Contains(caddyfile, "http://admin.internal") ||
		!strings.Contains(caddyfile, "http:// {") {
		t.Fatalf("Render() internal routes = %q", caddyfile)
	}
}

func TestRenderRejectsUnreachableTargetsAndInvalidTemplates(t *testing.T) {
	// Rationale: an enabled router must fail closed instead of silently joining
	// target networks, publishing ports, or accepting superseded placeholders.
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*core.Environment, *core.Component)
	}{
		{
			name: "target outside Zone",
			mutate: func(environment *core.Environment, _ *core.Component) {
				service := environment.Services["api"]
				service.Zones = []string{"backend"}
				environment.Services["api"] = service
			},
		},
		{
			name: "port not exposed",
			mutate: func(environment *core.Environment, _ *core.Component) {
				service := environment.Services["api"]
				service.Expose = []string{"9000"}
				environment.Services["api"] = service
			},
		},
		{
			name: "gateway address",
			mutate: func(_ *core.Environment, component *core.Component) {
				component.PinnedIPv4 = "10.40.0.1"
			},
		},
		{
			name: "legacy template",
			mutate: func(_ *core.Environment, component *core.Component) {
				component.Config["caddyfile_template"] = "{routes}\n# {slot}"
			},
		},
		{
			name: "duplicate marker",
			mutate: func(_ *core.Environment, component *core.Component) {
				component.Config["caddyfile_template"] = "{routes}\n{routes}"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			environment, adapter := renderFixture()
			test.mutate(&environment, &adapter)
			_, _, err := (&component{}).Render(environment, adapter)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Render() error = %v, want %q", err, errs.CodeValidationFailed)
			}
		})
	}
}

func TestRenderRejectsRouteSyntaxInjectionAndMixedHostExposure(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*core.Environment)
	}{
		{
			name: "path directive injection",
			mutate: func(environment *core.Environment) {
				environment.Routes[0].Path = "/ok\n}\nrespond 200\n"
			},
		},
		{
			name: "mixed public and internal host",
			mutate: func(environment *core.Environment) {
				environment.Routes[1].Exposure = "internal"
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			environment, adapter := renderFixture()
			test.mutate(&environment)
			_, files, err := (&component{}).Render(environment, adapter)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("Render() error = %v, want %q", err, errs.CodeValidationFailed)
			}
			if caddyfile := string(files[caddyfileName]); strings.Contains(caddyfile, "respond 200") {
				t.Fatalf("Render() emitted injected directive: %q", caddyfile)
			}
		})
	}
}

func TestRenderDisabledComponentProducesNoArtifacts(t *testing.T) {
	// Rationale: disabling Caddy removes its generated service and files rather
	// than retaining a compatibility listener.
	t.Parallel()
	environment, adapter := renderFixture()
	adapter.Enabled = false
	adapter.PinnedIPv4 = ""
	adapter.Config = nil

	services, files, err := (&component{}).Render(environment, adapter)
	if err != nil || len(services) != 0 || len(files) != 0 {
		t.Fatalf("Render() = %#v, %#v, %v", services, files, err)
	}
}

func renderFixture() (core.Environment, core.Component) {
	environment := core.Environment{
		ID: "env_test",
		Zones: map[string]core.Zone{
			"frontend": {ID: "net_frontend", Name: "frontend", Subnet: "10.40.0.0/24"},
		},
		Services: map[string]core.Service{
			"api": {
				ID: "svc_api", Name: "api", Image: "app:1", Zones: []string{"frontend"}, Expose: []string{"8080"},
			},
			"websocket": {
				ID: "svc_websocket", Name: "websocket", Image: "app:1", Zones: []string{"frontend"},
				Expose: []string{"8080/tcp"},
			},
		},
		Routes: []core.Route{
			{
				ID:              "rte_root",
				Host:            "app.example.com",
				Path:            "/",
				TargetServiceID: "svc_api",
				TargetPort:      8080,
				Exposure:        "public",
			},
			{
				ID:              "rte_app",
				Host:            "app.example.com",
				Path:            "/app/*",
				TargetServiceID: "svc_websocket",
				TargetPort:      8080,
				Exposure:        "public",
			},
			{
				ID:              "rte_apps",
				Host:            "app.example.com",
				Path:            "/apps/*",
				TargetServiceID: "svc_websocket",
				TargetPort:      8080,
				Exposure:        "public",
			},
		},
	}
	component := core.Component{
		ID: "cmp_caddy", Owner: core.ComponentOwnerEnvironment, OwnerID: environment.ID,
		Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config:            map[string]any{"zone_id": "net_frontend", "caddyfile_template": "{routes}"},
		GeneratedServices: []string{"svc_caddy"}, PinnedIPv4: "10.40.0.2", Healthy: true,
	}
	return environment, component
}
