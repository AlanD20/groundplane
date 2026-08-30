package component

import "testing"

// Rationale: replay validation must reject a changed service contract even
// when the managed file bytes are unchanged.
func TestDigestEnvironmentPlanIncludesServiceBehavior(t *testing.T) {
	base := EnvironmentPlan{
		Services: []ManagedService{{
			ID: "svc_resolver", Name: "resolver", Image: "example/resolver:1",
			Command: []string{"--config", "/etc/resolver/config"}, Restart: "unless-stopped",
			Replicas: 1, Mounts: []ManagedMount{{Source: "config", Target: "/etc/resolver/config", ReadOnly: true}},
		}},
		Files: []ManagedFile{{Path: "config", Content: []byte("same bytes\n")}},
	}
	changed := CloneEnvironmentPlan(base)
	changed.Services[0].Image = "example/resolver:2"
	if base.Files[0].Path != changed.Files[0].Path || string(base.Files[0].Content) != string(changed.Files[0].Content) {
		t.Fatal("replay fixture must retain identical file metadata and bytes")
	}
	if DigestEnvironmentPlan(base) == DigestEnvironmentPlan(changed) {
		t.Fatal("DigestEnvironmentPlan() ignored changed service behavior")
	}
}

// Rationale: an edge transport must receive one canonical managed-Service
// origin rather than infer an implementation name or accept an ambiguous URL.
func TestValidateHTTPRouterInputRequiresCanonicalManagedOrigin(t *testing.T) {
	t.Parallel()
	valid := HTTPRouterInput{
		ComponentID: "cmp_router", Enabled: true, GeneratedServiceID: "svc_router",
		ZoneID: "net_frontend", ZoneName: "frontend", PinnedIPv4: "10.40.0.2",
		Origin: HTTPRouterOrigin{ServiceName: "edge-router", URL: "http://edge-router:8080"},
	}
	if err := ValidateHTTPRouterInput(valid); err != nil {
		t.Fatalf("ValidateHTTPRouterInput() error = %v", err)
	}
	for _, invalid := range []HTTPRouterOrigin{
		{},
		{ServiceName: "edge-router", URL: "http://another-router:8080"},
		{ServiceName: "edge-router", URL: "http://edge-router:8080/"},
		{ServiceName: "edge-router", URL: "https://edge-router:8080"},
		{ServiceName: "edge-router", URL: "http://edge-router:08080"},
	} {
		candidate := valid
		candidate.Origin = invalid
		if err := ValidateHTTPRouterInput(candidate); err == nil {
			t.Fatalf("ValidateHTTPRouterInput() accepted origin %#v", invalid)
		}
	}
}

// Rationale: fixed-revision planning must retain the exact origin while
// detaching mutable Route storage from the caller.
func TestCloneHTTPRouterInputPreservesOriginAndDetachesRoutes(t *testing.T) {
	t.Parallel()
	input := HTTPRouterInput{
		Origin: HTTPRouterOrigin{ServiceName: "edge-router", URL: "http://edge-router:8080"},
		Routes: []HTTPRoute{{ID: "rte_one", Path: "/one"}},
	}
	cloned := CloneHTTPRouterInput(input)
	cloned.Routes[0].Path = "/changed"
	if input.Routes[0].Path != "/one" || cloned.Origin != input.Origin {
		t.Fatalf("CloneHTTPRouterInput() source/clone = %#v / %#v", input, cloned)
	}
}
