package component

import (
	"strings"
	"testing"
)

// QA: CMP-04; pure plan-digest proof only, not durable replay or runtime rejection.
// Rationale: replay validation must reject a changed service contract even
// when the managed file bytes are unchanged.
func TestDigestEnvironmentPlanIncludesServiceBehavior(t *testing.T) {
	base := EnvironmentPlan{
		Services: []ManagedService{{
			ID: "svc_resolver", Name: "resolver", Image: environmentPlanTestImage("example/resolver"),
			Command: []string{"--config", "/etc/resolver/config"}, Restart: "unless-stopped",
			Replicas: 1, Mounts: []ManagedMount{{Source: "config", Target: "/etc/resolver/config", ReadOnly: true}},
		}},
		Files: []ManagedFile{{Path: "config", Content: []byte("same bytes\n")}},
	}
	changed := CloneEnvironmentPlan(base)
	changed.Services[0].Image = environmentPlanTestImage("example/resolver-next")
	if DigestEnvironmentPlan(base) == DigestEnvironmentPlan(changed) {
		t.Fatal("DigestEnvironmentPlan() ignored changed service behavior")
	}
	configChanged := CloneEnvironmentPlan(base)
	configChanged.Services[0].Image.Platforms[0].ConfigDigest = strings.Repeat("d", 64)
	if DigestEnvironmentPlan(base) == DigestEnvironmentPlan(configChanged) {
		t.Fatal("DigestEnvironmentPlan() ignored changed image config identity")
	}
}

func environmentPlanTestImage(repository string) OCIImage {
	return OCIImage{
		Repository:  repository,
		IndexDigest: strings.Repeat("a", 64),
		Platforms: []OCIPlatform{
			{
				OS:           "linux",
				Architecture: "amd64",
				ChildDigest:  strings.Repeat("b", 64),
				ConfigDigest: strings.Repeat("c", 64),
			},
			{
				OS:           "linux",
				Architecture: "arm64",
				Variant:      "v8",
				ChildDigest:  strings.Repeat("d", 64),
				ConfigDigest: strings.Repeat("e", 64),
			},
		},
	}
}

// QA: CMP-01, CMP-04; pure compiled-image selection only, not an image pull or host-architecture journey.
// Rationale: runtime selection uses only the host platform and returns the
// catalog-authenticated variant rather than accepting a caller-invented value.
func TestOCIImageSelectReturnsAuthenticatedPlatform(t *testing.T) {
	image := OCIImage{Repository: "example/resolver", IndexDigest: strings.Repeat("e", 64), Platforms: []OCIPlatform{
		{
			OS:           "linux",
			Architecture: "amd64",
			ChildDigest:  strings.Repeat("a", 64),
			ConfigDigest: strings.Repeat("b", 64),
		},
		{
			OS:           "linux",
			Architecture: "arm64",
			Variant:      "v8",
			ChildDigest:  strings.Repeat("c", 64),
			ConfigDigest: strings.Repeat("d", 64),
		},
	}}
	selected, reference, found := image.Select("linux", "arm64")
	if !found || selected.Variant != "v8" || selected.ConfigDigest != strings.Repeat("d", 64) ||
		reference != "example/resolver@sha256:"+strings.Repeat("c", 64) {
		t.Fatalf("Select() = %#v, %q, %t", selected, reference, found)
	}
	if _, _, found := image.Select("linux", "s390x"); found {
		t.Fatal("Select() accepted unsupported architecture")
	}
}

// QA: CMP-01, CMP-04; pure image-authority validation only, not registry verification or runtime selection.
// Rationale: managed images must contain one canonical authenticated identity
// for each supported platform and reject mutable, ambiguous, or malformed variants.
func TestOCIImageValidateRejectsMalformedAuthority(t *testing.T) {
	valid := environmentPlanTestImage("example/resolver")
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid image rejected: %v", err)
	}
	tests := map[string]func(*OCIImage){
		"missing repository": func(image *OCIImage) { image.Repository = "" },
		"tagged repository":  func(image *OCIImage) { image.Repository += "@sha256:mutable" },
		"missing index":      func(image *OCIImage) { image.IndexDigest = "" },
		"uppercase index":    func(image *OCIImage) { image.IndexDigest = strings.Repeat("A", 64) },
		"missing platform":   func(image *OCIImage) { image.Platforms = image.Platforms[:1] },
		"duplicate platform": func(image *OCIImage) { image.Platforms[1] = image.Platforms[0] },
		"extra platform":     func(image *OCIImage) { image.Platforms = append(image.Platforms, OCIPlatform{}) },
		"wrong platform order": func(image *OCIImage) {
			image.Platforms[0], image.Platforms[1] = image.Platforms[1], image.Platforms[0]
		},
		"amd64 variant":   func(image *OCIImage) { image.Platforms[0].Variant = "v8" },
		"arm64 variant":   func(image *OCIImage) { image.Platforms[1].Variant = "v9" },
		"zero digest":     func(image *OCIImage) { image.Platforms[0].ConfigDigest = strings.Repeat("0", 64) },
		"malformed child": func(image *OCIImage) { image.Platforms[0].ChildDigest = "sha256:invalid" },
		"uppercase config": func(image *OCIImage) {
			image.Platforms[1].ConfigDigest = strings.Repeat("F", 64)
		},
		"reused child":  func(image *OCIImage) { image.Platforms[1].ChildDigest = image.Platforms[0].ChildDigest },
		"reused config": func(image *OCIImage) { image.Platforms[1].ConfigDigest = image.Platforms[0].ConfigDigest },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			image := environmentPlanTestImage("example/resolver")
			mutate(&image)
			if image.Validate() == nil {
				t.Fatal("Validate() accepted malformed image")
			}
			if _, _, found := image.Select("linux", "amd64"); found {
				t.Fatal("Select() accepted malformed image")
			}
		})
	}
}

// QA: HTTP-03, HTTP-04; pure router-input validation only, not rendering, publication, or reachability.
// Rationale: an HTTP-router planning input must carry one canonical managed
// Service origin rather than accept an ambiguous URL.
func TestValidateHTTPRouterInputRequiresCanonicalManagedOrigin(t *testing.T) {
	t.Parallel()
	valid := HTTPRouterInput{
		ComponentID: "cmp_router", Enabled: true, GeneratedServiceID: "svc_router",
		Zones: []HTTPRouterZoneInput{
			{ID: "net_frontend", Name: "frontend", StaticIPv4: "10.40.0.2"},
			{ID: "net_services", Name: "services"},
		},
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

// QA: NET-02, HTTP-07; pure ordered-Zone validation only, not reservation or runtime attachment.
// Rationale: primary address ownership and replay require one ordered,
// duplicate-free Zone view with a static address only on the first Zone.
func TestValidateHTTPRouterInputRejectsInvalidOrderedZones(t *testing.T) {
	t.Parallel()
	valid := HTTPRouterInput{
		ComponentID: "cmp_router", Enabled: true, GeneratedServiceID: "svc_router",
		Zones: []HTTPRouterZoneInput{
			{ID: "net_primary", Name: "primary", StaticIPv4: "10.40.0.2"},
			{ID: "net_secondary", Name: "secondary"},
		},
		Origin: HTTPRouterOrigin{ServiceName: "edge-router", URL: "http://edge-router:8080"},
	}
	for name, mutate := range map[string]func(*HTTPRouterInput){
		"missing zones":       func(input *HTTPRouterInput) { input.Zones = nil },
		"duplicate id":        func(input *HTTPRouterInput) { input.Zones[1].ID = input.Zones[0].ID },
		"duplicate name":      func(input *HTTPRouterInput) { input.Zones[1].Name = input.Zones[0].Name },
		"missing primary pin": func(input *HTTPRouterInput) { input.Zones[0].StaticIPv4 = "" },
		"secondary pin":       func(input *HTTPRouterInput) { input.Zones[1].StaticIPv4 = "10.40.1.2" },
	} {
		t.Run(name, func(t *testing.T) {
			input := CloneHTTPRouterInput(valid)
			mutate(&input)
			if ValidateHTTPRouterInput(input) == nil {
				t.Fatalf("ValidateHTTPRouterInput() accepted %#v", input.Zones)
			}
		})
	}
}

// QA: CMP-02, CMP-04; pure snapshot-copy proof only, not a fixed-revision read or publication race.
// Rationale: fixed-revision planning must retain the exact origin while
// detaching mutable Route storage from the caller.
func TestCloneHTTPRouterInputPreservesOriginAndDetachesRoutes(t *testing.T) {
	t.Parallel()
	input := HTTPRouterInput{
		Origin: HTTPRouterOrigin{ServiceName: "edge-router", URL: "http://edge-router:8080"},
		Zones:  []HTTPRouterZoneInput{{ID: "net_primary", Name: "primary", StaticIPv4: "10.40.0.2"}},
		Routes: []HTTPRoute{{ID: "rte_one", Path: "/one"}},
	}
	cloned := CloneHTTPRouterInput(input)
	cloned.Routes[0].Path = "/changed"
	cloned.Zones[0].Name = "changed"
	if input.Routes[0].Path != "/one" || input.Zones[0].Name != "primary" || cloned.Origin != input.Origin {
		t.Fatalf("CloneHTTPRouterInput() source/clone = %#v / %#v", input, cloned)
	}
}

// QA: CMP-04, HTTP-08; pure plan-digest proof only, not Tunnel connectivity or durable replay.
// Rationale: gateway selection changes runtime connectivity and therefore must
// be immutable replay authority rather than digest-invisible metadata.
func TestDigestEnvironmentPlanIncludesGatewayPriority(t *testing.T) {
	t.Parallel()
	base := EnvironmentPlan{Services: []ManagedService{{
		ID: "svc_tunnel", Name: "tunnel", Image: environmentPlanTestImage("example/tunnel"),
		NetworkMode: ManagedNetworkModeZones,
		Networks:    []ManagedNetworkAttachment{{Name: "frontend"}},
	}}}
	changed := CloneEnvironmentPlan(base)
	changed.Services[0].Networks[0].GatewayPriority = 1
	if DigestEnvironmentPlan(base) == DigestEnvironmentPlan(changed) {
		t.Fatal("DigestEnvironmentPlan() ignored gateway priority")
	}
}
