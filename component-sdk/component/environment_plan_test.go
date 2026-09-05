package component

import (
	"strings"
	"testing"
)

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
	if base.Files[0].Path != changed.Files[0].Path ||
		string(base.Files[0].Content) != string(changed.Files[0].Content) {
		t.Fatal("replay fixture must retain identical file metadata and bytes")
	}
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
			{OS: "linux", Architecture: "amd64", ChildDigest: strings.Repeat("b", 64), ConfigDigest: strings.Repeat("c", 64)},
			{OS: "linux", Architecture: "arm64", Variant: "v8", ChildDigest: strings.Repeat("d", 64), ConfigDigest: strings.Repeat("e", 64)},
		},
	}
}

// Rationale: runtime selection uses only the host platform and returns the
// catalog-authenticated variant rather than accepting a caller-invented value.
func TestOCIImageSelectReturnsAuthenticatedPlatform(t *testing.T) {
	image := OCIImage{Repository: "example/resolver", IndexDigest: strings.Repeat("e", 64), Platforms: []OCIPlatform{
		{OS: "linux", Architecture: "amd64", ChildDigest: strings.Repeat("a", 64), ConfigDigest: strings.Repeat("b", 64)},
		{OS: "linux", Architecture: "arm64", Variant: "v8", ChildDigest: strings.Repeat("c", 64), ConfigDigest: strings.Repeat("d", 64)},
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

func TestOCIImageValidateRejectsMalformedAuthority(t *testing.T) {
	valid := environmentPlanTestImage("example/resolver")
	if err := valid.Validate(); err != nil || !valid.Equal(environmentPlanTestImage("example/resolver")) {
		t.Fatalf("valid image rejected or unequal: %v", err)
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

// Rationale: an HTTP-router planning input must carry one canonical managed
// Service origin rather than accept an ambiguous URL.
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
