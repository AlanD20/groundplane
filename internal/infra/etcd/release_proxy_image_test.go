package etcd

import (
	"encoding/json"
	"strings"
	"testing"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"
	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func releaseProxyImageFixture() *ReleaseProxyImage {
	return &ReleaseProxyImage{Repository: "docker.io/library/caddy", IndexDigest: strings.Repeat("a", 64),
		Platform: componentsdk.OCIPlatform{OS: "linux", Architecture: "arm64", Variant: "v8",
			ChildDigest: strings.Repeat("b", 64), ConfigDigest: strings.Repeat("c", 64)}}
}

func addressableReleaseProxyFixture() ReleaseRenderInput {
	input := portlessReleaseRenderInput(domain.StrategyRecreate)
	input.ProxyPorts, input.ProxyGeneration, input.ProxyConfigDigest = []uint16{8080}, 2, "candidate"
	input.ProxyImage = releaseProxyImageFixture()
	return input
}

// Codec boundaries must reject missing or malformed managed image authority;
// accepting just the OCI reference would lose the actual config identity.
func TestReleaseRenderInputRejectsMissingOrMalformedProxyImage(t *testing.T) {
	for name, mutate := range map[string]func(*ReleaseRenderInput){
		"missing":    func(input *ReleaseRenderInput) { input.ProxyImage = nil },
		"repository": func(input *ReleaseRenderInput) { input.ProxyImage.Repository = "" },
		"index":      func(input *ReleaseRenderInput) { input.ProxyImage.IndexDigest = "bad" },
		"child":      func(input *ReleaseRenderInput) { input.ProxyImage.Platform.ChildDigest = "bad" },
		"config":     func(input *ReleaseRenderInput) { input.ProxyImage.Platform.ConfigDigest = "" },
		"platform":   func(input *ReleaseRenderInput) { input.ProxyImage.Platform.Architecture = "s390x" },
		"variant":    func(input *ReleaseRenderInput) { input.ProxyImage.Platform.Variant = "v7" },
		"portless-stray": func(input *ReleaseRenderInput) {
			input.ProxyPorts, input.ProxyGeneration, input.ProxyConfigDigest = nil, 0, ""
		},
	} {
		t.Run(name, func(t *testing.T) {
			input := addressableReleaseProxyFixture()
			mutate(&input)
			if _, err := EncodeReleaseRenderInput(input); err == nil {
				t.Fatal("invalid proxy image authority accepted on publication")
			}
			stored, err := json.Marshal(input)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := decodeReleaseRenderInput(stored); err == nil {
				t.Fatal("invalid proxy image authority accepted from durable storage")
			}
		})
	}
}

func TestReleaseRenderInputProxyImageRoundTripAndClone(t *testing.T) {
	input := addressableReleaseProxyFixture()
	want := *input.ProxyImage
	encoded, err := EncodeReleaseRenderInput(input)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeReleaseRenderInput(encoded)
	if err != nil || decoded.ProxyImage == nil || *decoded.ProxyImage != want ||
		decoded.ProxyImage.Reference() != want.Repository+"@sha256:"+want.Platform.ChildDigest {
		t.Fatalf("persisted proxy authority changed: %v", err)
	}
	cloned := cloneReleaseRenderInput(decoded)
	decoded.ProxyImage.Repository = "changed/repository"
	decoded.ProxyImage.IndexDigest = strings.Repeat("d", 64)
	decoded.ProxyImage.Platform.ConfigDigest = strings.Repeat("e", 64)
	if cloned.ProxyImage == decoded.ProxyImage || cloned.ProxyImage == nil || *cloned.ProxyImage != want {
		t.Fatal("cloned proxy authority aliases mutable caller state")
	}
	portless := portlessReleaseRenderInput(domain.StrategyRecreate)
	if cloneReleaseRenderInput(portless).ProxyImage != nil {
		t.Fatal("clone invented proxy authority for portless workload")
	}
}
