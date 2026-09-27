package registryimages

import (
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// RUN-13: Docker's classic and containerd stores expose different local IDs.
// Both must prove the same selected content; an index cannot substitute for it.
func TestFetchVerifiesSelectedContentAcrossDockerStores(t *testing.T) {
	plan := imagefetch.Plan{
		Repository: "qa/delivery", Architecture: "amd64",
		ManifestDigest: "sha256:" + strings.Repeat("a", 64),
		ConfigDigest:   "sha256:" + strings.Repeat("b", 64),
	}
	observed := client.ImageInspectResult{InspectResponse: image.InspectResponse{
		ID: plan.ConfigDigest, Os: "linux", Architecture: "amd64", RepoDigests: []string{plan.Reference()},
	}}
	if got, err := verifyImage(observed, plan); err != nil || got != plan.ConfigDigest {
		t.Fatalf("classic store content = %q, %v", got, err)
	}
	observed.ID = plan.ManifestDigest
	observed.Descriptor = &ocispec.Descriptor{
		MediaType: ocispec.MediaTypeImageManifest, Digest: digest.Digest(plan.ManifestDigest), Size: 512,
	}
	if got, err := verifyImage(observed, plan); err != nil || got != plan.ConfigDigest {
		t.Fatalf("containerd store content = %q, %v", got, err)
	}
	observed.Descriptor.MediaType = ocispec.MediaTypeImageIndex
	if _, err := verifyImage(observed, plan); err == nil {
		t.Fatal("index accepted instead of the pinned child manifest")
	}
}
