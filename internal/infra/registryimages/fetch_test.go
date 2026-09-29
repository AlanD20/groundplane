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
		Repository: "docker.io/qa/delivery", Architecture: "amd64",
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

// IMG-01: familiar Docker Hub names must verify as the same pinned repository;
// normalizing a name must never accept another repository, digest or platform.
func TestFetchVerifiesNormalizedRepositoryDigest(t *testing.T) {
	plan := imagefetch.Plan{
		Repository:     "docker.io/library/nginx",
		Architecture:   "amd64",
		ManifestDigest: "sha256:" + strings.Repeat("a", 64),
		ConfigDigest:   "sha256:" + strings.Repeat("b", 64),
	}
	for _, value := range []struct {
		ref      string
		accepted bool
	}{
		{"nginx@" + plan.ManifestDigest, true},
		{"library/nginx@" + plan.ManifestDigest, true},
		{plan.Reference(), true},
		{"other.example/library/nginx@" + plan.ManifestDigest, false},
		{"nginx@sha256:" + strings.Repeat("c", 64), false},
		{"nginx:latest", false},
	} {
		t.Run(value.ref, func(t *testing.T) {
			observed := client.ImageInspectResult{
				InspectResponse: image.InspectResponse{
					ID:           plan.ConfigDigest,
					Os:           "linux",
					Architecture: "amd64",
					RepoDigests:  []string{value.ref},
				},
			}
			_, err := verifyImage(observed, plan)
			if (err == nil) != value.accepted {
				t.Fatalf("accepted=%t, error=%v", value.accepted, err)
			}
			observed.Architecture = "arm64"
			if _, err := verifyImage(observed, plan); err == nil {
				t.Fatal("wrong platform accepted")
			}
		})
	}
}
