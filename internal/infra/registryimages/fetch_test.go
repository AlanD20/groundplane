package registryimages

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// IMG-01: tagging is a separate, post-verification Docker effect. A successful
// assignment must resolve to the selected bytes, while a daemon failure is
// releasable only when the prior tag identity is still observed unchanged.
func TestAssignTagMovesOnlyToVerifiedContentAndPreservesPriorOnFailure(t *testing.T) {
	plan := imagefetch.Plan{
		Requested: "nginx:latest", Repository: "docker.io/library/nginx", Architecture: "amd64",
		ManifestDigest: "sha256:" + strings.Repeat("a", 64),
		ConfigDigest:   "sha256:" + strings.Repeat("b", 64),
	}
	old := client.ImageInspectResult{InspectResponse: image.InspectResponse{
		ID: "sha256:" + strings.Repeat("c", 64), Os: "linux", Architecture: "amd64",
		RepoTags: []string{"nginx:latest"},
	}}
	exact := client.ImageInspectResult{InspectResponse: image.InspectResponse{
		ID: plan.ConfigDigest, Os: "linux", Architecture: "amd64", RepoTags: []string{"nginx:latest"},
	}}
	t.Run("move", func(t *testing.T) {
		engine := &tagEngine{current: old, after: exact}
		registry := &Client{engine: engine, architecture: "amd64"}
		outcome, err := registry.AssignTag(context.Background(), plan)
		if err != nil || outcome != imagefetch.TagAssignmentApplied ||
			engine.tag.Source != plan.Reference() || engine.tag.Target != "docker.io/library/nginx:latest" {
			t.Fatalf("AssignTag() = %v, %v, tag=%#v", outcome, err, engine.tag)
		}
	})
	t.Run("failed unchanged", func(t *testing.T) {
		daemonErr := errors.New("daemon rejected tag")
		engine := &tagEngine{current: old, tagErr: daemonErr}
		registry := &Client{engine: engine, architecture: "amd64"}
		outcome, err := registry.AssignTag(context.Background(), plan)
		if outcome != imagefetch.TagAssignmentUnchanged || !errors.Is(err, daemonErr) || engine.current.ID != old.ID {
			t.Fatalf("AssignTag() = %v, %v, current=%s", outcome, err, engine.current.ID)
		}
	})
}

type tagEngine struct {
	current client.ImageInspectResult
	after   client.ImageInspectResult
	tag     client.ImageTagOptions
	tagErr  error
}

func (engine *tagEngine) ImagePull(
	context.Context,
	string,
	client.ImagePullOptions,
) (client.ImagePullResponse, error) {
	return nil, errors.New("unexpected pull")
}

func (engine *tagEngine) ImageInspect(
	context.Context,
	string,
	...client.ImageInspectOption,
) (client.ImageInspectResult, error) {
	return engine.current, nil
}

func (engine *tagEngine) ImageTag(
	_ context.Context,
	options client.ImageTagOptions,
) (client.ImageTagResult, error) {
	engine.tag = options
	if engine.tagErr == nil {
		engine.current = engine.after
	}
	return client.ImageTagResult{}, engine.tagErr
}

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
