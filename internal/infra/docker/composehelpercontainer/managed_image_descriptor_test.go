package composehelpercontainer

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/moby/moby/api/types/image"
	"github.com/moby/moby/client"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-31: OCI and classic Docker identities need different evidence; neither
// a wrong platform nor an index/config descriptor can stand in for the child.
func TestManagedImageDescriptorIdentity(t *testing.T) {
	for _, name := range []string{
		"oci-child", "docker-child", "explicit-platform", "classic-config",
		"child-without-descriptor", "descriptor-with-config-id", "wrong-child",
		"wrong-id", "index", "config-media", "empty-media", "zero-size",
		"negative-size", "wrong-os", "wrong-architecture", "wrong-variant",
		"descriptor-platform", "descriptor-os-version", "descriptor-os-features",
	} {
		t.Run(name, func(t *testing.T) {
			service := managedService()
			child := "sha256:" + hex.EncodeToString(service.ImageChildDigest)
			config := "sha256:" + hex.EncodeToString(service.ImageConfigDigest)
			observed := client.ImageInspectResult{InspectResponse: image.InspectResponse{
				ID: child, Os: "linux", Architecture: "amd64",
				Descriptor: &ocispec.Descriptor{
					Digest:    digest.Digest(child),
					MediaType: ocispec.MediaTypeImageManifest,
					Size:      1917,
				},
			}}
			wantSuccess := name == "oci-child" || name == "docker-child" || name == "explicit-platform" ||
				name == "classic-config"
			switch name {
			case "docker-child":
				observed.Descriptor.MediaType = "application/vnd.docker.distribution.manifest.v2+json"
			case "explicit-platform":
				observed.Descriptor.Platform = &ocispec.Platform{OS: "linux", Architecture: "amd64"}
			case "classic-config":
				observed.ID, observed.Descriptor = config, nil
			case "child-without-descriptor":
				observed.Descriptor = nil
			case "descriptor-with-config-id":
				observed.ID = config
			case "wrong-child":
				observed.Descriptor.Digest = digest.FromString("other-child")
				observed.ID = observed.Descriptor.Digest.String()
			case "wrong-id":
				observed.ID = digest.FromString("other-id").String()
			case "index":
				observed.Descriptor.MediaType = ocispec.MediaTypeImageIndex
			case "config-media":
				observed.Descriptor.MediaType = ocispec.MediaTypeImageConfig
			case "empty-media":
				observed.Descriptor.MediaType = ""
			case "zero-size":
				observed.Descriptor.Size = 0
			case "negative-size":
				observed.Descriptor.Size = -1
			case "wrong-os":
				observed.Os = "windows"
			case "wrong-architecture":
				observed.Architecture = "arm64"
			case "wrong-variant":
				observed.Variant = "v8"
			case "descriptor-platform":
				observed.Descriptor.Platform = &ocispec.Platform{OS: "linux", Architecture: "arm64"}
			case "descriptor-os-version":
				observed.Descriptor.Platform = &ocispec.Platform{
					OS:           "linux",
					Architecture: "amd64",
					OSVersion:    "unsealed",
				}
			case "descriptor-os-features":
				observed.Descriptor.Platform = &ocispec.Platform{
					OS:           "linux",
					Architecture: "amd64",
					OSFeatures:   []string{"unsealed"},
				}
			}
			engine := &imageEngine{fakeEngine: newFakeEngine(nil), observed: observed}
			executor, err := NewWithEngine(engine, helperImage)
			if err != nil {
				t.Fatal(err)
			}
			err = executor.prepareServiceImage(context.Background(), service)
			if wantSuccess {
				if err != nil {
					t.Fatalf("valid managed identity rejected: %v", err)
				}
			} else if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("unproven managed identity accepted: %v", err)
			}
			if len(engine.pulled) != 0 || len(engine.inspected) != 1 || engine.inspected[0] != service.ImageReference {
				t.Fatalf(
					"image lookup changed sealed authority: inspected=%v pulled=%v",
					engine.inspected,
					engine.pulled,
				)
			}
		})
	}
}
