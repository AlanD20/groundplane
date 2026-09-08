package managedconfighelpercontainer

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/AlanD20/groundplane/pkg/errs"
	containerderrdefs "github.com/containerd/errdefs"
)

// Docker's containerd image store identifies a platform image by its child
// manifest. The verified observed ID, not the config digest, must reach create.
func TestValidatorCreatesFromContainerdChildManifestIdentity(t *testing.T) {
	child := strings.Split(pinnedValidatorImage, "@")[1]
	createErr := errors.New("stop after verified create")
	engine := &fakeEngine{
		imageID: child,
		descriptor: &ocispec.Descriptor{
			MediaType: ocispec.MediaTypeImageManifest,
			Digest:    digest.Digest(child),
			Size:      123,
		},
		createErr: createErr,
	}
	err := mustExecutor(
		t,
		engine,
	).Validate(t.Context(), validatorTestImage(), []string{"-conf", "/dev/stdin"}, []byte(".:53 {}"))
	if !errors.Is(err, createErr) ||
		!reflect.DeepEqual(engine.events, []string{"inspect:" + pinnedValidatorImage, "create:" + child}) {
		t.Fatalf("containerd identity rejected before create: error=%v events=%v", err, engine.events)
	}
}

func TestValidatorPullVerifiesObservedIdentityAndCleansCreatedContainer(t *testing.T) {
	for _, containerd := range []bool{false, true} {
		name := "classic"
		if containerd {
			name = "containerd"
		}
		t.Run(name, func(t *testing.T) {
			attachErr := errors.New("attach unavailable")
			engine := &fakeEngine{
				imageID:    pinnedValidatorID,
				inspectErr: containerderrdefs.ErrNotFound,
				createdID:  "validator",
				attachErr:  attachErr,
			}
			if containerd {
				engine.imageID = strings.Split(pinnedValidatorImage, "@")[1]
				engine.descriptor = &ocispec.Descriptor{
					MediaType: ocispec.MediaTypeImageManifest,
					Digest:    digest.Digest(engine.imageID),
					Size:      123,
				}
			}
			err := mustExecutor(
				t,
				engine,
			).Validate(t.Context(), validatorTestImage(), []string{"-conf", "/dev/stdin"}, []byte(".:53 {}"))
			want := []string{
				"inspect:" + pinnedValidatorImage,
				"pull:" + pinnedValidatorImage,
				"pull-wait",
				"pull-close",
				"inspect:" + pinnedValidatorImage,
				"create:" + engine.imageID,
				"attach",
				"remove",
			}
			if !errors.Is(err, attachErr) || !reflect.DeepEqual(engine.events, want) {
				t.Fatalf("pull/verify/create/cleanup: error=%v events=%v", err, engine.events)
			}
		})
	}
}

func TestValidatorRejectsDescriptorAndOuterPlatformMismatchBeforeCreate(t *testing.T) {
	for _, mismatch := range []string{"child", "local-id", "index", "os", "architecture", "variant"} {
		t.Run(mismatch, func(t *testing.T) {
			child := strings.Split(pinnedValidatorImage, "@")[1]
			engine := &fakeEngine{
				imageID: child,
				descriptor: &ocispec.Descriptor{
					MediaType: ocispec.MediaTypeImageManifest,
					Digest:    digest.Digest(child),
					Size:      123,
				},
				platform: &ocispec.Platform{OS: "linux", Architecture: "amd64"},
			}
			switch mismatch {
			case "child":
				engine.descriptor.Digest = digest.Digest("sha256:" + strings.Repeat("f", 64))
			case "local-id":
				engine.imageID = pinnedValidatorID
			case "index":
				engine.descriptor.MediaType = ocispec.MediaTypeImageIndex
			case "os":
				engine.platform.OS = "windows"
			case "architecture":
				engine.platform.Architecture = "arm64"
			case "variant":
				engine.platform.Variant = "v8"
			}
			err := mustExecutor(
				t,
				engine,
			).Validate(t.Context(), validatorTestImage(), []string{"-conf", "/dev/stdin"}, []byte(".:53 {}"))
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) ||
				!reflect.DeepEqual(engine.events, []string{"inspect:" + pinnedValidatorImage}) {
				t.Fatalf("mismatch accepted: error=%v events=%v", err, engine.events)
			}
		})
	}
}

func TestValidatorRejectsUnboundReferenceBeforeInspect(t *testing.T) {
	image := validatorTestImage()
	image.Platform.ChildDigest = strings.Repeat("f", 64)
	engine := &fakeEngine{}
	err := mustExecutor(t, engine).Validate(t.Context(), image, []string{"-conf", "/dev/stdin"}, []byte(".:53 {}"))
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || len(engine.events) != 0 {
		t.Fatalf("unbound reference accepted: error=%v events=%v", err, engine.events)
	}
}

// Rationale: cached metadata claiming the requested OCI reference must not
// authorize a validator whose actual image/config ID differs from the catalog.
func TestValidatorRejectsWrongConfigIdentityBeforeCreate(t *testing.T) {
	engine := &fakeEngine{imageID: "sha256:" + strings.Repeat("f", 64), createErr: errors.New("create reached")}
	err := mustExecutor(
		t,
		engine,
	).Validate(t.Context(), validatorTestImage(), []string{"-conf", "/dev/stdin"}, []byte(".:53 {}"))
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("wrong image error = %v, want identity conflict", err)
	}
	for _, event := range engine.events {
		if strings.HasPrefix(event, "create:") || strings.HasPrefix(event, "pull:") {
			t.Fatalf("wrong image authorized side effect: %v", engine.events)
		}
	}
}

// Rationale: finishing the pull stream does not prove the downloaded image is
// the catalog's config. Reinspection must precede validator creation.
func TestValidatorRejectsPulledConfigMismatchBeforeCreate(t *testing.T) {
	engine := &fakeEngine{imageID: "sha256:" + strings.Repeat("f", 64), inspectErr: containerderrdefs.ErrNotFound}
	err := mustExecutor(
		t,
		engine,
	).Validate(t.Context(), validatorTestImage(), []string{"-conf", "/dev/stdin"}, []byte(".:53 {}"))
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("pulled wrong image error = %v, want identity conflict", err)
	}
	for _, event := range engine.events {
		if strings.HasPrefix(event, "create:") {
			t.Fatalf("created substituted image: %v", engine.events)
		}
	}
}
