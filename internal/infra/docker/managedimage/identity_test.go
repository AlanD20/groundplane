package managedimage

import (
	"errors"
	"testing"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestVerifyRequiresSealedAuthority(t *testing.T) {
	for _, name := range []string{"child", "config", "os", "architecture", "variant", "os-version", "os-features"} {
		t.Run(name, func(t *testing.T) {
			child, config := digest.FromString("child").String(), digest.FromString("config").String()
			platform := ocispec.Platform{OS: "linux", Architecture: "amd64"}
			switch name {
			case "child":
				child = "sha256:invalid"
			case "config":
				config = ""
			case "os":
				platform.OS = "windows"
			case "architecture":
				platform.Architecture = "unknown"
			case "variant":
				platform.Variant = "v8"
			case "os-version":
				platform.OSVersion = "unsealed"
			case "os-features":
				platform.OSFeatures = []string{"unsealed"}
			}
			if err := Verify(config, nil, child, config, platform); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("invalid sealed authority accepted: %v", err)
			}
		})
	}
}

func TestVerifyArm64DescriptorVariantIsExact(t *testing.T) {
	child, config := digest.FromString("child"), digest.FromString("config")
	for _, variant := range []string{"", "v8"} {
		platform := ocispec.Platform{OS: "linux", Architecture: "arm64", Variant: variant}
		descriptor := &ocispec.Descriptor{
			Digest:    child,
			MediaType: ocispec.MediaTypeImageManifest,
			Size:      1917,
			Platform:  &platform,
		}
		if err := Verify(child.String(), descriptor, child.String(), config.String(), platform); err != nil {
			t.Fatalf("valid arm64 variant rejected: %v", err)
		}
		other := platform
		other.Variant = "v9"
		descriptor.Platform = &other
		if err := Verify(child.String(), descriptor, child.String(), config.String(), platform); !errors.Is(
			err,
			errs.New(errs.KindStateConflict, ""),
		) {
			t.Fatalf("different descriptor variant accepted: %v", err)
		}
	}
}
