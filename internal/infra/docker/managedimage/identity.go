// Package managedimage verifies sealed managed image identity at Docker boundaries.
package managedimage

import (
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Verify distinguishes classic config IDs from containerd target descriptors.
// The caller must separately check any observed image configuration platform.
func Verify(
	localID string,
	descriptor *ocispec.Descriptor,
	childDigest, configDigest string,
	platform ocispec.Platform,
) error {
	if !workloadimage.LocalIDValid(childDigest) || !workloadimage.LocalIDValid(configDigest) ||
		platform.OS != "linux" || platform.Architecture != "amd64" && platform.Architecture != "arm64" ||
		platform.Architecture == "amd64" && platform.Variant != "" ||
		platform.Architecture == "arm64" && platform.Variant != "" && platform.Variant != "v8" ||
		platform.OSVersion != "" || len(platform.OSFeatures) != 0 {
		return errs.New(errs.KindValidationFailed, "managed image lacks sealed platform authority")
	}
	// Classic Docker reports the config digest. Its containerd image store
	// instead reports the target descriptor digest, which must be our exact
	// selected child manifest, never an index or a fallback config identity.
	if descriptor == nil {
		if localID != configDigest {
			return errs.New(errs.KindStateConflict, "managed image differs from sealed config identity")
		}
		return nil
	}
	if descriptor.MediaType != ocispec.MediaTypeImageManifest &&
		descriptor.MediaType != "application/vnd.docker.distribution.manifest.v2+json" ||
		descriptor.Size <= 0 ||
		descriptor.Digest.String() != childDigest ||
		localID != descriptor.Digest.String() {
		return errs.New(errs.KindStateConflict, "managed image differs from sealed child manifest identity")
	}
	if observed := descriptor.Platform; observed != nil &&
		(observed.OS != platform.OS || observed.Architecture != platform.Architecture || observed.Variant != platform.Variant ||
			observed.OSVersion != "" || len(observed.OSFeatures) != 0) {
		return errs.New(errs.KindStateConflict, "managed image descriptor differs from sealed platform identity")
	}
	return nil
}
