package registryimages

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

func (registryClient *Client) Resolve(ctx context.Context, requested string) (imagefetch.Plan, error) {
	named, err := imagefetch.ParseRequested(requested)
	if err != nil {
		return imagefetch.Plan{}, err
	}
	selector := ""
	if tagged, ok := named.(reference.NamedTagged); ok {
		selector = tagged.Tag()
	} else if pinned, ok := named.(reference.Digested); ok {
		selector = pinned.Digest().String()
	}
	value, manifestDigest, err := registryClient.document(ctx, named, "manifests", selector)
	if err != nil {
		return imagefetch.Plan{}, err
	}
	index, err := decodeDocument[ocispec.Index](value)
	if err != nil || index.SchemaVersion != 2 {
		return imagefetch.Plan{}, errs.New(errs.KindStateConflict, "registry image schema is unsupported")
	}
	if index.MediaType == ocispec.MediaTypeImageIndex ||
		index.MediaType == "application/vnd.docker.distribution.manifest.list.v2+json" {
		var selected *ocispec.Descriptor
		for _, descriptor := range index.Manifests {
			platform := descriptor.Platform
			if platform == nil || platform.OS != "linux" || platform.Architecture != registryClient.architecture ||
				(platform.Variant != "" && !(platform.Architecture == "arm64" && platform.Variant == "v8")) {
				continue
			}
			if selected != nil {
				return imagefetch.Plan{}, errs.New(
					errs.KindStateConflict,
					"registry image has ambiguous host platforms",
				)
			}
			copy := descriptor
			selected = &copy
		}
		if selected == nil || selected.Digest.Validate() != nil || selected.Digest.Algorithm().String() != "sha256" ||
			selected.Size <= 0 || selected.Size > maximumDocumentBytes {
			return imagefetch.Plan{}, errs.New(
				errs.KindStateConflict,
				"registry image lacks a supported host platform",
			)
		}
		value, manifestDigest, err = registryClient.document(ctx, named, "manifests", selected.Digest.String())
		if err != nil {
			return imagefetch.Plan{}, err
		}
		if int64(len(value)) != selected.Size {
			return imagefetch.Plan{}, errs.New(
				errs.KindStateConflict,
				"registry manifest size differs from its index",
			)
		}
	}
	manifest, err := decodeDocument[ocispec.Manifest](value)
	if err != nil || manifest.SchemaVersion != 2 ||
		(manifest.MediaType != ocispec.MediaTypeImageManifest && manifest.MediaType != "application/vnd.docker.distribution.manifest.v2+json") ||
		manifest.Config.Digest.Validate() != nil || manifest.Config.Digest.Algorithm().String() != "sha256" ||
		manifest.Config.Size <= 0 || manifest.Config.Size > maximumDocumentBytes {
		return imagefetch.Plan{}, errs.New(errs.KindStateConflict, "registry image manifest is invalid")
	}
	configBytes, configDigest, err := registryClient.document(ctx, named, "blobs", manifest.Config.Digest.String())
	if err != nil {
		return imagefetch.Plan{}, err
	}
	configuration, err := decodeDocument[ocispec.Image](configBytes)
	if err != nil || int64(len(configBytes)) != manifest.Config.Size || configuration.OS != "linux" ||
		configuration.Architecture != registryClient.architecture {
		return imagefetch.Plan{}, errs.New(
			errs.KindStateConflict,
			"registry image does not match the host platform",
		)
	}
	plan := imagefetch.Plan{
		Requested: requested, Repository: reference.TrimNamed(named).String(),
		ManifestDigest: manifestDigest, ConfigDigest: configDigest,
		Architecture: configuration.Architecture, Variant: configuration.Variant,
	}
	return plan, plan.Validate()
}
