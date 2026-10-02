package postgres16protocol

import (
	"bytes"
	"encoding/json"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ManagedReleaseImage binds the measured helper files to one native OCI
// manifest and its configuration digest. These are different identities:
// Docker's image ID is not the image manifest or the multi-platform index.
// This record must come from authenticated release packaging, not inspection
// of an operator-selected container.
type ManagedReleaseImage struct {
	RepositoryDigest string                 `json:"repository_digest"`
	ImageID          string                 `json:"image_id"`
	Manifest         ManagedReleaseManifest `json:"manifest"`
}

func (image ManagedReleaseImage) Validate() error {
	if !imageref.IsDigestPinned(image.RepositoryDigest) || !workloadimage.LocalIDValid(image.ImageID) {
		return invalidConfinement("managed PostgreSQL release image identity is invalid")
	}
	_, err := image.Manifest.Authority()
	return err
}

func (image ManagedReleaseImage) ManifestDigest() string {
	_, digest, _ := strings.Cut(image.RepositoryDigest, "@")
	return digest
}

// ManagedReleaseIndex is the release-owned image choice. A Controller uses its
// index for deployment; an Agent executes only the native child selected here.
type ManagedReleaseIndex struct {
	Schema uint32                `json:"schema"`
	Image  string                `json:"image"`
	Images []ManagedReleaseImage `json:"images"`
}

func (index ManagedReleaseIndex) Validate() error {
	if index.Schema != 1 || !imageref.IsDigestPinned(index.Image) || len(index.Images) < 1 || len(index.Images) > 2 {
		return invalidConfinement("managed PostgreSQL release index is invalid")
	}
	repository, digest, _ := strings.Cut(index.Image, "@")
	previousArchitecture := ""
	for _, image := range index.Images {
		childRepository, _, _ := strings.Cut(image.RepositoryDigest, "@")
		if image.Validate() != nil || childRepository != repository ||
			image.Manifest.Architecture <= previousArchitecture ||
			(len(index.Images) == 1 && index.Image != image.RepositoryDigest) ||
			(len(index.Images) == 2 && digest == image.ManifestDigest()) {
			return invalidConfinement("managed PostgreSQL release index children are invalid")
		}
		previousArchitecture = image.Manifest.Architecture
	}
	if len(index.Images) == 2 && (index.Images[0].RepositoryDigest == index.Images[1].RepositoryDigest ||
		index.Images[0].ImageID == index.Images[1].ImageID) {
		return invalidConfinement("managed PostgreSQL native images must be distinct")
	}
	return nil
}

func (index ManagedReleaseIndex) Select(architecture string) (ManagedReleaseImage, error) {
	if err := index.Validate(); err != nil {
		return ManagedReleaseImage{}, err
	}
	for _, image := range index.Images {
		if image.Manifest.Architecture == architecture {
			return image, nil
		}
	}
	return ManagedReleaseImage{}, invalidConfinement("managed PostgreSQL build does not support this architecture")
}

func (index ManagedReleaseIndex) ContainsImageID(imageID string) bool {
	for _, image := range index.Images {
		if image.ImageID == imageID {
			return true
		}
	}
	return false
}

func DecodeManagedReleaseIndex(data []byte) (ManagedReleaseIndex, error) {
	var index ManagedReleaseIndex
	if len(data) == 0 || len(data) > 16384 {
		return index, invalidConfinement("managed PostgreSQL release index size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&index); err != nil {
		return ManagedReleaseIndex{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	canonical, err := json.Marshal(index)
	if err != nil || !bytes.Equal(canonical, bytes.TrimSpace(data)) {
		return ManagedReleaseIndex{}, invalidConfinement("managed PostgreSQL release index is not canonical")
	}
	if err := index.Validate(); err != nil {
		return ManagedReleaseIndex{}, err
	}
	return index, nil
}
