package controllerupgrade

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BuildMetadata is the local build's declaration, transferred with the exact
// Controller bytes. Deployment adds the registry-resolved Agent digest before
// hashing the complete immutable Manifest. It is not an activation input.
type BuildMetadata struct {
	Schema            int                          `json:"schema"`
	ControllerSHA256  Digest                       `json:"controller_sha256"`
	ControllerVersion string                       `json:"controller_version"`
	ControllerSource  buildversion.BuildProvenance `json:"controller_source"`
	StorageEpoch      int                          `json:"storage_epoch"`
	ChannelSchema     int                          `json:"channel_schema"`
}

func DescribeBuild(binary Digest, version string, source buildversion.BuildProvenance) (BuildMetadata, error) {
	if !binary.Valid() || !validVersion(version) || source.Validate() != nil {
		return BuildMetadata{}, errs.New(errs.KindValidationFailed, "controller build identity is invalid")
	}
	return BuildMetadata{
		Schema:            ManifestSchema,
		ControllerSHA256:  binary,
		ControllerVersion: version,
		ControllerSource:  source,
		StorageEpoch:      StorageEpoch,
		ChannelSchema:     executionplan.SchemaVersion,
	}, nil
}
