package agentprotocol

import (
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	buildversion "github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const ImageMetadataPath = "/groundplane-agent-release.json"

// ImageMetadata is produced with the Agent binary, not supplied by an operator
// as an update override. The image digest binds both files.
type ImageMetadata struct {
	Schema        int                          `json:"schema"`
	BinarySHA256  string                       `json:"binary_sha256"`
	Version       string                       `json:"version"`
	Source        buildversion.BuildProvenance `json:"source"`
	ChannelSchema int                          `json:"channel_schema"`
}

func DescribeImage(binary, version string, source buildversion.BuildProvenance) (ImageMetadata, error) {
	metadata := ImageMetadata{Schema: 1, BinarySHA256: binary, Version: version,
		Source: source, ChannelSchema: SchemaVersion}
	return metadata, metadata.Validate()
}

func ParseImageMetadata(raw []byte) (ImageMetadata, error) {
	if len(raw) == 0 || len(raw) > 4096 {
		return ImageMetadata{}, invalidImageMetadata()
	}
	metadata, err := jcs.Decode[ImageMetadata](raw)
	if err != nil {
		return ImageMetadata{}, invalidImageMetadata()
	}
	return metadata, metadata.Validate()
}

func (metadata ImageMetadata) Validate() error {
	encoded := strings.TrimPrefix(metadata.BinarySHA256, "sha256:")
	digest, err := hex.DecodeString(encoded)
	validDigest := strings.HasPrefix(metadata.BinarySHA256, "sha256:") && len(encoded) == 64 &&
		err == nil && len(digest) == 32 && strings.ToLower(encoded) == encoded
	if metadata.Schema != 1 || metadata.ChannelSchema < 1 ||
		len(metadata.Version) == 0 || len(metadata.Version) > 128 ||
		!validDigest ||
		metadata.Source.Validate() != nil {
		return invalidImageMetadata()
	}
	return nil
}

func invalidImageMetadata() error {
	return errs.New(
		errs.KindSoftwareAgentMetadataInvalid,
		"Agent build metadata is missing or invalid; rebuild this Agent image before applying it",
	)
}
