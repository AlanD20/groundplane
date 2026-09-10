// Package controllerupgrade defines the closed native release and recovery
// records shared by the Controller coordinator and host infrastructure.
package controllerupgrade

import (
	"crypto/sha256"
	"fmt"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	ManifestSchema         = 1
	StorageEpoch           = 1
	MaxManifestBytes       = 4096
	TaskTimeoutSeconds     = 600
	DrainTimeoutSeconds    = 120
	RecoveryReserveSeconds = 150
)

// Digest is a canonical SHA256 identity, never an operator-supplied path.
type Digest string

func (digest Digest) Valid() bool {
	if len(digest) != 71 || !strings.HasPrefix(string(digest), "sha256:") {
		return false
	}
	for _, character := range digest[7:] {
		if !(character >= '0' && character <= '9' || character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func Hash(raw []byte) Digest {
	return Digest(fmt.Sprintf("sha256:%x", sha256.Sum256(raw)))
}

// Manifest is the complete immutable upgrade input. Compatibility epochs are
// explicitly advanced whenever a release can no longer roll back its writes.
type Manifest struct {
	Schema            int    `json:"schema"`
	ControllerSHA256  Digest `json:"controller_sha256"`
	ControllerVersion string `json:"controller_version"`
	AgentImage        string `json:"agent_image"`
	StorageEpoch      int    `json:"storage_epoch"`
	ChannelSchema     int    `json:"channel_schema"`
}

// ParseManifest accepts byte-exact canonical JSON so every publisher and reader
// agrees on the selected release. No candidate subprocess is needed to validate.
func ParseManifest(raw []byte, expected Digest) (Manifest, error) {
	if len(raw) == 0 || len(raw) > MaxManifestBytes || !expected.Valid() || Hash(raw) != expected {
		return Manifest{}, errs.New(
			errs.KindValidationFailed,
			"controller release manifest digest or size is invalid",
		)
	}
	manifest, err := jcs.Decode[Manifest](raw)
	if err != nil {
		return Manifest{}, err
	}
	if err := manifest.Validate(); err != nil {
		return Manifest{}, err
	}
	return manifest, nil
}

func (manifest Manifest) Validate() error {
	if manifest.Schema != ManifestSchema || manifest.StorageEpoch != StorageEpoch ||
		manifest.ChannelSchema != executionplan.SchemaVersion {
		return errs.New(
			errs.KindValidationFailed,
			"controller release has incompatible storage or protocol format",
		)
	}
	if !manifest.ControllerSHA256.Valid() || !imageref.IsDigestPinned(manifest.AgentImage) ||
		len(manifest.AgentImage) > 1024 || !validVersion(manifest.ControllerVersion) {
		return errs.New(errs.KindValidationFailed, "controller release identities are invalid")
	}
	return nil
}

func validVersion(version string) bool {
	if len(version) == 0 || len(version) > 128 {
		return false
	}
	for _, character := range version {
		if !(character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' || strings.ContainsRune("._+-", character)) {
			return false
		}
	}
	return true
}
