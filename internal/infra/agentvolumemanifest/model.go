// Package agentvolumemanifest owns the Agent's private durable RESTORE_NEW
// Volume manifest receiver. It retains only authenticated metadata, not file
// content or a replacement archive.
package agentvolumemanifest

import (
	"crypto/sha256"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/oklog/ulid/v2"
)

const (
	AgentRoot           = agentprotocol.StatePath + "/volume-manifests"
	MaximumJournalBytes = backupvolume.RestoreHeadroom
	MaximumJournals     = 1024
	maximumAuthority    = 4096
	maximumTransaction  = backupvolumetransfer.MaxFrameBytes + 2048
)

type Config struct {
	Root                string
	Binding             backupvolumetransfer.Binding
	PointID             string
	RestoreGenerationID string
	ExpectedArchive     backupvolume.ArchiveEvidence
	ExpectedSource      backupformat.Evidence
}

// Recovered inventories one exact transfer namespace. Every row, including a
// complete one, blocks Ready until its assignment is disposed or recovered.
type Recovered struct {
	DirectoryName string
	Config        Config
	CurrentCredit *agentpb.BackupVolumeManifestAckCredit
	Complete      bool
	Partial       bool
	Unbound       bool
	Retiring      bool
	Discarding    bool
}

func (config Config) validate() error {
	if config.Root != AgentRoot ||
		config.Binding.Validate() != nil ||
		config.Binding.Direction != agentpb.BackupVolumeManifestDirection_BACKUP_VOLUME_MANIFEST_DIRECTION_RESTORE_NEW ||
		ids.Validate(ids.KindRecoveryPoint, config.PointID) != nil ||
		len(config.RestoreGenerationID) != 26 ||
		config.RestoreGenerationID != strings.ToUpper(config.RestoreGenerationID) ||
		config.ExpectedArchive.ContentManifestSHA256 == ([sha256.Size]byte{}) ||
		config.ExpectedArchive.FullTreeSHA256 == ([sha256.Size]byte{}) ||
		config.ExpectedSource.SHA256 == ([sha256.Size]byte{}) {
		return invalid("Volume manifest receiver authority is invalid")
	}
	if _, err := ulid.ParseStrict(config.RestoreGenerationID); err != nil {
		return invalid("Volume manifest receiver generation is invalid")
	}
	if err := (backupvolume.ArtifactEvidence{Source: config.ExpectedSource,
		Archive: config.ExpectedArchive}).Validate(); err != nil {
		return err
	}
	return nil
}

func invalid(message string) error  { return errs.New(errs.KindValidationFailed, message) }
func conflict(message string) error { return errs.New(errs.KindStateConflict, message) }
func storage(err error) error {
	if err == nil {
		return nil
	}
	return errs.Wrap(errs.KindStorageUnavailable, err)
}
