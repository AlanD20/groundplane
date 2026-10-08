package backupobject

import (
	"context"
	"crypto/sha256"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const artifactObjectName = "artifact.bin"

// PruneAuthority is the complete Controller-sealed authority for removing one
// Backup artifact. The selected provider identity is sealed before assignment;
// HEAD proves this exact object and cannot select the latest key's replacement.
type PruneAuthority struct {
	Key             string
	EnvironmentID   string
	SourceID        string
	RecoveryPointID string
	Evidence        Evidence
	Discriminator   Discriminator
	MetadataCount   uint32
	MetadataSHA256  [sha256.Size]byte
}

func (authority PruneAuthority) Validate(prefix string) error {
	if ids.Validate(ids.KindEnvironment, authority.EnvironmentID) != nil ||
		ids.Validate(ids.KindBackupSource, authority.SourceID) != nil ||
		ids.Validate(ids.KindRecoveryPoint, authority.RecoveryPointID) != nil ||
		authority.Evidence.SourceSizeBytes == 0 || authority.Evidence.StoredSizeBytes == 0 ||
		authority.Evidence.StoredSizeBytes > MaxObjectSize || authority.Discriminator.Validate() != nil ||
		!ValidMetadataCount(authority.MetadataCount) {
		return errs.New(errs.KindValidationFailed, "backup prune authority is invalid")
	}
	parts := []string{authority.EnvironmentID, authority.SourceID, authority.RecoveryPointID, artifactObjectName}
	if cleanPrefix := strings.Trim(prefix, "/"); cleanPrefix != "" {
		parts = append([]string{cleanPrefix}, parts...)
	}
	if authority.Key != strings.Join(parts, "/") {
		return errs.New(errs.KindValidationFailed, "backup prune object key is outside its sealed identity")
	}
	return nil
}

type Pruner interface {
	PruneExact(context.Context, PruneAuthority) error
}
