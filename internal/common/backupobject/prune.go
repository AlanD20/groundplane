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
// Backup artifact. It deliberately excludes provider-specific object identity:
// the adapter discovers that identity with an exact HEAD before deleting.
type PruneAuthority struct {
	Key             string
	EnvironmentID   string
	SourceID        string
	RecoveryPointID string
	StoredSizeBytes uint64
	StoredSHA256    [sha256.Size]byte
}

func (authority PruneAuthority) Validate(prefix string) error {
	if ids.Validate(ids.KindEnvironment, authority.EnvironmentID) != nil ||
		ids.Validate(ids.KindBackupSource, authority.SourceID) != nil ||
		ids.Validate(ids.KindRecoveryPoint, authority.RecoveryPointID) != nil ||
		authority.StoredSizeBytes == 0 {
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
