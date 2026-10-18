package executionplan

import (
	"bytes"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func backupCheckpointFence(fence *agentpb.CheckpointFence, authority []byte) bool {
	return fence != nil && fence.DedupeKeyModRevision > 0 &&
		backupCheckpointDigest(fence.AuthorityDigest) && bytes.Equal(fence.AuthorityDigest, authority)
}

func backupCheckpointDigest(value []byte) bool { return len(value) == sha256.Size }

func backupCheckpointPoint(value string) bool {
	return ids.Validate(ids.KindRecoveryPoint, value) == nil
}

func invalidBackupCheckpoint(message string) error {
	return errs.New(errs.KindValidationFailed, "backup checkpoint "+message)
}
