//go:build linux

package backupstage

import (
	"context"

	"golang.org/x/sys/unix"
)

// DiscardResumedFile retires one exact derived artifact under the startup
// recovery lock. The caller's durable disposition, not inventory alone,
// authorizes this removal. It cannot delete an unclassified stage or source.
func (session *RecoverySession) DiscardResumedFile(ctx context.Context, recoveryID RecoveryID,
	expected ArtifactEvidence,
) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	if session == nil {
		return internalError("recovery session is required")
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.completed || (expected.Name != "stored" && expected.Name != ".stored.partial") {
		return stateConflictError("derived file retirement is outside startup recovery")
	}
	for _, prepared := range session.prepared {
		if prepared.RecoveryID != recoveryID {
			continue
		}
		stage := prepared.Stage
		stage.mu.Lock()
		defer stage.mu.Unlock()
		for index, file := range prepared.Files {
			if file.Evidence != expected {
				continue
			}
			artifact := file.Artifact
			if stage.closed || stage.cleaned || artifact.file == nil || len(artifact.readers) != 0 {
				return stateConflictError("derived recovered artifact is not available")
			}
			actual, _, err := evidenceAndHashFromFD(ctx, expected.Name, int(artifact.file.Fd()))
			if err != nil {
				return err
			}
			if actual != expected {
				return stateConflictError("derived recovered artifact changed before retirement")
			}
			var pinned, named unix.Stat_t
			if err := unix.Fstat(int(artifact.file.Fd()), &pinned); err != nil {
				return systemError("inspect derived artifact descriptor", err)
			}
			if err := unix.Fstatat(stage.pointFD, expected.Name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
				return systemError("inspect derived artifact name", err)
			}
			if pinned.Dev != named.Dev || pinned.Ino != named.Ino {
				return stateConflictError("derived artifact namespace changed before retirement")
			}
			if err := unix.Unlinkat(stage.pointFD, expected.Name, 0); err != nil {
				return systemError("retire derived artifact", err)
			}
			if err := syncFD(ctx, stage.pointFD); err != nil {
				return err
			}
			closeErr := rawOperationError("close derived artifact", artifact.file.Close())
			artifact.file, artifact.state = nil, artifactRemoved
			delete(stage.artifacts, artifact)
			prepared.Files = append(prepared.Files[:index], prepared.Files[index+1:]...)
			return joinPrivate(closeErr, artifact.sealGrowthLocked(ctx))
		}
		// A replay after successful deletion has no corresponding descriptor.
		return syncFD(ctx, stage.pointFD)
	}
	return stateConflictError("derived artifact stage has not been resumed")
}
