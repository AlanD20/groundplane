package executionplan

import (
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/oklog/ulid/v2"
)

// BackupSourceStagingKey identifies captured source files independently of
// diagnostic steps or attempt retries. Execution IDs are immutable source IDs.
func BackupSourceStagingKey(taskID, executionID, pointID string) ([]byte, error) {
	if ids.Validate(ids.KindTask, taskID) != nil || ids.Validate(ids.KindRecoveryPoint, pointID) != nil ||
		len(executionID) != 26 {
		return nil, invalidBackupStaging()
	}
	if _, err := ulid.ParseStrict(executionID); err != nil {
		return nil, invalidBackupStaging()
	}
	digest := sha256.Sum256(
		[]byte("groundplane.backup.source-staging-key.v1\x00" + taskID + "/" + executionID + "/" + pointID),
	)
	return digest[:], nil
}
