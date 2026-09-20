package backupruntime

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	BackupScheduleCursorPrefix             = "/v1/runtime/backup-schedule-cursors/"
	BackupDueOutcomePrefix                 = "/v1/runtime/backup-due/"
	backupDueRetentionIndexPrefix          = "/v1/indexes/backup-due/by-retention/"
	backupSourceTargetExclusionPrefix      = "/v1/runtime/backup-source-target-exclusions/"
	backupRunPrefix                        = "/v1/runtime/backup-runs/"
	BackupRunEnvironmentPrefix             = "/v1/indexes/backup-runs/by-environment/"
	backupRecoveryPointPrefix              = "/v1/records/recovery-points/"
	BackupRecoveryPointEnvironmentPrefix   = "/v1/indexes/recovery-points/by-environment/"
	BackupRecoveryPointSourcePrefix        = "/v1/indexes/recovery-points/by-source/"
	BackupRecoveryPointConnectorPrefix     = "/v1/indexes/recovery-points/by-connector/"
	backupOrphanPrefix                     = "/v1/runtime/backup-orphans/"
	BackupOrphanConnectorPrefix            = "/v1/indexes/backup-orphans/by-connector/"
	BackupOrphanEnvironmentPrefix          = "/v1/indexes/backup-orphans/by-environment/"
	BackupRetentionPrefix                  = "/v1/runtime/backup-retention/"
	backupRecoveryPointPrunePrefix         = "/v1/runtime/recovery-point-prunes/"
	backupRecoveryPointPruneDispatchPrefix = "/v1/runtime/recovery-point-prune-dispatches/"
	backupRestorePrefix                    = "/v1/runtime/backup-restores/"
	BackupRestoreEnvironmentPrefix         = "/v1/indexes/backup-restores/by-environment/"
	backupRestoreServicePrefix             = "/v1/runtime/backup-restore-services/"
	backupKeyRotationPrefix                = "/v1/runtime/backup-key-rotations/"
	BackupKeyRotationEnvironmentPrefix     = "/v1/indexes/backup-key-rotations/by-environment/"
	backupRuntimeOrderedSegmentWidth       = 20
	backupRecoveryPointULIDAlphabet        = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
)

func backupScheduleCursorKey(environmentID string, policyRevision int64) (string, error) {
	orderedRevision, err := backupRuntimeOrderedPositiveInt64(policyRevision)
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil || err != nil {
		return "", errs.New(errs.KindValidationFailed, "backup schedule cursor key is invalid")
	}
	return BackupScheduleCursorPrefix + environmentID + "/" + orderedRevision, nil
}

func BackupDueOutcomeKey(
	environmentID string,
	policyRevision int64,
	scheduledAt time.Time,
) (string, error) {
	orderedRevision, err := backupRuntimeOrderedPositiveInt64(policyRevision)
	if err != nil {
		return "", err
	}
	orderedSchedule, err := backupRuntimeOrderedInstant(scheduledAt)
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil || err != nil {
		return "", errs.New(errs.KindValidationFailed, "backup due key is invalid")
	}
	return BackupDueOutcomePrefix + environmentID + "/" + orderedRevision + "/" + orderedSchedule, nil
}

func BackupDueRetentionIndexKey(
	retainUntil time.Time,
	environmentID string,
	policyRevision int64,
	scheduledAt time.Time,
) (string, error) {
	orderedRetention, err := backupRuntimeOrderedInstant(retainUntil)
	if err != nil {
		return "", err
	}
	orderedRevision, err := backupRuntimeOrderedPositiveInt64(policyRevision)
	if err != nil {
		return "", err
	}
	orderedSchedule, err := backupRuntimeOrderedInstant(scheduledAt)
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil || err != nil {
		return "", errs.New(errs.KindValidationFailed, "backup due retention key is invalid")
	}
	return backupDueRetentionIndexPrefix + orderedRetention + "/" + environmentID + "/" + orderedRevision +
		"/" + orderedSchedule, nil
}

func BackupSourceTargetExclusionKey(kind BackupSourceTargetKind, stableID string) (string, error) {
	if validateBackupSourceTargetIdentity(kind, stableID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup source-target exclusion key is invalid",
		)
	}
	return backupSourceTargetExclusionPrefix + string(kind) + "/" + stableID, nil
}

func BackupRunKey(taskID string) string {
	return backupRunPrefix + taskID
}

func BackupRunEnvironmentIndexKey(environmentID string, taskID string) (string, error) {
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup run environment index key is invalid",
		)
	}
	return BackupRunEnvironmentPrefix + environmentID + "/" + taskID, nil
}

func BackupRecoveryPointKey(recoveryPointID string) string {
	return backupRecoveryPointPrefix + recoveryPointID
}

func BackupRecoveryPointEnvironmentIndexKey(
	environmentID string,
	recoveryPointID string,
) (string, error) {
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup recovery point environment index key is invalid",
		)
	}
	inverted, err := invertedBackupRecoveryPointID(recoveryPointID)
	if err != nil {
		return "", err
	}
	return BackupRecoveryPointEnvironmentPrefix + environmentID + "/" + inverted, nil
}

func BackupRecoveryPointIDFromEnvironmentIndexKey(environmentID string, key string) (string, error) {
	prefix := BackupRecoveryPointEnvironmentPrefix + environmentID + "/"
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil || !strings.HasPrefix(key, prefix) {
		return "", errs.New(errs.KindInternal, "backup recovery point continuation key is invalid")
	}
	body, ok := InvertBackupRecoveryPointULIDBody(strings.TrimPrefix(key, prefix))
	if !ok {
		return "", errs.New(errs.KindInternal, "backup recovery point continuation key is invalid")
	}
	pointID := string(ids.KindRecoveryPoint) + "_" + body
	if recordcodec.ValidateID(ids.KindRecoveryPoint, pointID) != nil {
		return "", errs.New(errs.KindInternal, "backup recovery point continuation key is invalid")
	}
	return pointID, nil
}

func BackupRecoveryPointSourceIndexKey(sourceID string, recoveryPointID string) (string, error) {
	if recordcodec.ValidateID(ids.KindBackupSource, sourceID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup recovery point source index key is invalid",
		)
	}
	inverted, err := invertedBackupRecoveryPointID(recoveryPointID)
	if err != nil {
		return "", err
	}
	return BackupRecoveryPointSourcePrefix + sourceID + "/" + inverted, nil
}

func BackupRecoveryPointConnectorIndexKey(
	connectorID string,
	recoveryPointID string,
) (string, error) {
	if recordcodec.ValidateID(ids.KindConnector, connectorID) != nil ||
		recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup recovery point connector index key is invalid",
		)
	}
	return BackupRecoveryPointConnectorPrefix + connectorID + "/" + recoveryPointID, nil
}

func BackupOrphanKey(recoveryPointID string) string {
	return backupOrphanPrefix + recoveryPointID
}

func BackupOrphanConnectorIndexKey(connectorID string, recoveryPointID string) (string, error) {
	if recordcodec.ValidateID(ids.KindConnector, connectorID) != nil ||
		recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup orphan connector index key is invalid",
		)
	}
	return BackupOrphanConnectorPrefix + connectorID + "/" + recoveryPointID, nil
}

func BackupOrphanEnvironmentIndexKey(
	environmentID string,
	recoveryPointID string,
) (string, error) {
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup orphan environment index key is invalid",
		)
	}
	inverted, err := invertedBackupRecoveryPointID(recoveryPointID)
	if err != nil {
		return "", err
	}
	return BackupOrphanEnvironmentPrefix + environmentID + "/" + inverted, nil
}

func BackupRetentionKey(sourceID string, triggerRecoveryPointID string) string {
	return BackupRetentionPrefix + sourceID + "/" + triggerRecoveryPointID
}

func BackupRecoveryPointPruneKey(recoveryPointID string) string {
	return backupRecoveryPointPrunePrefix + recoveryPointID
}

func BackupRecoveryPointPruneDispatchKey(taskID string) string {
	return backupRecoveryPointPruneDispatchPrefix + taskID
}

func backupRestoreKey(taskID string) string {
	return backupRestorePrefix + taskID
}

func backupRestoreEnvironmentIndexKey(environmentID string, taskID string) (string, error) {
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup restore environment index key is invalid",
		)
	}
	return BackupRestoreEnvironmentPrefix + environmentID + "/" + taskID, nil
}

func backupRestoreServiceKey(taskID string, ordinal uint32) string {
	return backupRestoreServicePrefix + taskID + "/" + backupRuntimeOrderedUint32(ordinal)
}

func BackupKeyRotationKey(taskID string) string {
	return backupKeyRotationPrefix + taskID
}

func BackupKeyRotationEnvironmentIndexKey(environmentID string, taskID string) (string, error) {
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup key rotation environment index key is invalid",
		)
	}
	return BackupKeyRotationEnvironmentPrefix + environmentID + "/" + taskID, nil
}

func backupRuntimeOrderedPositiveInt64(value int64) (string, error) {
	if value <= 0 {
		return "", errs.New(errs.KindValidationFailed, "backup ordered integer must be positive")
	}
	return fmt.Sprintf("%0*d", backupRuntimeOrderedSegmentWidth, value), nil
}

func backupRuntimeOrderedInstant(value time.Time) (string, error) {
	if !ValidBackupRuntimeInstant(value) {
		return "", errs.New(errs.KindValidationFailed, "backup ordered timestamp is invalid")
	}
	return backupRuntimeOrderedPositiveInt64(value.UnixNano())
}

func backupRuntimeOrderedUint32(value uint32) string {
	return fmt.Sprintf("%0*d", backupRuntimeOrderedSegmentWidth, value)
}

func invertedBackupRecoveryPointID(recoveryPointID string) (string, error) {
	if err := recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID); err != nil {
		return "", err
	}
	body := strings.TrimPrefix(recoveryPointID, string(ids.KindRecoveryPoint)+"_")
	inverted, ok := InvertBackupRecoveryPointULIDBody(body)
	if !ok {
		return "", errs.New(
			errs.KindValidationFailed,
			"recovery point id has a non-canonical ULID body",
		)
	}
	return inverted, nil
}

func InvertBackupRecoveryPointULIDBody(body string) (string, bool) {
	if len(body) != 26 {
		return "", false
	}
	inverted := make([]byte, len(body))
	for index := range len(body) {
		position := strings.IndexByte(backupRecoveryPointULIDAlphabet, body[index])
		if position < 0 {
			return "", false
		}
		inverted[index] = backupRecoveryPointULIDAlphabet[len(backupRecoveryPointULIDAlphabet)-1-position]
	}
	return string(inverted), true
}
