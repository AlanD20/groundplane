package etcd

import (
	"fmt"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	backupScheduleCursorPrefix             = "/v1/runtime/backup-schedule-cursors/"
	backupDueOutcomePrefix                 = "/v1/runtime/backup-due/"
	backupDueRetentionIndexPrefix          = "/v1/indexes/backup-due/by-retention/"
	backupSourceTargetExclusionPrefix      = "/v1/runtime/backup-source-target-exclusions/"
	backupRunPrefix                        = "/v1/runtime/backup-runs/"
	backupRunEnvironmentPrefix             = "/v1/indexes/backup-runs/by-environment/"
	backupRecoveryPointPrefix              = "/v1/records/recovery-points/"
	backupRecoveryPointEnvironmentPrefix   = "/v1/indexes/recovery-points/by-environment/"
	backupRecoveryPointSourcePrefix        = "/v1/indexes/recovery-points/by-source/"
	backupRecoveryPointConnectorPrefix     = "/v1/indexes/recovery-points/by-connector/"
	backupOrphanPrefix                     = "/v1/runtime/backup-orphans/"
	backupOrphanConnectorPrefix            = "/v1/indexes/backup-orphans/by-connector/"
	backupOrphanEnvironmentPrefix          = "/v1/indexes/backup-orphans/by-environment/"
	backupRetentionPrefix                  = "/v1/runtime/backup-retention/"
	backupRecoveryPointPrunePrefix         = "/v1/runtime/recovery-point-prunes/"
	backupRecoveryPointPruneDispatchPrefix = "/v1/runtime/recovery-point-prune-dispatches/"
	backupRestorePrefix                    = "/v1/runtime/backup-restores/"
	backupRestoreEnvironmentPrefix         = "/v1/indexes/backup-restores/by-environment/"
	backupRestoreServicePrefix             = "/v1/runtime/backup-restore-services/"
	backupKeyRotationPrefix                = "/v1/runtime/backup-key-rotations/"
	backupKeyRotationEnvironmentPrefix     = "/v1/indexes/backup-key-rotations/by-environment/"
	backupRuntimeOrderedSegmentWidth       = 20
	backupRecoveryPointULIDAlphabet        = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
)

func backupScheduleCursorKey(environmentID string, policyRevision int64) (string, error) {
	orderedRevision, err := backupRuntimeOrderedPositiveInt64(policyRevision)
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil || err != nil {
		return "", errs.New(errs.KindValidationFailed, "backup schedule cursor key is invalid")
	}
	return backupScheduleCursorPrefix + environmentID + "/" + orderedRevision, nil
}

func backupDueOutcomeKey(
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
	return backupDueOutcomePrefix + environmentID + "/" + orderedRevision + "/" + orderedSchedule, nil
}

func backupDueRetentionIndexKey(
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

func backupSourceTargetExclusionKey(kind BackupSourceTargetKind, stableID string) (string, error) {
	if validateBackupSourceTargetIdentity(kind, stableID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup source-target exclusion key is invalid",
		)
	}
	return backupSourceTargetExclusionPrefix + string(kind) + "/" + stableID, nil
}

func backupRunKey(taskID string) string {
	return backupRunPrefix + taskID
}

func backupRunEnvironmentIndexKey(environmentID string, taskID string) (string, error) {
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup run environment index key is invalid",
		)
	}
	return backupRunEnvironmentPrefix + environmentID + "/" + taskID, nil
}

func backupRecoveryPointKey(recoveryPointID string) string {
	return backupRecoveryPointPrefix + recoveryPointID
}

func backupRecoveryPointEnvironmentIndexKey(
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
	return backupRecoveryPointEnvironmentPrefix + environmentID + "/" + inverted, nil
}

func backupRecoveryPointIDFromEnvironmentIndexKey(environmentID string, key string) (string, error) {
	prefix := backupRecoveryPointEnvironmentPrefix + environmentID + "/"
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil || !strings.HasPrefix(key, prefix) {
		return "", errs.New(errs.KindInternal, "backup recovery point continuation key is invalid")
	}
	body, ok := invertBackupRecoveryPointULIDBody(strings.TrimPrefix(key, prefix))
	if !ok {
		return "", errs.New(errs.KindInternal, "backup recovery point continuation key is invalid")
	}
	pointID := string(ids.KindRecoveryPoint) + "_" + body
	if recordcodec.ValidateID(ids.KindRecoveryPoint, pointID) != nil {
		return "", errs.New(errs.KindInternal, "backup recovery point continuation key is invalid")
	}
	return pointID, nil
}

func backupRecoveryPointSourceIndexKey(sourceID string, recoveryPointID string) (string, error) {
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
	return backupRecoveryPointSourcePrefix + sourceID + "/" + inverted, nil
}

func backupRecoveryPointConnectorIndexKey(
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
	return backupRecoveryPointConnectorPrefix + connectorID + "/" + recoveryPointID, nil
}

func backupOrphanKey(recoveryPointID string) string {
	return backupOrphanPrefix + recoveryPointID
}

func backupOrphanConnectorIndexKey(connectorID string, recoveryPointID string) (string, error) {
	if recordcodec.ValidateID(ids.KindConnector, connectorID) != nil ||
		recordcodec.ValidateID(ids.KindRecoveryPoint, recoveryPointID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup orphan connector index key is invalid",
		)
	}
	return backupOrphanConnectorPrefix + connectorID + "/" + recoveryPointID, nil
}

func backupOrphanEnvironmentIndexKey(
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
	return backupOrphanEnvironmentPrefix + environmentID + "/" + inverted, nil
}

func backupRetentionKey(sourceID string, triggerRecoveryPointID string) string {
	return backupRetentionPrefix + sourceID + "/" + triggerRecoveryPointID
}

func backupRecoveryPointPruneKey(recoveryPointID string) string {
	return backupRecoveryPointPrunePrefix + recoveryPointID
}

func backupRecoveryPointPruneDispatchKey(taskID string) string {
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
	return backupRestoreEnvironmentPrefix + environmentID + "/" + taskID, nil
}

func backupRestoreServiceKey(taskID string, ordinal uint32) string {
	return backupRestoreServicePrefix + taskID + "/" + backupRuntimeOrderedUint32(ordinal)
}

func backupKeyRotationKey(taskID string) string {
	return backupKeyRotationPrefix + taskID
}

func backupKeyRotationEnvironmentIndexKey(environmentID string, taskID string) (string, error) {
	if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return "", errs.New(
			errs.KindValidationFailed,
			"backup key rotation environment index key is invalid",
		)
	}
	return backupKeyRotationEnvironmentPrefix + environmentID + "/" + taskID, nil
}

func backupRuntimeOrderedPositiveInt64(value int64) (string, error) {
	if value <= 0 {
		return "", errs.New(errs.KindValidationFailed, "backup ordered integer must be positive")
	}
	return fmt.Sprintf("%0*d", backupRuntimeOrderedSegmentWidth, value), nil
}

func backupRuntimeOrderedInstant(value time.Time) (string, error) {
	if !validBackupRuntimeInstant(value) {
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
	inverted, ok := invertBackupRecoveryPointULIDBody(body)
	if !ok {
		return "", errs.New(
			errs.KindValidationFailed,
			"recovery point id has a non-canonical ULID body",
		)
	}
	return inverted, nil
}

func invertBackupRecoveryPointULIDBody(body string) (string, bool) {
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
