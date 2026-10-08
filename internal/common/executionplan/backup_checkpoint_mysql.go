package executionplan

import (
	"bytes"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func validBackupMySQLObservation(value *agentpb.BackupMySQLContainerObserved) bool {
	return value != nil && backupCheckpointServiceID(value.ServiceId) && backupCheckpointHexID(value.ContainerId) &&
		backupCheckpointDigest(value.ImageReferenceSha256) && value.ObservedLabelCount > 0 &&
		backupCheckpointDigest(value.ObservedLabelsSha256) && backupCheckpointDigest(value.ObservationSha256) &&
		backupCheckpointDigest(value.DatabaseImageIdSha256)
}

func validBackupMySQLDump(value *agentpb.BackupMySQLDumpStart) bool {
	return value != nil && backupCheckpointPoint(value.PointId) && backupCheckpointDigest(value.ExecutionNonce) &&
		backupCheckpointHexID(value.ContainerId) && backupCheckpointHexID(value.ExecId) &&
		backupCheckpointDigest(value.ImageReferenceSha256) && backupCheckpointDigest(value.ExpectedLabelsSha256) &&
		value.AdapterContractVersion == mysql84protocol.AdapterContractVersion &&
		mysql84protocol.ValidGeneratedIdentity(
			value.DatabaseName,
		) && mysql84protocol.ValidGeneratedIdentity(value.RoleName) &&
		validBackupCheckpointMySQLArchive(value.Archive) &&
		(value.MaxPlaintextBytes == backupformat.MaxStoredBytes || value.MaxPlaintextBytes == backupformat.MaxAgeSourceBytes)
}

func validBackupMySQLApply(value *agentpb.BackupMySQLRestoreApplyStartCheckpoint) bool {
	return value != nil && backupCheckpointPoint(value.PointId) && backupCheckpointDigest(value.ExecutionNonce) &&
		backupCheckpointHexID(value.ContainerId) && backupCheckpointHexID(value.ExecId) &&
		backupCheckpointDigest(value.ImageReferenceSha256) && backupCheckpointDigest(value.ExpectedLabelsSha256) &&
		value.SourceSizeBytes > 0 && value.SourceSizeBytes <= backupformat.MaxStoredBytes &&
		backupCheckpointDigest(value.SourceSha256)
}

func validBackupMySQLService(value *agentpb.BackupMySQLServiceProgress) bool {
	return value != nil && backupCheckpointServiceID(value.ServiceId) &&
		backupCheckpointServicePhase(value.Phase) && backupCheckpointDigest(value.ObservationSha256)
}

func mysqlObservationMatches(value *agentpb.BackupMySQLContainerObserved, step *agentpb.BackupStepAuthority) bool {
	if !validBackupMySQLObservation(value) {
		return false
	}
	mysql := step.GetCapture().GetMysql()
	if restore := step.GetRestore().GetMysql(); restore != nil {
		mysql = &agentpb.BackupMySQLCaptureAuthority{DatabaseServiceId: restore.DatabaseServiceId,
			DatabaseImageReferenceSha256: restore.DatabaseImageReferenceSha256}
	}
	return mysql != nil && value.ServiceId == mysql.DatabaseServiceId &&
		bytes.Equal(value.ImageReferenceSha256, mysql.DatabaseImageReferenceSha256)
}

func mysqlObservationMatchesService(value *agentpb.BackupMySQLContainerObserved, step *agentpb.BackupStepAuthority,
	services []*agentpb.BackupServiceFact,
) bool {
	if !mysqlObservationMatches(value, step) {
		return false
	}
	for _, fact := range services {
		if fact.ServiceId == value.ServiceId && fact.RequiredLabelCount == value.ObservedLabelCount &&
			bytes.Equal(fact.RequiredLabelsSha256, value.ObservedLabelsSha256) &&
			bytes.Equal(fact.LocalImageIdSha256, value.DatabaseImageIdSha256) {
			return true
		}
	}
	return false
}

func mysqlDumpMatches(value *agentpb.BackupMySQLDumpStart, step *agentpb.BackupStepAuthority) bool {
	mysql := step.GetCapture().GetMysql()
	return mysql != nil && validBackupMySQLDump(value) && value.PointId == step.GetCapture().PointId &&
		value.AdapterContractVersion == mysql.AdapterContractVersion && value.DatabaseName == mysql.DatabaseName &&
		value.RoleName == mysql.RoleName && value.MaxPlaintextBytes == mysql.MaxPlaintextBytes &&
		bytes.Equal(value.ImageReferenceSha256, mysql.DatabaseImageReferenceSha256)
}

func (replay *BackupStepResumeReplay) mysqlCaptureCheckpoint(request *agentpb.BackupCheckpointRequest) error {
	if replay.step.GetCapture().GetMysql() == nil || replay.prepared != nil || replay.uploaded != nil {
		return backupResumeHistoryInvalid()
	}
	if value := request.GetMysqlContainerObserved(); value != nil {
		if replay.sequence != 0 || replay.mysqlObserved != nil ||
			!mysqlObservationMatchesService(value, replay.step, replay.authority.Services) {
			return backupResumeHistoryInvalid()
		}
		replay.mysqlObserved = value
		replay.capture.Checkpoint = &agentpb.BackupCaptureResume_MysqlContainerObserved{MysqlContainerObserved: value}
		return nil
	}
	value, observed := request.GetMysqlDumpStart(), replay.mysqlObserved
	if observed == nil || replay.mysqlDumpStart != nil || !mysqlDumpMatches(value, replay.step) ||
		value.ContainerId != observed.ContainerId ||
		!bytes.Equal(value.ImageReferenceSha256, observed.ImageReferenceSha256) ||
		!bytes.Equal(value.ExpectedLabelsSha256, observed.ObservedLabelsSha256) {
		return backupResumeHistoryInvalid()
	}
	replay.mysqlDumpStart = value
	replay.capture.MysqlDumpStart = value
	replay.capture.Checkpoint = &agentpb.BackupCaptureResume_MysqlDumpStartCheckpoint{MysqlDumpStartCheckpoint: value}
	return nil
}
