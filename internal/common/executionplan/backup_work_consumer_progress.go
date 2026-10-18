package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

type backupConsumerCursor struct {
	serviceID string
	cursor    uint32
	phase     agentpb.BackupServicePhase
}

// Select only the source's own checkpoint variant. A checkpoint for another
// source kind cannot authorize a consumer mutation, even if its fields match.
func backupConsumerProgress(step *agentpb.BackupStepAuthority,
	report *agentpb.BackupWorkCheckpointRequest, resume *agentpb.BackupRestoreResume,
) (backupConsumerCursor, backupConsumerCursor, error) {
	var intent, progress backupConsumerCursor
	switch {
	case step.GetRestore().GetVolume() != nil:
		i, p := report.GetVolume().GetServiceProgress(), resume.GetVolumeProgress()
		if i == nil || p == nil {
			return intent, progress, invalidBackupConsumerHelper()
		}
		intent = backupConsumerCursor{i.ServiceId, i.ServiceCursor, i.Phase}
		progress = backupConsumerCursor{cursor: p.ServiceCursor, phase: p.ServicePhase}
	case step.GetRestore().GetPostgres() != nil:
		i, p := report.GetPostgresServiceProgress(), resume.GetPostgresServiceProgress()
		if i == nil || p == nil || i.ServiceId != p.ServiceId {
			return intent, progress, invalidBackupConsumerHelper()
		}
		intent = backupConsumerCursor{i.ServiceId, i.ServiceCursor, i.Phase}
		progress = backupConsumerCursor{p.ServiceId, p.ServiceCursor, p.Phase}
	case step.GetRestore().GetMysql() != nil:
		i, p := report.GetMysqlServiceProgress(), resume.GetMysqlServiceProgress()
		if i == nil || p == nil || i.ServiceId != p.ServiceId {
			return intent, progress, invalidBackupConsumerHelper()
		}
		intent = backupConsumerCursor{i.ServiceId, i.ServiceCursor, i.Phase}
		progress = backupConsumerCursor{p.ServiceId, p.ServiceCursor, p.Phase}
	default:
		return intent, progress, invalidBackupConsumerHelper()
	}
	return intent, progress, nil
}
