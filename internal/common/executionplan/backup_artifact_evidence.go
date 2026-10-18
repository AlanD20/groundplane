package executionplan

import "github.com/AlanD20/groundplane/proto/agentpb"

// Artifact validation is independent of transport envelopes and execution
// identity. The capture owner separately binds evidence to its exact input.
func ValidateBackupPreparedArtifact(value *agentpb.BackupArtifactPrepared) error {
	if value == nil || RejectUnknown(value) != nil || !validBackupArtifactPrepared(value) {
		return invalidBackupCheckpoint("prepared artifact is invalid")
	}
	return nil
}

func ValidateBackupCompletedUpload(value *agentpb.BackupUploadCompleted) error {
	if value == nil || RejectUnknown(value) != nil || !validBackupUploadCompleted(value) {
		return invalidBackupCheckpoint("completed upload is invalid")
	}
	return nil
}

func ValidateBackupVerifiedUpload(value *agentpb.BackupUploadVerified) error {
	if value == nil || RejectUnknown(value) != nil || !backupCheckpointPoint(value.PointId) ||
		!validBackupCheckpointEvidence(value.Evidence) || !validBackupCheckpointObject(value.Object, value.PointId) ||
		!validBackupCheckpointMetadata(value.MetadataCount, value.MetadataSha256) {
		return invalidBackupCheckpoint("verified upload is invalid")
	}
	return nil
}
