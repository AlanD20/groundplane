package backupwork

import (
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func projectCaptureUpload(source *backupruntime.BackupRunSourceAttemptRecord, cursor *agentpb.BackupCaptureResume) error {
	if completed := cursor.GetUploadCompleted(); completed != nil {
		source.Upload.Target = captureObjectTarget(completed.Target)
		switch {
		case completed.GetReturnedObject() != nil:
			source.Upload.Kind = backupruntime.BackupUploadReturned
			source.Upload.ReturnedObject = captureObjectIdentity(completed.GetReturnedObject())
		case completed.GetUnknown() != nil:
			source.Upload.Kind = backupruntime.BackupUploadUnknown
			source.Upload.ReturnedObject = backupruntime.BackupObjectIdentity{}
		default:
			return conflict("Backup upload outcome is missing")
		}
		source.Phase = backupruntime.BackupSourcePhaseHeadVerification
		return nil
	}
	if verified := cursor.GetUploadVerified(); verified != nil {
		source.Object = captureObjectIdentity(verified.Object)
		source.Phase = backupruntime.BackupSourcePhasePointCommit
		return nil
	}
	return conflict("Backup capture report has no resource progress transition")
}

func captureObjectTarget(value *agentpb.BackupObjectTarget) backupruntime.BackupObjectTarget {
	return backupruntime.BackupObjectTarget{ConnectorID: value.Connector.ConnectorId,
		ConnectorPrefix: value.Connector.Prefix, ConnectorEndpoint: value.Connector.CanonicalEndpointUrl,
		ConnectorBucket: value.Bucket, ConnectorRegion: value.Connector.Region,
		ConnectorPathStyle: value.Connector.GetPathStyle(), ObjectKey: value.ObjectKey}
}

func captureObjectIdentity(value *agentpb.BackupObjectIdentity) backupruntime.BackupObjectIdentity {
	result := backupruntime.BackupObjectIdentity{Target: captureObjectTarget(&agentpb.BackupObjectTarget{
		Connector: value.Connector, Bucket: value.Bucket, ObjectKey: value.ObjectKey})}
	if version := value.GetVersionId(); version != nil {
		result.Discriminator = backupruntime.BackupObjectDiscriminator{Kind: backupobject.DiscriminatorVersionID, Value: version.Value}
	} else {
		result.Discriminator = backupruntime.BackupObjectDiscriminator{Kind: backupobject.DiscriminatorETag, Value: value.GetEtag().GetValue()}
	}
	return result
}
