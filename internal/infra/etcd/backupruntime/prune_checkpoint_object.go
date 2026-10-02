package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The sealed point retains all four evidence fields. Its deletion checkpoint
// must name that exact immutable object, never a later object at the same key.
func BackupPruneCheckpointMatchesObject(
	input BackupCheckpointInput,
	ordinal uint32,
	pointID string,
	object BackupObjectIdentity,
) bool {
	request, err := executionplan.ValidateBackupCheckpointRequest(input.Request, input.Sequence)
	if err != nil || request.TaskId != input.TaskID || request.AssignmentId != input.AssignmentID ||
		request.StepId != input.StepID || request.ExecutionId != input.ExecutionID {
		return false
	}
	deleted := request.GetPruneObjectDeleted()
	if deleted == nil || uint64(deleted.Ordinal) != uint64(ordinal)+1 || deleted.PointId != pointID {
		return false
	}
	wire := deleted.Object
	if wire == nil || wire.Connector == nil || wire.Connector.PathStyle == nil ||
		object.Target.ConnectorID != wire.Connector.ConnectorId ||
		object.Target.ConnectorPrefix != wire.Connector.Prefix ||
		object.Target.ConnectorEndpoint != wire.Connector.CanonicalEndpointUrl ||
		object.Target.ConnectorRegion != wire.Connector.Region ||
		object.Target.ConnectorPathStyle != wire.Connector.GetPathStyle() ||
		object.Target.ConnectorBucket != wire.Bucket || object.Target.ObjectKey != wire.ObjectKey {
		return false
	}
	switch discriminator := wire.Discriminator.(type) {
	case *agentpb.BackupObjectIdentity_VersionId:
		return discriminator != nil && discriminator.VersionId != nil &&
			object.Discriminator.Kind == backupobject.DiscriminatorVersionID &&
			object.Discriminator.Value == discriminator.VersionId.Value
	case *agentpb.BackupObjectIdentity_Etag:
		return discriminator != nil && discriminator.Etag != nil &&
			object.Discriminator.Kind == backupobject.DiscriminatorETag &&
			object.Discriminator.Value == discriminator.Etag.Value
	default:
		return false
	}
}
