package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/backupobject"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"strings"
)

// BackupPruneObjectAuthority selects only the exact captured object, never a bucket listing.
func BackupPruneObjectAuthority(
	environmentID string,
	object *agentpb.BackupPruneObject,
) (backupobject.PruneAuthority, error) {
	if object == nil || object.Object == nil || object.Object.Connector == nil || object.Evidence == nil {
		return backupobject.PruneAuthority{}, errs.New(errs.KindValidationFailed, "Backup prune object is missing")
	}
	if len(object.Evidence.SourceSha256) != 32 || len(object.Evidence.StoredSha256) != 32 ||
		len(object.MetadataSha256) != 32 {
		return backupobject.PruneAuthority{}, errs.New(
			errs.KindValidationFailed,
			"Backup prune object evidence is incomplete",
		)
	}
	key := object.Object.ObjectKey
	prefix := strings.Trim(object.Object.Connector.Prefix, "/")
	if prefix != "" {
		key = strings.TrimPrefix(key, prefix+"/")
	}
	parts := strings.Split(key, "/")
	if len(parts) != 4 || parts[0] != environmentID || parts[2] != object.PointId {
		return backupobject.PruneAuthority{}, errs.New(
			errs.KindValidationFailed,
			"Backup prune object scope is invalid",
		)
	}
	authority := backupobject.PruneAuthority{
		Key:             object.Object.ObjectKey,
		EnvironmentID:   environmentID,
		SourceID:        parts[1],
		RecoveryPointID: object.PointId,
		Evidence: backupobject.Evidence{
			SourceSizeBytes: object.Evidence.SourceSizeBytes,
			StoredSizeBytes: object.Evidence.StoredSizeBytes,
		},
		MetadataCount: object.MetadataCount,
	}
	copy(authority.Evidence.SourceSHA256[:], object.Evidence.SourceSha256)
	copy(authority.Evidence.StoredSHA256[:], object.Evidence.StoredSha256)
	copy(authority.MetadataSHA256[:], object.MetadataSha256)
	switch discriminator := object.Object.Discriminator.(type) {
	case *agentpb.BackupObjectIdentity_VersionId:
		authority.Discriminator = backupobject.Discriminator{Kind: backupobject.DiscriminatorVersionID, Value: discriminator.VersionId.Value}
	case *agentpb.BackupObjectIdentity_Etag:
		authority.Discriminator = backupobject.Discriminator{Kind: backupobject.DiscriminatorETag, Value: discriminator.Etag.Value}
	}
	return authority, authority.Validate(object.Object.Connector.Prefix)
}
