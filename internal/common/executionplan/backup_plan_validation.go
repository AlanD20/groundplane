package executionplan

import (
	"crypto/sha256"
	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"net"
	"net/url"
	"path"
	"strings"
	"unicode/utf8"
)

func validateBackupPlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindEnvironment, plan.TargetId) != nil || len(plan.Artifacts) != 0 ||
		len(plan.Steps) == 0 || len(plan.Steps) > MaximumBackupSources {
		return errs.New(errs.KindValidationFailed, "backup execution plan shape is invalid")
	}
	stepIDs := make(map[string]struct{}, len(plan.Steps))
	sourceIDs := make(map[string]struct{}, len(plan.Steps))
	pointIDs := make(map[string]struct{}, len(plan.Steps))
	var fixed *agentpb.BackupSourceCapture
	for _, step := range plan.Steps {
		if step == nil || validateID(ids.KindStep, step.StepId) != nil || step.TimeoutSeconds == 0 {
			return errs.New(errs.KindValidationFailed, "backup execution step identity or timeout is invalid")
		}
		capture := step.GetBackupSourceCapture()
		if err := validateBackupSourceCapture(plan.TargetId, capture); err != nil {
			return err
		}
		if _, duplicate := stepIDs[step.StepId]; duplicate {
			return errs.New(errs.KindValidationFailed, "execution plan step ids must be unique")
		}
		if _, duplicate := sourceIDs[capture.SourceId]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup source ids must be unique")
		}
		if _, duplicate := pointIDs[capture.PointId]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup point ids must be unique")
		}
		stepIDs[step.StepId] = struct{}{}
		sourceIDs[capture.SourceId] = struct{}{}
		pointIDs[capture.PointId] = struct{}{}
		if fixed == nil {
			fixed = capture
			continue
		}
		if capture.ConnectorId != fixed.ConnectorId || capture.ConnectorRevision != fixed.ConnectorRevision ||
			capture.Encryption != fixed.Encryption || capture.KeyEra != fixed.KeyEra ||
			capture.AgeRecipient != fixed.AgeRecipient {
			return errs.New(errs.KindValidationFailed, "backup plan policy controls must be identical across sources")
		}
	}
	return nil
}

func validateBackupPrunePlan(plan *agentpb.ExecutionPlan) error {
	if validateID(ids.KindEnvironment, plan.TargetId) != nil || len(plan.Artifacts) != 0 ||
		len(plan.Steps) == 0 || len(plan.Steps) > MaximumBackupPrunePoints {
		return errs.New(errs.KindValidationFailed, "backup prune execution plan shape is invalid")
	}
	stepIDs := make(map[string]struct{}, len(plan.Steps))
	pointIDs := make(map[string]struct{}, len(plan.Steps))
	objectKeys := make(map[string]struct{}, len(plan.Steps))
	pruneOperationID := ""
	for index, step := range plan.Steps {
		if step == nil || validateID(ids.KindStep, step.StepId) != nil ||
			step.TimeoutSeconds != MaximumBackupPruneStepTimeoutSeconds {
			return errs.New(errs.KindValidationFailed, "backup prune step identity or timeout is invalid")
		}
		prune := step.GetBackupArtifactPrune()
		if err := validateBackupArtifactPrune(plan.TargetId, uint32(index+1), prune); err != nil {
			return err
		}
		if _, duplicate := stepIDs[step.StepId]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup prune step ids must be unique")
		}
		if _, duplicate := pointIDs[prune.PointId]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup prune point ids must be unique")
		}
		if _, duplicate := objectKeys[prune.ProtectedObjectKey]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup prune object keys must be unique")
		}
		stepIDs[step.StepId] = struct{}{}
		pointIDs[prune.PointId] = struct{}{}
		objectKeys[prune.ProtectedObjectKey] = struct{}{}
		if pruneOperationID == "" {
			pruneOperationID = prune.PruneOperationId
		} else if prune.PruneOperationId != pruneOperationID {
			return errs.New(errs.KindValidationFailed, "backup prune operation must be identical across points")
		}
	}
	return nil
}

func validateBackupArtifactPrune(
	environmentID string,
	expectedOrdinal uint32,
	prune *agentpb.BackupArtifactPrune,
) error {
	if prune == nil || prune.Ordinal != expectedOrdinal ||
		validateID(ids.KindOperation, prune.PruneOperationId) != nil || prune.PruneRevision == 0 ||
		validateID(ids.KindRecoveryPoint, prune.PointId) != nil || prune.PointRevision == 0 ||
		validateID(ids.KindBackupSource, prune.SourceId) != nil || prune.SourceRevision == 0 ||
		validateID(ids.KindEnvironment, prune.EnvironmentId) != nil || prune.EnvironmentRevision == 0 ||
		(environmentID != "" && prune.EnvironmentId != environmentID) ||
		validateID(ids.KindConnector, prune.ConnectorId) != nil || prune.ConnectorRevision == 0 ||
		!validBackupConnectorEndpoint(prune.ConnectorEndpoint) ||
		!validBackupConnectorBucket(prune.ConnectorBucket) ||
		!validBackupConnectorPrefix(prune.ConnectorPrefix) ||
		!validBackupConnectorRegion(prune.ConnectorRegion) ||
		!validBackupS3Addressing(prune.ConnectorAddressing) ||
		!validBackupPruneObjectKey(prune) || prune.StoredSizeBytes == 0 ||
		len(prune.StoredSha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "backup artifact prune control data is invalid")
	}
	return nil
}

func validBackupConnectorEndpoint(value string) bool {
	if value == "" || !utf8.ValidString(value) || len(value) > MaximumPlanBytes {
		return false
	}
	parsed, err := url.ParseRequestURI(value)
	return err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" &&
		parsed.User == nil && parsed.RawQuery == "" && parsed.Fragment == "" &&
		(parsed.Path == "" || parsed.Path == "/") && !strings.HasSuffix(value, "/")
}

func validBackupConnectorBucket(value string) bool {
	if len(value) < 3 || len(value) > 63 || net.ParseIP(value) != nil ||
		!backupConnectorBucketAlphaNumeric(value[0]) || !backupConnectorBucketAlphaNumeric(value[len(value)-1]) ||
		strings.Contains(value, "..") || strings.Contains(value, ".-") || strings.Contains(value, "-.") {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if !backupConnectorBucketAlphaNumeric(character) && character != '-' && character != '.' {
			return false
		}
	}
	return true
}

func backupConnectorBucketAlphaNumeric(character byte) bool {
	return character >= 'a' && character <= 'z' || character >= '0' && character <= '9'
}

func validBackupConnectorPrefix(value string) bool {
	if value == "" {
		return true
	}
	if len(value) > maximumBackupObjectKeyBytes || !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') ||
		strings.Contains(value, `\`) || strings.HasPrefix(value, "/") || !strings.HasSuffix(value, "/") {
		return false
	}
	withoutSlash := strings.TrimSuffix(value, "/")
	if withoutSlash == "" || path.Clean(withoutSlash) != withoutSlash {
		return false
	}
	for _, component := range strings.Split(withoutSlash, "/") {
		if component == "." || component == ".." {
			return false
		}
	}
	return true
}

func validBackupConnectorRegion(value string) bool {
	if value == "" || len(value) > MaximumPlanBytes {
		return false
	}
	for index := range len(value) {
		if value[index] < 0x21 || value[index] > 0x7e {
			return false
		}
	}
	return true
}

func validBackupS3Addressing(value agentpb.BackupS3Addressing) bool {
	return value == agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_PATH_STYLE ||
		value == agentpb.BackupS3Addressing_BACKUP_S3_ADDRESSING_VIRTUAL_HOSTED_STYLE
}

func validBackupPruneObjectKey(prune *agentpb.BackupArtifactPrune) bool {
	if prune.ProtectedObjectKey == "" || len(prune.ProtectedObjectKey) > maximumBackupObjectKeyBytes ||
		!utf8.ValidString(prune.ProtectedObjectKey) || strings.ContainsRune(prune.ProtectedObjectKey, '\x00') ||
		strings.Contains(prune.ProtectedObjectKey, `\`) || strings.HasPrefix(prune.ProtectedObjectKey, "/") {
		return false
	}
	expected := prune.ConnectorPrefix + prune.EnvironmentId + "/" + prune.SourceId + "/" + prune.PointId +
		"/artifact.bin"
	return prune.ProtectedObjectKey == expected
}

func validateBackupSourceCapture(environmentID string, capture *agentpb.BackupSourceCapture) error {
	if capture == nil || validateID(ids.KindBackupSource, capture.SourceId) != nil ||
		capture.SourceRevision == 0 || capture.TargetRevision == 0 ||
		validateID(ids.KindRecoveryPoint, capture.PointId) != nil ||
		validateID(ids.KindConnector, capture.ConnectorId) != nil || capture.ConnectorRevision == 0 {
		return errs.New(errs.KindValidationFailed, "backup source identity or revision is invalid")
	}
	if err := validateBackupUploadAuthority(environmentID, capture); err != nil {
		return err
	}
	switch capture.Encryption {
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE:
		if capture.KeyEra != 0 || capture.AgeRecipient != "" {
			return errs.New(errs.KindValidationFailed, "unencrypted backup source carries age control data")
		}
	case agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE:
		recipient, err := age.ParseX25519Recipient(capture.AgeRecipient)
		if capture.KeyEra == 0 || err != nil || recipient.String() != capture.AgeRecipient {
			return errs.New(errs.KindValidationFailed, "age-encrypted backup source control data is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup source encryption is unsupported")
	}
	switch source := capture.Source.(type) {
	case *agentpb.BackupSourceCapture_Attach:
		if source.Attach == nil || capture.SourceFormat !=
			agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_POSTGRES_CUSTOM_V1 ||
			validateID(ids.KindAttach, capture.TargetId) != nil ||
			validateID(ids.KindService, source.Attach.BackingServiceId) != nil ||
			source.Attach.BackingServiceRevision == 0 || !validAdapterIdentity(source.Attach.Database, true) ||
			!validAdapterIdentity(source.Attach.Role, true) {
			return errs.New(errs.KindValidationFailed, "postgresql backup source control data is invalid")
		}
	case *agentpb.BackupSourceCapture_Config:
		if source.Config == nil || capture.SourceFormat !=
			agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_ENVIRONMENT_CONFIG_V1 ||
			capture.TargetId != environmentID || validateID(ids.KindEnvironment, capture.TargetId) != nil ||
			source.Config.SnapshotRevision == 0 ||
			capture.Encryption != agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE {
			return errs.New(errs.KindValidationFailed, "config backup source control data is invalid")
		}
	case *agentpb.BackupSourceCapture_Volume:
		if source.Volume == nil || capture.SourceFormat !=
			agentpb.BackupSourceFormat_BACKUP_SOURCE_FORMAT_VOLUME_TAR_V1 ||
			validateID(ids.KindVolume, capture.TargetId) != nil ||
			validateID(ids.KindConfig, source.Volume.ArtifactId) != nil ||
			len(source.Volume.ArtifactSha256) != sha256.Size || source.Volume.ArtifactRevision == 0 ||
			source.Volume.ProjectionRoot == 0 || source.Volume.RenderGeneration == 0 ||
			!validManagedVolumeComposeKey(source.Volume.ComposeVolumeKey) ||
			source.Volume.DockerVolumeName != "gp_vol_"+capture.TargetId ||
			validateVolumeDirectory(source.Volume.AuthorizedVolumeDir, environmentID) != nil {
			return errs.New(errs.KindValidationFailed, "volume backup source control data is invalid")
		}
		previousServiceID := ""
		for _, service := range source.Volume.Services {
			if service == nil || validateID(ids.KindService, service.ServiceId) != nil ||
				service.ServiceId <= previousServiceID || service.ServiceRevision == 0 ||
				!validBackupServiceRuntimeIntent(service.PriorIntent) ||
				!validManagedVolumeComposeKey(service.ComposeKey) || len(service.MountPaths) == 0 {
				return errs.New(errs.KindValidationFailed, "volume backup service control data is invalid")
			}
			previousMountPath := ""
			for _, mountPath := range service.MountPaths {
				if !path.IsAbs(mountPath) || path.Clean(mountPath) != mountPath ||
					mountPath <= previousMountPath || strings.ContainsRune(mountPath, '\x00') {
					return errs.New(errs.KindValidationFailed, "volume backup mount path is invalid or unsorted")
				}
				previousMountPath = mountPath
			}
			previousServiceID = service.ServiceId
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup source kind is unsupported")
	}
	return nil
}

func validateBackupUploadAuthority(environmentID string, capture *agentpb.BackupSourceCapture) error {
	upload := capture.Upload
	if upload == nil || !validBackupConnectorEndpoint(upload.ConnectorEndpoint) ||
		!validBackupConnectorBucket(upload.ConnectorBucket) ||
		!validBackupConnectorPrefix(upload.ConnectorPrefix) ||
		!validBackupConnectorRegion(upload.ConnectorRegion) ||
		!validBackupS3Addressing(upload.ConnectorAddressing) ||
		!upload.ImmutableCreate || !upload.PutAfterArtifactPreparedAck ||
		!upload.HeadAfterUploadCompletedAck || upload.ProtectedObjectKey == "" ||
		len(upload.ProtectedObjectKey) > maximumBackupObjectKeyBytes ||
		!utf8.ValidString(upload.ProtectedObjectKey) || strings.ContainsRune(upload.ProtectedObjectKey, '\x00') ||
		strings.Contains(upload.ProtectedObjectKey, `\`) || strings.HasPrefix(upload.ProtectedObjectKey, "/") {
		return errs.New(errs.KindValidationFailed, "backup upload authority is invalid")
	}
	expected := upload.ConnectorPrefix + environmentID + "/" + capture.SourceId + "/" + capture.PointId + "/artifact.bin"
	if upload.ProtectedObjectKey != expected {
		return errs.New(errs.KindValidationFailed, "backup upload object identity is invalid")
	}
	return nil
}

func validBackupServiceRuntimeIntent(intent agentpb.BackupServiceRuntimeIntent) bool {
	switch intent {
	case agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING,
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_STOPPED,
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_ABSENT:
		return true
	default:
		return false
	}
}
