package backupplanning

import (
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func snapshotRevisionDigest(value *etcdstore.KeyValue) *agentpb.RevisionDigest {
	digest := sha256.Sum256(value.Value)
	return &agentpb.RevisionDigest{ModRevision: value.ModRevision, Sha256: digest[:]}
}

func (repository *Planner) manualBackupPlanScope(ctx context.Context, environment *etcdstore.KeyValue,
	projectID string, fixedRevision int64) (*agentpb.BackupPlanScope, error) {
	read, err := repository.reader.ReadFixedKeys(ctx, []string{hierarchyrecord.ProjectKey(projectID)}, fixedRevision)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return nil, errs.New(errs.KindStateConflict, "backup Project snapshot is unavailable")
	}
	project, err := hierarchyrecord.DecodeProject(read.Values[0].Value)
	selected, environmentErr := hierarchyrecord.DecodeEnvironment(environment.Value)
	if err != nil || environmentErr != nil || project.ID != projectID || selected.ProjectID != projectID {
		return nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	return &agentpb.BackupPlanScope{ProjectId: projectID, Project: snapshotRevisionDigest(read.Values[0]),
		EnvironmentId: selected.ID, Environment: snapshotRevisionDigest(environment), TaskAttempt: 1}, nil
}

func (repository *Planner) manualBackupConnectorAuthority(ctx context.Context, run backupruntime.BackupRunRecord,
	projectID string, fixedRevision int64) (*agentpb.BackupConnectorAuthority, error) {
	read, err := repository.reader.ReadFixedKeys(ctx, []string{connectorrecord.RecordKey(run.ConnectorID),
		connectorrecord.CredentialValueKey(run.ConnectorID)}, fixedRevision)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearValues(read.Values)
	if err := backupruntime.ValidateBackupConnectorSnapshotEvidence(read.Values, run); err != nil {
		return nil, err
	}
	connector, err := connectorrecord.DecodeRecord(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	access, secret, err := repository.manualBackupCredentialAuthorities(
		ctx,
		connector,
		read.Values[1],
		projectID,
		fixedRevision,
	)
	if err != nil {
		return nil, err
	}
	pathStyle := run.ConnectorPathStyle
	return &agentpb.BackupConnectorAuthority{
		ConnectorId:          run.ConnectorID,
		Connector:            snapshotRevisionDigest(read.Values[0]),
		CanonicalEndpointUrl: run.ConnectorEndpoint,
		Region:               run.ConnectorRegion,
		PathStyle:            &pathStyle,
		Prefix:               run.ConnectorPrefix,
		AccessKeySlotId:      backupsecret.AccessKeySlotID,
		SecretKeySlotId:      backupsecret.SecretKeySlotID,
		AccessKeySlot:        access,
		SecretKeySlot:        secret,
	}, nil
}

func (repository *Planner) manualBackupEncryptionAuthority(ctx context.Context, run backupruntime.BackupRunRecord,
	fixedRevision int64) (*agentpb.BackupEncryptionAuthority, error) {
	if run.Encryption == backupruntime.BackupRuntimeEncryptionNone {
		return &agentpb.BackupEncryptionAuthority{Kind: agentpb.BackupEncryption_BACKUP_ENCRYPTION_NONE}, nil
	}
	read, err := repository.reader.ReadFixedKeys(ctx, []string{backuppolicy.BackupKeyKey(run.EnvironmentID),
		backuppolicy.BackupKeyValueKey(run.EnvironmentID)}, fixedRevision)
	if err != nil {
		return nil, err
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[0].ModRevision != run.BackupKeyRecordRevision ||
		read.Values[1].ModRevision != run.BackupKeyValueRevision {
		return nil, errs.New(errs.KindStateConflict, "backup age identity snapshot changed")
	}
	record, err := backuppolicy.DecodeBackupKeyRecord(read.Values[0].Value)
	if err != nil || record.KeyEra != run.KeyEra || record.Recipient != run.Recipient {
		return nil, backupruntime.CorruptBackupRuntimeRecord()
	}
	digest := sha256.Sum256([]byte(run.Recipient))
	return &agentpb.BackupEncryptionAuthority{
		Kind:            agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
		SecretSlotId:    backupsecret.CurrentAgeIdentitySlotID,
		SecretSlot:      snapshotRevisionDigest(read.Values[1]),
		RecipientSha256: digest[:],
		KeyEra:          proto.Uint64(uint64(run.KeyEra)),
	}, nil
}
