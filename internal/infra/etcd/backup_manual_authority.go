package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *BackupRuntimeRepository) manualBackupOwner(
	ctx context.Context,
	environment hierarchyrecord.EnvironmentRecord,
	fixedRevision int64,
) (taskjournal.TaskOwner, error) {
	read, err := repository.readFixedKeys(
		ctx,
		[]string{hierarchyrecord.ProjectKey(environment.ProjectID)},
		fixedRevision,
	)
	if err != nil {
		return taskjournal.TaskOwner{}, err
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil {
		return taskjournal.TaskOwner{}, errs.New(errs.KindStateConflict, "backup Project is unavailable")
	}
	project, err := hierarchyrecord.DecodeProject(read.Values[0].Value)
	if err != nil || project.ID != environment.ProjectID || project.Kind != hierarchyrecord.ProjectKindTenant {
		return taskjournal.TaskOwner{}, errs.New(
			errs.KindStateConflict,
			"backing Environments cannot run consumer backups",
		)
	}
	tenantRead, err := repository.readFixedKeys(
		ctx,
		[]string{hierarchyrecord.TenantKey(project.TenantID)},
		fixedRevision,
	)
	if err != nil {
		return taskjournal.TaskOwner{}, err
	}
	defer clearKeyValues(tenantRead.Values)
	if tenantRead.Values[0] == nil {
		return taskjournal.TaskOwner{}, errs.New(errs.KindStateConflict, "backup Tenant is unavailable")
	}
	tenant, err := hierarchyrecord.DecodeTenant(tenantRead.Values[0].Value)
	if err != nil || tenant.ID != project.TenantID {
		return taskjournal.TaskOwner{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	return taskjournal.EnvironmentTaskOwner(project, environment)
}

func (repository *BackupRuntimeRepository) manualBackupConnector(
	ctx context.Context,
	connectorID string,
	environmentID string,
	fixedRevision int64,
) (connectorrecord.Record, int64, int64, bool, error) {
	read, err := repository.readFixedKeys(ctx, []string{
		connectorrecord.RecordKey(connectorID), connectorrecord.CredentialValueKey(connectorID),
	}, fixedRevision)
	if err != nil {
		return connectorrecord.Record{}, 0, 0, false, err
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil {
		return connectorrecord.Record{}, 0, 0, false, errs.New(
			errs.KindStateConflict,
			"backup Connector is unavailable",
		)
	}
	connector, err := connectorrecord.DecodeRecord(read.Values[0].Value)
	if err != nil || connector.Connector.ID != connectorID ||
		connector.Connector.EnvironmentID != environmentID ||
		connector.Connector.Kind != backupConnectorKindS3Compatible {
		return connectorrecord.Record{}, 0, 0, false, errs.New(
			errs.KindStateConflict,
			"backup Connector evidence changed",
		)
	}
	hasDirect := connectorrecord.HasDirectCredentials(connector)
	if !hasDirect {
		if read.Values[1] != nil {
			return connectorrecord.Record{}, 0, 0, false, errs.New(
				errs.KindStateConflict, "backup Connector credential evidence changed",
			)
		}
		return connector, read.Values[0].ModRevision, 0, false, nil
	}
	if read.Values[1] == nil {
		return connectorrecord.Record{}, 0, 0, false, errs.New(
			errs.KindStateConflict, "backup Connector credentials are unavailable",
		)
	}
	credentials, err := connectorrecord.DecodeEncryptedCredentials(read.Values[1].Value)
	if err != nil || credentials.ConnectorID != connectorID {
		clear(credentials.Ciphertext)
		return connectorrecord.Record{}, 0, 0, false, errs.New(
			errs.KindStateConflict, "backup Connector credential evidence changed",
		)
	}
	clear(credentials.Ciphertext)
	return connector, read.Values[0].ModRevision, read.Values[1].ModRevision, true, nil
}

func (repository *BackupRuntimeRepository) manualBackupKey(
	ctx context.Context,
	run *backupruntime.BackupRunRecord,
	fixedRevision int64,
) error {
	if run.Encryption == backupruntime.BackupRuntimeEncryptionNone {
		return nil
	}
	if run.Encryption != backupruntime.BackupRuntimeEncryptionAge {
		return errs.New(errs.KindStateConflict, "backup encryption strategy is unsupported")
	}
	read, err := repository.readFixedKeys(ctx, []string{
		backuppolicy.BackupKeyKey(run.EnvironmentID), backuppolicy.BackupKeyValueKey(run.EnvironmentID),
	}, fixedRevision)
	if err != nil {
		return err
	}
	defer clearKeyValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return errs.New(errs.KindStateConflict, "backup age key is unavailable")
	}
	record, recordErr := backuppolicy.DecodeBackupKeyRecord(read.Values[0].Value)
	value, valueErr := backuppolicy.DecodeBackupKeyEncryptedValue(read.Values[1].Value)
	defer clear(value.Ciphertext)
	if recordErr != nil || valueErr != nil || record.EnvironmentID != run.EnvironmentID ||
		value.EnvironmentID != run.EnvironmentID || record.KeyEra != value.KeyEra {
		return errs.New(errs.KindStateConflict, "backup age key evidence changed")
	}
	run.BackupKeyRecordRevision = read.Values[0].ModRevision
	run.BackupKeyValueRevision = read.Values[1].ModRevision
	run.KeyEra = record.KeyEra
	run.Recipient = record.Recipient
	return nil
}
