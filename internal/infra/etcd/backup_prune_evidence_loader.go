package etcd

import (
	"context"
	"encoding/hex"

	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppruneevidence"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *BackupRuntimeRepository) loadBackupPruneExecutionEvidence(
	ctx context.Context,
	pending []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	authorityValues []*etcdstore.KeyValue,
	readRevision int64,
) ([]backupplanning.PruneExecutionEvidence, error) {
	if readRevision <= 0 || len(pending) == 0 || len(pending) > backupruntime.MaximumBackupPruneBatch ||
		len(authorityValues) != 1+len(pending)*5 {
		return nil, errs.New(errs.KindValidationFailed, "backup prune publication snapshot is invalid")
	}
	evidence := make([]backupplanning.PruneExecutionEvidence, len(pending))
	for index, version := range pending {
		start := 1 + index*5
		if err := backupruntime.ValidatePendingBackupPruneAuthority(authorityValues[start:start+5], version); err != nil {
			return nil, err
		}
		item, err := repository.loadBackupPrunePointExecutionEvidence(
			ctx,
			version,
			authorityValues[start+1],
			readRevision,
		)
		if err != nil {
			return nil, err
		}
		evidence[index] = item
	}
	return evidence, nil
}

func (repository *BackupRuntimeRepository) loadBackupPrunePointExecutionEvidence(
	ctx context.Context,
	version etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
	pointValue *etcdstore.KeyValue,
	readRevision int64,
) (backupplanning.PruneExecutionEvidence, error) {
	point, err := backupruntime.DecodeBackupRecoveryPointRecord(pointValue.Value)
	if err != nil || point.BackupRecoveryPointSnapshot != version.Record.Point ||
		version.Record.PolicyRevision <= 0 || !recordcodec.ValidSHA256(version.Record.PolicySHA256) {
		return backupplanning.PruneExecutionEvidence{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	fixed, err := repository.ReadFixedKeys(ctx, []string{
		backuppolicy.BackupSourceKey(point.SourceID), hierarchyrecord.EnvironmentKey(point.EnvironmentID),
		connectorrecord.RecordKey(point.ConnectorID), connectorrecord.CredentialValueKey(point.ConnectorID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), point.ConnectorID),
	}, readRevision)
	if err != nil {
		return backupplanning.PruneExecutionEvidence{}, err
	}
	defer etcdstore.ClearValues(fixed.Values)
	if len(fixed.Values) != 5 || fixed.Values[0] == nil || fixed.Values[1] == nil || fixed.Values[2] == nil {
		return backupplanning.PruneExecutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup prune plan evidence is missing",
		)
	}
	source, sourceErr := backuppolicy.DecodeBackupSourceRecord(fixed.Values[0].Value)
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(fixed.Values[1].Value)
	connector, connectorErr := connectorrecord.DecodeRecord(fixed.Values[2].Value)
	if sourceErr != nil || environmentErr != nil || connectorErr != nil ||
		source.ID != point.SourceID || source.EnvironmentID != point.EnvironmentID ||
		string(source.Kind) != string(point.SourceKind) || source.TargetID != point.TargetID ||
		environment.ID != point.EnvironmentID || connector.Connector.ID != point.ConnectorID ||
		connector.Connector.EnvironmentID != point.EnvironmentID || connector.Connector.Kind != "s3-compatible" {
		return backupplanning.PruneExecutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup prune plan evidence changed",
		)
	}
	if err := backuppruneevidence.ValidateNoDeletion(
		fixed.Values[4],
		deletionrecord.DeletionTargetConnector,
		point.ConnectorID,
	); err != nil {
		return backupplanning.PruneExecutionEvidence{}, err
	}
	authority, err := backuppruneevidence.LoadConnectorAuthority(
		ctx,
		repository,
		connector,
		fixed.Values[2],
		fixed.Values[3],
		environment.ProjectID,
		readRevision,
	)
	if err != nil {
		return backupplanning.PruneExecutionEvidence{}, err
	}
	if authority.CanonicalEndpointUrl != point.ConnectorEndpoint ||
		connector.Connector.Bucket != point.ConnectorBucket ||
		authority.Region != point.ConnectorRegion ||
		authority.Prefix != point.ConnectorPrefix ||
		authority.GetPathStyle() != point.ConnectorPathStyle {
		return backupplanning.PruneExecutionEvidence{}, errs.New(
			errs.KindStateConflict,
			"backup prune retained object locator changed",
		)
	}
	policyDigest, err := hex.DecodeString(version.Record.PolicySHA256)
	if err != nil {
		return backupplanning.PruneExecutionEvidence{}, backupruntime.CorruptBackupRuntimeRecord()
	}
	return backupplanning.PruneExecutionEvidence{
		Prune: version.Record, PruneRevision: version.Revision, PointRevision: pointValue.ModRevision,
		SourceRevision: fixed.Values[0].ModRevision, EnvironmentRevision: fixed.Values[1].ModRevision,
		ConnectorRevision: fixed.Values[2].ModRevision, ConnectorEndpoint: connector.Connector.Endpoint,
		ConnectorBucket: connector.Connector.Bucket, ConnectorPrefix: connector.Connector.Prefix,
		ConnectorRegion: connector.Connector.Region, ConnectorPathStyle: connector.Connector.PathStyle,
		RetentionPolicy: &agentpb.RevisionDigest{ModRevision: version.Record.PolicyRevision, Sha256: policyDigest},
		PointSHA256:     backuppruneevidence.RevisionDigest(pointValue).Sha256, ConnectorAuthority: authority,
	}, nil
}
