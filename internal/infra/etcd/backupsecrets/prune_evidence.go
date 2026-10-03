package backupsecrets

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	backupplanning "github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (reader *Reader) decodePruneDynamicEvidence(
	ctx context.Context,
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *Evidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
) error {
	dispatch := evidence.Dispatch
	if dispatch == nil {
		return errs.New(errs.KindInternal, "backup prune dispatch evidence is missing")
	}
	if stepIndex < 0 || stepIndex >= len(dispatch.RecoveryPointIDs) ||
		len(plan.Steps) != len(dispatch.RecoveryPointIDs) {
		return errs.New(errs.KindStateConflict, "backup prune selected step is unavailable")
	}
	index := stepIndex
	pointID := dispatch.RecoveryPointIDs[index]
	planned := plan.Steps[index].GetBackupStep().GetPrune()
	if planned == nil || len(planned.Objects) != 1 || planned.Objects[0].Ordinal != uint32(index+1) ||
		planned.Objects[0].PointId != pointID {
		return errs.New(errs.KindStateConflict, "backup prune sealed authority is unavailable")
	}
	pointValue := result.Values[dynamic.points[pointID]]
	pruneValue := result.Values[dynamic.prunes[pointID]]
	if pointValue == nil || pruneValue == nil {
		return errs.New(errs.KindStateConflict, "backup prune point evidence is unavailable")
	}
	point, pointErr := backupruntime.DecodeBackupRecoveryPointRecord(pointValue.Value)
	prune, pruneErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(pruneValue.Value)
	if pointErr != nil || pruneErr != nil || prune.Point != point.BackupRecoveryPointSnapshot ||
		prune.PointRevision != pointValue.ModRevision ||
		prune.Point.ID != pointID || prune.OperationID != dispatch.OperationID ||
		prune.TaskID != dispatch.TaskID || prune.State != backupruntime.BackupPruneAssigned ||
		prune.Point.EnvironmentID != dispatch.EnvironmentID ||
		pruneValue.ModRevision != evidence.DispatchRevision {
		return errs.New(errs.KindStateConflict, "backup prune point evidence changed")
	}
	policyDigest, err := hex.DecodeString(prune.PolicySHA256)
	if err != nil || len(policyDigest) != sha256.Size || planned.RetentionPolicy == nil ||
		planned.RetentionPolicy.ModRevision != prune.PolicyRevision || !bytes.Equal(planned.RetentionPolicy.Sha256, policyDigest) {
		return errs.New(errs.KindStateConflict, "backup prune retained policy authority changed")
	}
	policy := &agentpb.RevisionDigest{ModRevision: prune.PolicyRevision, Sha256: policyDigest}
	sourceValue := result.Values[dynamic.sources[point.SourceID]]
	environmentValue := result.Values[dynamic.environment]
	connectorValue := result.Values[dynamic.connectors[point.ConnectorID]]
	if sourceValue == nil || environmentValue == nil || connectorValue == nil {
		return errs.New(errs.KindStateConflict, "backup prune authority evidence is unavailable")
	}
	source, sourceErr := backuppolicy.DecodeBackupSourceRecord(sourceValue.Value)
	environment, environmentErr := hierarchyrecord.DecodeEnvironment(environmentValue.Value)
	connector, connectorErr := connectorrecord.DecodeRecord(connectorValue.Value)
	if sourceErr != nil {
		return errs.New(errs.KindStateConflict, "backup prune source authority is corrupt")
	}
	if environmentErr != nil {
		return errs.New(errs.KindStateConflict, "backup prune Environment authority is corrupt")
	}
	if connectorErr != nil {
		return errs.New(errs.KindStateConflict, "backup prune Connector authority is corrupt")
	}
	if source.ID != point.SourceID || source.EnvironmentID != point.EnvironmentID ||
		string(source.Kind) != string(point.SourceKind) || source.TargetID != point.TargetID {
		return errs.New(errs.KindStateConflict, "backup prune source authority changed")
	}
	if environment.ID != point.EnvironmentID {
		return errs.New(errs.KindStateConflict, "backup prune Environment authority changed")
	}
	if connector.Connector.ID != point.ConnectorID ||
		connector.Connector.EnvironmentID != point.EnvironmentID {
		return errs.New(errs.KindStateConflict, "backup prune Connector authority changed")
	}
	if err := requireNoDeletionFence(
		result.Values[dynamic.connectorFences[point.ConnectorID]], deletionrecord.DeletionTargetConnector, point.ConnectorID,
	); err != nil {
		return err
	}
	pointDigest := sha256.Sum256(pointValue.Value)
	item := backupplanning.PruneExecutionEvidence{
		Prune: prune, PruneRevision: pruneValue.ModRevision,
		PointRevision: pointValue.ModRevision, SourceRevision: sourceValue.ModRevision,
		EnvironmentRevision: environmentValue.ModRevision,
		ConnectorRevision:   connectorValue.ModRevision,
		ConnectorEndpoint:   connector.Connector.Endpoint, ConnectorBucket: connector.Connector.Bucket,
		ConnectorPrefix: connector.Connector.Prefix, ConnectorRegion: connector.Connector.Region,
		ConnectorPathStyle: connector.Connector.PathStyle,
		RetentionPolicy:    policy, PointSHA256: append([]byte(nil), pointDigest[:]...),
		ConnectorAuthority: dynamic.connectorAuthorities[point.ConnectorID],
	}
	if err := backupplanning.ValidateBackupPruneExecutionStep(*dispatch, item, plan, index); err != nil {
		return err
	}
	evidence.Point = &point
	evidence.Prune = &prune
	evidence.Source = mustDecodeBackupSource(result.Values[dynamic.sources[point.SourceID]])
	evidence.SourceRevision = item.SourceRevision
	return nil
}

func mustDecodeBackupSource(value *etcdstore.KeyValue) backuppolicy.BackupSourceRecord {
	if value == nil {
		return backuppolicy.BackupSourceRecord{}
	}
	record, _ := backuppolicy.DecodeBackupSourceRecord(value.Value)
	return record
}
