package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (reader *BackupSecretResolutionReader) decodePruneDynamicEvidence(
	ctx context.Context,
	result *etcdstore.GetManyResult,
	dynamic *backupSecretDynamicRead,
	evidence *BackupSecretResolutionEvidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
) error {
	dispatch := evidence.Dispatch
	if dispatch == nil {
		return errs.New(errs.KindInternal, "backup prune dispatch evidence is missing")
	}
	items := make([]backupPruneExecutionEvidence, len(dispatch.RecoveryPointIDs))
	for index, pointID := range dispatch.RecoveryPointIDs {
		planned := plan.Steps[index].GetBackupArtifactPrune()
		if planned == nil || planned.PruneRevision == 0 {
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
		sealed, err := reader.readFixed(
			ctx, []string{backupruntime.BackupRecoveryPointPruneKey(pointID)}, int64(planned.PruneRevision),
		)
		if err != nil {
			return err
		}
		sealedValue := sealed.Values[0]
		if sealedValue == nil || sealedValue.ModRevision != int64(planned.PruneRevision) {
			etcdstore.ClearValues(sealed.Values)
			return errs.New(errs.KindStateConflict, "backup prune sealed authority changed")
		}
		sealedPrune, sealedErr := backupruntime.DecodeBackupRecoveryPointPruneRecord(sealedValue.Value)
		etcdstore.ClearValues(sealed.Values)
		if sealedErr != nil || sealedPrune.Point != prune.Point ||
			sealedPrune.PointRevision != prune.PointRevision ||
			sealedPrune.OperationID != prune.OperationID || sealedPrune.TaskID != "" ||
			sealedPrune.State != backupruntime.BackupPrunePending {
			return errs.New(errs.KindStateConflict, "backup prune sealed authority changed")
		}
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
		items[index] = backupPruneExecutionEvidence{
			prune: prune, pruneRevision: int64(planned.PruneRevision),
			pointRevision: pointValue.ModRevision, sourceRevision: sourceValue.ModRevision,
			environmentRevision: environmentValue.ModRevision,
			connectorRevision:   connectorValue.ModRevision,
			connectorEndpoint:   connector.Connector.Endpoint, connectorBucket: connector.Connector.Bucket,
			connectorPrefix: connector.Connector.Prefix, connectorRegion: connector.Connector.Region,
			connectorPathStyle: connector.Connector.PathStyle,
		}
	}
	if err := validateBackupPruneExecutionPlan(*dispatch, items, plan); err != nil {
		return err
	}
	selected := plan.Steps[stepIndex]
	prune := selected.GetBackupArtifactPrune()
	if prune == nil {
		return errs.New(errs.KindStateConflict, "backup prune step evidence changed")
	}
	for index, pointID := range dispatch.RecoveryPointIDs {
		if pointID == prune.PointId {
			pointValue := result.Values[dynamic.points[pointID]]
			pruneValue := result.Values[dynamic.prunes[pointID]]
			point, _ := backupruntime.DecodeBackupRecoveryPointRecord(pointValue.Value)
			pruneRecord, _ := backupruntime.DecodeBackupRecoveryPointPruneRecord(pruneValue.Value)
			evidence.Point = &point
			evidence.Prune = &pruneRecord
			evidence.Source = mustDecodeBackupSource(result.Values[dynamic.sources[point.SourceID]])
			evidence.SourceRevision = items[index].sourceRevision
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "backup prune point is not in its dispatch")
}

func mustDecodeBackupSource(value *etcdstore.KeyValue) backuppolicy.BackupSourceRecord {
	if value == nil {
		return backuppolicy.BackupSourceRecord{}
	}
	record, _ := backuppolicy.DecodeBackupSourceRecord(value.Value)
	return record
}
