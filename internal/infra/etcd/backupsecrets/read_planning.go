package backupsecrets

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type backupSecretDynamicRead struct {
	keys  []string
	index map[string]int

	environment          int
	project              int
	environmentFence     int
	projectFence         int
	connectors           map[string]int
	connectorAuthorities map[string]*agentpb.BackupConnectorAuthority
	scope                *agentpb.BackupPlanScope
	connectorFences      map[string]int
	credentials          map[string]int
	sources              map[string]int
	points               map[string]int
	prunes               map[string]int
	secretIndexes        []backupSecretIndexRead
	secretRecords        map[string]int
	secretFences         map[string]int
	secretValues         map[string]int
	last                 *etcdstore.GetManyResult
	currentAgeKey        int
	currentAgeValue      int
}

type backupSecretIndexRead struct {
	name          backupsecret.CredentialName
	reference     string
	projectIndex  int
	platformIndex int
}

func newBackupSecretDynamicRead() *backupSecretDynamicRead {
	return &backupSecretDynamicRead{
		index: make(map[string]int), connectors: make(map[string]int),
		connectorAuthorities: make(map[string]*agentpb.BackupConnectorAuthority),
		connectorFences:      make(map[string]int), credentials: make(map[string]int),
		sources: make(map[string]int), points: make(map[string]int),
		prunes: make(map[string]int), secretRecords: make(map[string]int),
		secretFences: make(map[string]int), secretValues: make(map[string]int),
		currentAgeKey: -1, currentAgeValue: -1,
	}
}

func (dynamic *backupSecretDynamicRead) add(key string) int {
	if position, ok := dynamic.index[key]; ok {
		return position
	}
	position := len(dynamic.keys)
	dynamic.keys = append(dynamic.keys, key)
	dynamic.index[key] = position
	return position
}

func (reader *Reader) planDynamicKeys(
	ctx context.Context,
	dynamic *backupSecretDynamicRead,
	evidence Evidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
) (string, map[string]int, map[string]int, error) {
	environmentID := ""
	connectorIDs := make(map[string]int)
	sourceIDs := make(map[string]int)
	dynamic.scope = plan.GetBackupScope()
	if evidence.Run != nil {
		environmentID = evidence.Run.EnvironmentID
		for _, source := range evidence.Run.Sources {
			position := dynamic.add(backuppolicy.BackupSourceKey(source.SourceID))
			sourceIDs[source.SourceID] = position
			dynamic.sources[source.SourceID] = position
		}
		position := dynamic.add(connectorrecord.RecordKey(evidence.Run.ConnectorID))
		connectorIDs[evidence.Run.ConnectorID] = position
		dynamic.connectors[evidence.Run.ConnectorID] = position
		dynamic.connectorAuthorities[evidence.Run.ConnectorID] = plan.Steps[stepIndex].GetBackupStep().
			GetCapture().
			GetTarget().
			GetConnector()
	} else if evidence.Restore != nil {
		restored := evidence.Restore
		environmentID = restored.EnvironmentID
		connectorID := restored.Point.ConnectorID
		position := dynamic.add(connectorrecord.RecordKey(connectorID))
		connectorIDs[connectorID], dynamic.connectors[connectorID] = position, position
		dynamic.connectorAuthorities[connectorID] = plan.Steps[stepIndex].GetBackupStep().GetRestore().GetSourceObject().GetConnector()
		position = dynamic.add(backuppolicy.BackupSourceKey(restored.Point.SourceID))
		sourceIDs[restored.Point.SourceID], dynamic.sources[restored.Point.SourceID] = position, position
		dynamic.points[restored.Point.ID] = dynamic.add(backupruntime.BackupRecoveryPointKey(restored.Point.ID))
	} else if evidence.Dispatch != nil {
		environmentID = evidence.Dispatch.EnvironmentID
		if len(plan.Steps) != len(evidence.Dispatch.RecoveryPointIDs) {
			return "", nil, nil, errs.New(errs.KindStateConflict, "backup prune sealed step count changed")
		}
		if stepIndex < 0 || stepIndex >= len(plan.Steps) {
			return "", nil, nil, errs.New(errs.KindStateConflict, "backup prune selected step is unavailable")
		}
		pointID := evidence.Dispatch.RecoveryPointIDs[stepIndex]
		pointKeys := []string{backupruntime.BackupRecoveryPointKey(pointID)}
		points, err := reader.readFixed(ctx, pointKeys, evidence.ReadRevision)
		if err != nil {
			return "", nil, nil, err
		}
		defer etcdstore.ClearValues(points.Values)
		dynamic.points[pointID] = dynamic.add(backupruntime.BackupRecoveryPointKey(pointID))
		dynamic.prunes[pointID] = dynamic.add(backupruntime.BackupRecoveryPointPruneKey(pointID))
		prune := plan.Steps[stepIndex].GetBackupStep().GetPrune()
		if prune == nil || len(prune.Objects) != 1 || prune.Objects[0].Ordinal != uint32(stepIndex+1) ||
			prune.Objects[0].PointId != pointID || points.Values[0] == nil {
			return "", nil, nil, errs.New(errs.KindStateConflict, "backup prune sealed point authority changed")
		}
		point, err := backupruntime.DecodeBackupRecoveryPointRecord(points.Values[0].Value)
		if err != nil || point.ID != pointID || point.EnvironmentID != environmentID {
			return "", nil, nil, errs.New(errs.KindStateConflict, "backup prune point evidence changed")
		}
		authority := prune.Objects[0].GetObject().GetConnector()
		if authority.GetConnectorId() != point.ConnectorID {
			return "", nil, nil, errs.New(errs.KindStateConflict, "backup prune Connector identity changed")
		}
		if previous := dynamic.connectorAuthorities[point.ConnectorID]; previous != nil && !proto.Equal(previous, authority) {
			return "", nil, nil, errs.New(errs.KindStateConflict, "backup prune Connector authorities disagree")
		}
		dynamic.connectorAuthorities[point.ConnectorID] = authority
		sourcePosition := dynamic.add(backuppolicy.BackupSourceKey(point.SourceID))
		sourceIDs[point.SourceID] = sourcePosition
		dynamic.sources[point.SourceID] = sourcePosition
		connectorPosition := dynamic.add(connectorrecord.RecordKey(point.ConnectorID))
		connectorIDs[point.ConnectorID] = connectorPosition
		dynamic.connectors[point.ConnectorID] = connectorPosition
	}
	if environmentID == "" || stepIndex < 0 || stepIndex >= len(plan.Steps) {
		return "", nil, nil, errs.New(errs.KindInternal, "backup secret resolution scope is incomplete")
	}
	if err := planCurrentAgeIdentityKeys(dynamic, plan.Steps[stepIndex], environmentID); err != nil {
		return "", nil, nil, err
	}
	dynamic.environment = dynamic.add(hierarchyrecord.EnvironmentKey(environmentID))
	dynamic.environmentFence = dynamic.add(
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environmentID),
	)
	for connectorID := range connectorIDs {
		dynamic.connectorFences[connectorID] = dynamic.add(
			deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetConnector), connectorID),
		)
		dynamic.credentials[connectorID] = dynamic.add(connectorrecord.CredentialValueKey(connectorID))
	}
	if evidence.Run != nil {
		source := evidence.Run.Sources[stepIndex]
		switch source.Kind {
		case backupruntime.BackupRuntimeSourceAttach:
			snapshot := source.Snapshot.Postgres
			if snapshot == nil {
				return "", nil, nil, errs.New(errs.KindInternal, "postgres backup source snapshot is corrupt")
			}
			dynamic.add(attachrecord.AttachKey(source.TargetID))
			dynamic.add(attachrecord.AttachFactsKey(source.TargetID))
			dynamic.add(hierarchyrecord.ProjectKey(snapshot.BackingProjectID))
			dynamic.add(hierarchyrecord.EnvironmentKey(snapshot.BackingEnvironmentID))
			dynamic.add(blueprints.EnvironmentBlueprintHeadKey(snapshot.BackingEnvironmentID))
		case backupruntime.BackupRuntimeSourceVolume:
			snapshot := source.Snapshot.Volume
			if snapshot == nil {
				return "", nil, nil, errs.New(errs.KindInternal, "volume backup source snapshot is corrupt")
			}
			dynamic.add(blueprints.EnvironmentBlueprintHeadKey(snapshot.EnvironmentID))
			dynamic.add(blueprints.EnvironmentBlueprintRootKey(snapshot.EnvironmentID, snapshot.DesiredRevisionID))
			for _, service := range snapshot.Services {
				dynamic.add(servicerecord.ServiceRuntimeKey(service.ServiceID))
			}
		case backupruntime.BackupRuntimeSourceConfig:
			if source.Snapshot.Config == nil {
				return "", nil, nil, errs.New(errs.KindInternal, "config backup source snapshot is corrupt")
			}
			// The target environment is already part of the common evidence.
			dynamic.add(hierarchyrecord.EnvironmentKey(environmentID))
		default:
			return "", nil, nil, errs.New(errs.KindInternal, "backup source kind is corrupt")
		}
	}
	return environmentID, connectorIDs, sourceIDs, nil
}

func (reader *Reader) readFixed(
	ctx context.Context,
	keys []string,
	revision int64,
) (*etcdstore.GetManyResult, error) {
	if len(keys) == 0 || revision <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "backup secret fixed read is invalid")
	}
	combined := &etcdstore.GetManyResult{Values: make([]*etcdstore.KeyValue, 0, len(keys)), ReadRevision: revision}
	for start := 0; start < len(keys); start += etcdstore.MaximumOperations {
		end := min(start+etcdstore.MaximumOperations, len(keys))
		batch := keys[start:end]
		result, err := getBackupSecretManyOwned(ctx, reader.store, batch, revision)
		if err != nil {
			etcdstore.ClearValues(combined.Values)
			return nil, err
		}
		combined.Values = append(combined.Values, result.Values...)
		combined.ResponseRevision = max(combined.ResponseRevision, result.ResponseRevision)
		result.Values = nil
	}
	return combined, nil
}

func anchorValues(result *etcdstore.GetManyResult) []*etcdstore.KeyValue {
	if result == nil {
		return nil
	}
	return result.Values
}
