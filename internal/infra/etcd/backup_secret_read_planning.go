package etcd

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
)

type backupSecretDynamicRead struct {
	keys  []string
	index map[string]int

	environment      int
	project          int
	environmentFence int
	projectFence     int
	connectors       map[string]int
	connectorFences  map[string]int
	credentials      map[string]int
	sources          map[string]int
	points           map[string]int
	prunes           map[string]int
	secretIndexes    []backupSecretIndexRead
	secretRecords    map[string]int
	secretFences     map[string]int
	secretValues     map[string]int
	last             *etcdstore.GetManyResult
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
		connectorFences: make(map[string]int), credentials: make(map[string]int),
		sources: make(map[string]int), points: make(map[string]int),
		prunes: make(map[string]int), secretRecords: make(map[string]int),
		secretFences: make(map[string]int), secretValues: make(map[string]int),
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

func (reader *BackupSecretResolutionReader) planDynamicKeys(
	dynamic *backupSecretDynamicRead,
	evidence BackupSecretResolutionEvidence,
	plan *agentpb.ExecutionPlan,
	stepIndex int,
) (string, map[string]int, map[string]int, error) {
	environmentID := ""
	connectorIDs := make(map[string]int)
	sourceIDs := make(map[string]int)
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
	} else if evidence.Dispatch != nil {
		environmentID = evidence.Dispatch.EnvironmentID
		for index, pointID := range evidence.Dispatch.RecoveryPointIDs {
			dynamic.points[pointID] = dynamic.add(backupruntime.BackupRecoveryPointKey(pointID))
			dynamic.prunes[pointID] = dynamic.add(backupruntime.BackupRecoveryPointPruneKey(pointID))
			if index < len(plan.Steps) {
				prune := plan.Steps[index].GetBackupArtifactPrune()
				if prune != nil {
					sourcePosition := dynamic.add(backuppolicy.BackupSourceKey(prune.SourceId))
					sourceIDs[prune.SourceId] = sourcePosition
					dynamic.sources[prune.SourceId] = sourcePosition
					connectorPosition := dynamic.add(connectorrecord.RecordKey(prune.ConnectorId))
					connectorIDs[prune.ConnectorId] = connectorPosition
					dynamic.connectors[prune.ConnectorId] = connectorPosition
				}
			}
		}
	}
	if environmentID == "" || stepIndex < 0 || stepIndex >= len(plan.Steps) {
		return "", nil, nil, errs.New(errs.KindInternal, "backup secret resolution scope is incomplete")
	}
	dynamic.environment = dynamic.add(hierarchyrecord.EnvironmentKey(environmentID))
	dynamic.environmentFence = dynamic.add(
		deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environmentID),
	)
	for connectorID := range connectorIDs {
		dynamic.connectorFences[connectorID] = dynamic.add(
			deletionTombstoneKey(string(deletionrecord.DeletionTargetConnector), connectorID),
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

func (reader *BackupSecretResolutionReader) readFixed(
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
