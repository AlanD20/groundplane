package releasequeries

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingpostgresruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (reader *Reader) GetPostgresRuntimeAt(ctx context.Context, environmentID, serviceID string,
	revision int64,
) (etcdstore.Versioned[backingpostgresruntime.Record], bool, error) {
	var empty etcdstore.Versioned[backingpostgresruntime.Record]
	if reader == nil || ctx == nil || revision <= 0 || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindService, serviceID) != nil {
		return empty, false, errs.New(errs.KindValidationFailed, "PostgreSQL runtime read scope is invalid")
	}
	key := backingpostgresruntime.Key(environmentID, serviceID)
	read, err := reader.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return empty, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return empty, false, releases.CorruptReleaseRecord()
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return empty, false, nil
	}
	record, err := backingpostgresruntime.Decode(read.Values[0].Value)
	if err != nil || record.EnvironmentID != environmentID || record.ServiceID != serviceID {
		return empty, false, releases.CorruptReleaseRecord()
	}
	return etcdstore.Versioned[backingpostgresruntime.Record]{Record: record, Revision: read.Values[0].ModRevision,
		ReadRevision: revision}, true, nil
}

// PostgresPatchConditions protects the selected Service's database storage and
// tool image without decoding an execution plan. The caller must join these
// comparisons to the transaction that makes the captured Release executable.
func (reader *Reader) PostgresPatchConditions(
	ctx context.Context,
	artifact *agentpb.ComposeArtifact,
	serviceID string,
	revision int64,
) ([]etcdstore.Condition, error) {
	if artifact == nil || artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		ids.Validate(ids.KindService, serviceID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "PostgreSQL patch requires the selected Service artifact")
	}
	selected, hasTools := false, false
	for _, workload := range artifact.GetServices() {
		if workload.GetServiceId() == serviceID {
			selected = true
			hasTools = hasTools || workload.GetPostgresToolsImage() != ""
		}
	}
	if !selected {
		return nil, errs.New(errs.KindValidationFailed, "Release artifact has no selected Service")
	}
	prior, found, err := reader.GetPostgresRuntimeAt(ctx, artifact.OwnerId, serviceID, revision)
	if err != nil {
		return nil, err
	}
	if !found && hasTools {
		return nil, errs.New(errs.KindStateConflict, "PostgreSQL must finish provisioning before an image update")
	}
	if found {
		// An existing database cannot evade storage checks by dropping the tool
		// marker from its candidate definition.
		if _, err := backingpostgresruntime.ValidatePatchArtifact(prior.Record, artifact); err != nil {
			return nil, err
		}
	}
	return []etcdstore.Condition{{Key: backingpostgresruntime.Key(artifact.OwnerId, serviceID), ModRevision: prior.Revision}}, nil
}
