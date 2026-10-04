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

// ValidatePostgresPatchPlan rejects changed storage before any candidate Task
// can execute. Both direct Deploy and Blueprint publication use this guard.
func (reader *Reader) ValidatePostgresPatchPlan(
	ctx context.Context,
	plan *agentpb.ExecutionPlan,
	revision int64,
) error {
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.GetArtifacts()))
	for _, artifact := range plan.GetArtifacts() {
		artifacts[artifact.GetArtifactId()] = artifact
	}
	for _, member := range plan.GetCandidateReleaseProcedure().GetMembers() {
		artifact := artifacts[member.GetCandidateArtifactId()]
		if artifact == nil {
			return releases.CorruptReleaseRecord()
		}
		for _, workload := range artifact.Services {
			if workload.ServiceId != member.GetServiceId() || workload.PostgresToolsImage == "" {
				continue
			}
			prior, found, err := reader.GetPostgresRuntimeAt(ctx, artifact.OwnerId, workload.ServiceId, revision)
			if err != nil {
				return err
			}
			if !found {
				return errs.New(errs.KindStateConflict, "PostgreSQL must finish provisioning before an image update")
			}
			if _, err := backingpostgresruntime.ValidatePatchArtifact(prior.Record, artifact); err != nil {
				return err
			}
		}
	}
	return nil
}
