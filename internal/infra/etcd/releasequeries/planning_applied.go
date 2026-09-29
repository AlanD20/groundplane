package releasequeries

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetPlanningAppliedProjection reads the acknowledged artifact, not the desired
// Blueprint head, at the same captured revision as the Release planning scope.
func (ledger *Reader) GetPlanningAppliedProjection(
	ctx context.Context,
	scope ReleasePlanningScope,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	return ledger.GetAppliedProjectionAt(ctx, scope.Environment.Record.ID, scope.ReadRevision)
}

// GetAppliedProjectionAt reads acknowledged runtime at a fixed revision, never
// the desired Blueprint head, including Backing Services provisioned without a Release.
func (ledger *Reader) GetAppliedProjectionAt(
	ctx context.Context, environmentID string, revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if ctx == nil || ledger == nil || ledger.store == nil || revision <= 0 ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, errs.New(
			errs.KindValidationFailed,
			"release applied projection scope is invalid",
		)
	}
	read, err := ledger.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{
			Keys:     []string{projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID)},
			Revision: revision,
		},
	)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, releases.CorruptReleaseRecord()
	}
	if read.Values[0] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
			ReadRevision: revision,
		}, false, nil
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
	if err != nil || projection.EnvironmentID != environmentID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, releases.CorruptReleaseRecord()
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record:       projection,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: revision,
	}, true, nil
}
