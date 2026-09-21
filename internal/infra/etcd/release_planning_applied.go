package etcd

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
func (ledger *ReleaseLedger) GetPlanningAppliedProjection(
	ctx context.Context,
	scope ReleasePlanningScope,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if ctx == nil || ledger == nil || ledger.store == nil || scope.ReadRevision <= 0 ||
		ids.Validate(ids.KindEnvironment, scope.Environment.Record.ID) != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, errs.New(
			errs.KindValidationFailed,
			"release applied projection scope is invalid",
		)
	}
	read, err := ledger.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{
			Keys:     []string{projectionrecord.EnvironmentComposeProjectionStorageKey(scope.Environment.Record.ID)},
			Revision: scope.ReadRevision,
		},
	)
	if err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	if read == nil || read.ReadRevision != scope.ReadRevision || len(read.Values) != 1 {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, releases.CorruptReleaseRecord()
	}
	if read.Values[0] == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{ReadRevision: scope.ReadRevision}, false, nil
	}
	projection, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
	if err != nil || projection.EnvironmentID != scope.Environment.Record.ID {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, releases.CorruptReleaseRecord()
	}
	return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{
		Record:       projection,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: scope.ReadRevision,
	}, true, nil
}
