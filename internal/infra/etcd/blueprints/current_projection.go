package blueprints

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func ReadCurrentProjection(
	ctx context.Context,
	store blueprintSnapshotReader,
	environmentID string,
	revision int64,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]{}, false, err
	}
	return ReadProjectionAtRevision(ctx, store, environmentID, revision)
}
