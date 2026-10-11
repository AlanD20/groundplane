package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Declared removal never publishes its candidate as applied state. This guard
// permits unrelated projection changes but fences any newly applied target.
func (repository *TaskRepository) declaredServiceRemovalAppliedAbsence(
	ctx context.Context, intent environmentchanges.ServiceRemovalIntent, revision int64,
) ([]keyvalue.Condition, error) {
	if intent.Authored == nil ||
		intent.Authored.ProjectionSource != environmentchanges.ServiceRemovalDeclaredProjection {
		return nil, nil
	}
	key := projectionrecord.EnvironmentComposeProjectionStorageKey(intent.EnvironmentID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, errs.New(errs.KindInternal, "Service applied absence read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[0].Value)
		if err != nil || applied.EnvironmentID != intent.EnvironmentID ||
			projectionContainsService(applied, intent.ServiceID) {
			return nil, errs.New(errs.KindStateConflict, "Service gained an applied runtime projection")
		}
	}
	return []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}}, nil
}
