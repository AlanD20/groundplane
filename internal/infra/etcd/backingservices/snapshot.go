package backingservices

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetBackingServiceAtRevision keeps deletion admission on its captured snapshot.
func (repository *Repository) GetBackingServiceAtRevision(
	ctx context.Context, projectID string, revision int64,
) (keyvalue.Versioned[Record], error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{hierarchy.ProjectKey(projectID)}, Revision: revision,
	})
	if err != nil {
		return keyvalue.Versioned[Record]{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindStateConflict, "Backing Service snapshot is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	project, err := hierarchy.DecodeProject(read.Values[0].Value)
	if err != nil || project.ID != projectID {
		return keyvalue.Versioned[Record]{}, errs.New(errs.KindInternal, "Backing Project snapshot is inconsistent")
	}
	return repository.composeBackingService(ctx, keyvalue.Versioned[hierarchy.ProjectRecord]{
		Record: project, Revision: read.Values[0].ModRevision, ReadRevision: revision,
	})
}
