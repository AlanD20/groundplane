package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ListEnvironmentVolumes proves the Environment, deletion fence, owner index,
// and every Volume primary at the page's one MVCC revision.
func (repository *VolumeRepository) ListEnvironmentVolumes(
	ctx context.Context,
	environmentID string,
	request PageRequest,
) (Page[VolumeRecord], error) {
	page, err := repository.ListVolumes(ctx, environmentID, request)
	if err != nil {
		return Page[VolumeRecord]{}, err
	}
	if page.Revision <= 0 {
		return Page[VolumeRecord]{}, errs.New(errs.KindInternal, "volume projection revision is invalid")
	}
	hierarchy, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentKey(environmentID),
			deletionTombstoneKey(string(DeletionTargetEnvironment), environmentID),
		},
		Revision: page.Revision,
	})
	if err != nil {
		return Page[VolumeRecord]{}, err
	}
	if hierarchy == nil || hierarchy.ReadRevision != page.Revision || len(hierarchy.Values) != 2 {
		return Page[VolumeRecord]{}, errs.New(errs.KindInternal, "volume hierarchy projection is incomplete")
	}
	if hierarchy.Values[0] == nil || hierarchy.Values[1] != nil {
		return Page[VolumeRecord]{}, errs.New(errs.KindEnvironmentNotFound, "environment was not found")
	}
	environment, err := decodeEnvironment(hierarchy.Values[0].Value)
	if err != nil || environment.ID != environmentID || ids.Validate(ids.KindEnvironment, environment.ID) != nil {
		return Page[VolumeRecord]{}, corruptRecord()
	}
	return page, nil
}
