package platformcomponents

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetPlatformComponentActiveTask reads the actual publication fence, including
// terminal Tasks whose effects still need reconciliation. Status alone cannot
// authorize another Component mutation.
func (repository *Persistence) GetPlatformComponentActiveTask(
	ctx context.Context,
	componentID string,
	revision int64,
) (string, error) {
	if ids.Validate(ids.KindComponent, componentID) != nil || revision <= 0 {
		return "", errs.New(errs.KindValidationFailed, "Component active Task read is invalid")
	}
	read, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{ActiveTaskKey(componentID)}, Revision: revision},
	)
	if err != nil {
		return "", err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return "", errs.New(errs.KindInternal, "Component active Task read is incomplete")
	}
	if read.Values[0] == nil {
		return "", nil
	}
	id := string(read.Values[0].Value)
	if ids.Validate(ids.KindTask, id) != nil {
		return "", errs.New(errs.KindInternal, "Component active Task identity is corrupt")
	}
	return id, nil
}

func ActiveTaskKey(componentID string) string {
	return "/v1/indexes/platform-component-tasks/by-component/" + componentID
}
