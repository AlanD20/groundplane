package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	attachoutputs "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachoutputs"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// GetBlueprintAttachOutputGeneration returns one exact hook result at the
// caller's fixed planning revision. A mutable current Attach facts key is not
// authority for replay or for a later Blueprint revision.
func (repository *EnvironmentBlueprintRepository) GetBlueprintAttachOutputGeneration(
	ctx context.Context,
	attachID string,
	childTaskID string,
	revision int64,
) (etcdstore.Versioned[attachoutputs.Generation], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[attachoutputs.Generation]{}, false, err
	}
	if ids.Validate(ids.KindAttach, attachID) != nil ||
		ids.Validate(ids.KindTask, childTaskID) != nil || revision <= 0 {
		return etcdstore.Versioned[attachoutputs.Generation]{}, false, errs.New(
			errs.KindValidationFailed, "Blueprint Attach output lookup identity is invalid",
		)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{attachoutputs.Key(attachID, childTaskID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[attachoutputs.Generation]{}, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return etcdstore.Versioned[attachoutputs.Generation]{}, false, errs.New(
			errs.KindInternal, "Blueprint Attach output generation read is incomplete",
		)
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] == nil {
		return etcdstore.Versioned[attachoutputs.Generation]{ReadRevision: revision}, false, nil
	}
	value := read.Values[0]
	generation, err := attachoutputs.Decode(value.Value)
	if err != nil {
		return etcdstore.Versioned[attachoutputs.Generation]{}, false, err
	}
	if generation.AttachID != attachID || generation.ChildTaskID != childTaskID {
		attachoutputs.Clear(&generation)
		return etcdstore.Versioned[attachoutputs.Generation]{}, false, errs.New(
			errs.KindInternal, "Blueprint Attach output generation key is corrupt",
		)
	}
	return etcdstore.Versioned[attachoutputs.Generation]{
		Record: generation, Revision: value.ModRevision, ReadRevision: revision,
	}, true, nil
}
