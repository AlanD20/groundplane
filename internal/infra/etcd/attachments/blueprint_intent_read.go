package attachments

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Reader) GetBlueprintAttachTaskIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[BlueprintAttachTaskIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Blueprint Attach Task id is invalid",
		)
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{BlueprintAttachTaskIntentKey(taskID)}})
	if err != nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	if result == nil || len(result.Values) != 1 {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, errs.New(
			errs.KindInternal,
			"Blueprint Attach Task intent read is incomplete",
		)
	}
	if result.Values[0] == nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := DecodeBlueprintAttachTaskIntent(result.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[BlueprintAttachTaskIntent]{}, false, err
	}
	return etcdstore.Versioned[BlueprintAttachTaskIntent]{
		Record: intent, Revision: result.Values[0].ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
