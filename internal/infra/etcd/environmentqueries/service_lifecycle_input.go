package environmentqueries

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *ServiceReader) GetServiceLifecycleRenderInput(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[releaserender.ServiceLifecycleRenderInput], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[releaserender.ServiceLifecycleRenderInput]{}, false, err
	}
	if ids.Validate(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[releaserender.ServiceLifecycleRenderInput]{}, false, errs.New(
			errs.KindValidationFailed,
			"Service lifecycle render input Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, releaserender.ServiceLifecycleRenderInputKey(taskID))
	if err != nil {
		return etcdstore.Versioned[releaserender.ServiceLifecycleRenderInput]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[releaserender.ServiceLifecycleRenderInput]{}, false, errs.New(
			errs.KindInternal,
			"Service lifecycle render input read is empty",
		)
	}
	if result.Entry == nil {
		return etcdstore.Versioned[releaserender.ServiceLifecycleRenderInput]{
			ReadRevision: result.ReadRevision,
		}, false, nil
	}
	input, err := releaserender.DecodeServiceLifecycleRenderInput(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[releaserender.ServiceLifecycleRenderInput]{}, false, err
	}
	return etcdstore.Versioned[releaserender.ServiceLifecycleRenderInput]{
		Record: input, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}
