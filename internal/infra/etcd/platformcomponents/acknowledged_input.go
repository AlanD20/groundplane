package platformcomponents

import (
	"context"

	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type renderInputReader interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
}

// ReadAcknowledgedInput follows the reporting Task's plan, not the container's
// ownership plan. Reloading configuration can retain an older container owner.
func ReadAcknowledgedInput(
	ctx context.Context,
	store renderInputReader,
	observation ComponentObservationRecord,
	taskPlanID string,
	revision int64,
) (etcdstore.Versioned[PlatformComponentTaskRenderInput], error) {
	var result etcdstore.Versioned[PlatformComponentTaskRenderInput]
	read, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{PlatformComponentTaskRenderInputKey(taskPlanID)}, Revision: revision,
	})
	if err != nil {
		return result, err
	}
	if read == nil || len(read.Values) != 1 || read.Values[0] == nil {
		return result, errs.New(errs.KindStateConflict, "acknowledged resolver input is unavailable")
	}
	input, err := DecodePlatformComponentTaskRenderInput(read.Values[0].Value)
	if err != nil {
		return result, err
	}
	if input.PlanID != taskPlanID ||
		input.ComponentID != observation.ComponentID ||
		input.TaskID != observation.TaskID ||
		input.OwnershipPlanID != observation.PlanID ||
		input.ArtifactSHA256 != observation.CorefileSHA256 {
		return result, errs.New(errs.KindStateConflict, "acknowledged resolver input identity changed")
	}
	return etcdstore.Versioned[PlatformComponentTaskRenderInput]{
		Record: input, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision,
	}, nil
}
