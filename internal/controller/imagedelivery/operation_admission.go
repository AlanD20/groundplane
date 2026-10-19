package imagedelivery

import (
	"context"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/distribution/reference"
)

func (service *Service) prepareOperation(
	ctx context.Context,
	requested string,
	at time.Time,
	remove bool,
) (imageoperations.Input, []byte, error) {
	input := imageoperations.Input{TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		PlanID: ids.New(ids.KindPlan), OperationID: ids.New(ids.KindOperation),
		Placement: workexecution.Placement{Executor: workexecution.Controller}, CreatedAt: at}
	if remove {
		if err := service.checkRemoval(ctx, requested, ""); err != nil {
			return imageoperations.Input{}, nil, err
		}
		input.Action, input.RemoveID = workexecution.ActionRemove, requested
		if err := input.Validate(); err != nil {
			return imageoperations.Input{}, nil, err
		}
		body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: input.TaskID})
		return input, body, err
	}
	named, err := imagefetch.ParseRequested(requested)
	if err != nil {
		return imageoperations.Input{}, nil, err
	}
	if reference.Domain(named) == "localhost:5000" {
		return imageoperations.Input{}, nil, errs.New(
			errs.KindValidationFailed,
			"Use registry.groundplane.internal:5000 to fetch from GP's private registry; localhost:5000 is the installer's host-local reference",
		)
	}
	plan, err := service.registry.Resolve(ctx, requested)
	if err != nil {
		return imageoperations.Input{}, nil, err
	}
	if plan.Requested != requested {
		return imageoperations.Input{}, nil, errs.New(errs.KindInternal, "registry resolved another image request")
	}
	input.Action, input.Fetch = workexecution.ActionFetch, &plan
	if err := input.Validate(); err != nil {
		return imageoperations.Input{}, nil, err
	}
	body, err := json.Marshal(
		apiTypes.ImageFetchAccepted{TaskID: input.TaskID, Image: plan.Reference(), ConfigDigest: plan.ConfigDigest},
	)
	return input, body, err
}
