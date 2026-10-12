package apiclient

import (
	"github.com/AlanD20/groundplane/internal/cli/apiclient/generated"
	"github.com/AlanD20/groundplane/internal/common/ids"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func taskExecutionsFromGenerated(page *generated.PageTaskExecution) (*apiTypes.Page[apiTypes.TaskExecution], error) {
	if page == nil {
		return nil, nil
	}
	if page.Items == nil {
		return nil, errs.New(errs.KindInternal, "apiclient: Blueprint execution page has no items")
	}
	result := &apiTypes.Page[apiTypes.TaskExecution]{
		Items:      make([]apiTypes.TaskExecution, 0, len(*page.Items)),
		NextCursor: valueOrEmpty(page.NextCursor),
	}
	if page.Revision != nil {
		result.Revision = *page.Revision
	}
	for _, child := range *page.Items {
		if ids.Validate(ids.KindTask, child.Id) != nil || !generated.TaskType(child.Type).Valid() {
			return nil, errs.New(errs.KindInternal, "apiclient: Blueprint execution identity is invalid")
		}
		unit := apiTypes.TaskExecution{
			ID: child.Id, Type: child.Type, Target: child.Target, TargetName: valueOrEmpty(child.TargetName),
			ResourceKind: valueOrEmpty(child.ResourceKind), Status: apiTypes.TaskStatus(child.Status),
			CreatedAt: child.CreatedAt, UpdatedAt: child.UpdatedAt, StartedAt: child.StartedAt, FinishedAt: child.FinishedAt,
			Failure: taskFailureFromGenerated(child.Failure), Recovery: taskRecoveryFromGenerated(child.Recovery),
			ReconciliationRequired: child.ReconciliationRequired,
		}
		if child.Steps != nil {
			for _, step := range *child.Steps {
				converted, err := taskStepFromGenerated(step)
				if err != nil {
					return nil, err
				}
				unit.Steps = append(unit.Steps, converted)
			}
		}
		result.Items = append(result.Items, unit)
	}
	return result, nil
}
