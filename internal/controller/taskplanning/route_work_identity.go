package taskplanning

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// NewRouteConfigurationWork names the operator's Route action while assigning
// its router effect to the owning Component. Both resources remain claimed.
func NewRouteConfigurationWork(
	scope workexecution.Scope,
	placement workexecution.Placement,
	claim blueprints.EnvironmentBlueprintStageClaim,
	action workexecution.Action,
	routeID, routeName, componentID string,
) (workexecution.Task, workexecution.Item, error) {
	if placement.Executor != workexecution.Agent || ids.Validate(ids.KindComponent, componentID) != nil {
		return workexecution.Task{}, workexecution.Item{}, errs.New(
			errs.KindValidationFailed,
			"Route configuration requires its router host",
		)
	}
	return newRouteWork(scope, placement, claim, action, routeID, routeName, componentID)
}

// NewRouteDefinitionWork represents a Route without a serving router. Its
// resource mutation and completion are committed together by the Controller.
func NewRouteDefinitionWork(scope workexecution.Scope, claim blueprints.EnvironmentBlueprintStageClaim,
	action workexecution.Action, routeID, routeName string,
) (workexecution.Task, workexecution.Item, error) {
	return newRouteWork(
		scope,
		workexecution.Placement{Executor: workexecution.Controller},
		claim,
		action,
		routeID,
		routeName,
		"",
	)
}

func newRouteWork(
	scope workexecution.Scope,
	placement workexecution.Placement,
	claim blueprints.EnvironmentBlueprintStageClaim,
	action workexecution.Action,
	routeID, routeName, componentID string,
) (workexecution.Task, workexecution.Item, error) {
	if blueprints.ValidateEnvironmentBlueprintStageClaim(claim) != nil ||
		claim.SourceKind != blueprints.EnvironmentBlueprintSourceMutation ||
		claim.EnvironmentID != scope.EnvironmentID || claim.RevisionID != claim.TaskID ||
		ids.Validate(ids.KindRoute, routeID) != nil ||
		(action != workexecution.ActionCreate && action != workexecution.ActionUpdate && action != workexecution.ActionRemove) {
		return workexecution.Task{}, workexecution.Item{}, errs.New(
			errs.KindValidationFailed,
			"Route action requires its sealed mutation and router host",
		)
	}
	task := workexecution.Task{
		ID: claim.TaskID, Actor: workexecution.OperatorActor, Scope: scope, Action: action,
		TargetID: routeID, TargetName: routeName,
		Intent: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, claim.CreatedAt, claim.TaskID, "request-intent"),
			SHA256: claim.Intent.CiphertextDigest,
		},
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt,
		Deadline: claim.CreatedAt.Add(
			time.Duration(desiredrevision.TaskTimeoutSeconds) * time.Second,
		), ExpectedItems: 1,
	}
	item := workexecution.Item{
		ID: ids.DeriveAt(ids.KindWorkItem, claim.CreatedAt, claim.TaskID, "route-configuration"), TaskID: task.ID,
		Operation: workexecution.Component, TargetID: componentID, Placement: placement,
		Phase: workexecution.Waiting, Cleanup: workexecution.CleanupNone, Wait: &workexecution.Wait{Reason: workexecution.WaitInput},
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline,
		Claims: []workexecution.Claim{{ScopeID: scope.EnvironmentID}, {ScopeID: routeID}},
	}
	if placement.Executor == workexecution.Agent {
		item.Claims = append(
			item.Claims,
			workexecution.Claim{HostID: placement.HostID, ScopeID: scope.EnvironmentID},
			workexecution.Claim{
				ScopeID: componentID,
			},
			workexecution.Claim{HostID: placement.HostID, ScopeID: componentID},
		)
	} else {
		item.TargetID = routeID
	}
	if err := workexecution.ValidateGraph(task, []workexecution.Item{item}); err != nil {
		return workexecution.Task{}, workexecution.Item{}, err
	}
	return task, item, nil
}
