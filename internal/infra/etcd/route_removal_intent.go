package etcd

import (
	"context"
	"reflect"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const routeRemovalIntentPrefix = "/v1/records/route-removal-intents/"

// RouteRemovalIntent is the private immutable candidate owned by one removal
// Task. Public Route and applied projection state stay active until successful
// terminal acknowledgement promotes CandidateProjection and removes Route.
type RouteRemovalIntent struct {
	TaskID                    string                        `json:"task_id"`
	EnvironmentID             string                        `json:"environment_id"`
	RouteID                   string                        `json:"route_id"`
	RouteRevision             int64                         `json:"route_revision"`
	CurrentProjectionRevision int64                         `json:"current_projection_revision,omitempty"`
	CurrentProjection         *EnvironmentComposeProjection `json:"current_projection,omitempty"`
	CandidateProjection       *EnvironmentComposeProjection `json:"candidate_projection,omitempty"`
	Provider                  *RouteProviderPin             `json:"provider,omitempty"`
	Status                    TaskStatus                    `json:"status"`
	CreatedAt                 time.Time                     `json:"created_at"`
	TerminalAt                *time.Time                    `json:"terminal_at,omitempty"`
}

type RouteRemovalTaskPreparation struct {
	Intent RouteRemovalIntent
	Task   TaskRecord
}

func NewRouteRemovalIntent(taskID, environmentID, routeID string, routeRevision int64, projection *Versioned[EnvironmentComposeProjection], createdAt time.Time) (RouteRemovalIntent, error) {
	intent := RouteRemovalIntent{TaskID: taskID, EnvironmentID: environmentID, RouteID: routeID, RouteRevision: routeRevision, Status: TaskStatusPending, CreatedAt: createdAt}
	if projection != nil {
		candidate, changed, err := SuppressEnvironmentRoute(projection.Record, routeID)
		if err != nil {
			return RouteRemovalIntent{}, err
		}
		if changed {
			current := cloneEnvironmentComposeProjection(projection.Record)
			intent.CurrentProjectionRevision = projection.Revision
			intent.CurrentProjection = &current
			intent.CandidateProjection = &candidate
		}
	}
	if err := validateRouteRemovalIntent(intent); err != nil {
		return RouteRemovalIntent{}, err
	}
	return cloneRouteRemovalIntent(intent), nil
}

func routeRemovalIntentKey(taskID string) string { return routeRemovalIntentPrefix + taskID }

func (repository *HierarchyRepository) GetRouteRemovalIntent(ctx context.Context, taskID string) (Versioned[RouteRemovalIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RouteRemovalIntent]{}, false, err
	}
	if validateStableID(ids.KindTask, taskID) != nil {
		return Versioned[RouteRemovalIntent]{}, false, errs.New(errs.KindValidationFailed, "Route removal intent Task id is invalid")
	}
	result, err := repository.store.Get(ctx, routeRemovalIntentKey(taskID))
	if err != nil {
		return Versioned[RouteRemovalIntent]{}, false, err
	}
	if result == nil {
		return Versioned[RouteRemovalIntent]{}, false, errs.New(errs.KindInternal, "Route removal intent read is empty")
	}
	if result.Entry == nil {
		return Versioned[RouteRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeRouteRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return Versioned[RouteRemovalIntent]{}, false, corruptRouteRemovalIntent()
	}
	return Versioned[RouteRemovalIntent]{Record: intent, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision}, true, nil
}

func terminalRouteRemovalIntent(intent RouteRemovalIntent, status TaskStatus, terminalAt time.Time) (RouteRemovalIntent, error) {
	if intent.Status != TaskStatusPending || !isTerminalTaskStatus(status) {
		return RouteRemovalIntent{}, errs.New(errs.KindStateConflict, "Route removal intent is not pending")
	}
	terminal := cloneRouteRemovalIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(terminalAt)
	if err := validateRouteRemovalIntent(terminal); err != nil {
		return RouteRemovalIntent{}, err
	}
	return terminal, nil
}

func encodeRouteRemovalIntent(intent RouteRemovalIntent) ([]byte, error) {
	if err := validateRouteRemovalIntent(intent); err != nil {
		return nil, err
	}
	return encodeEnvelope("route_removal_intent", intent)
}

func decodeRouteRemovalIntent(value []byte) (RouteRemovalIntent, error) {
	intent, err := decodeEnvelope[RouteRemovalIntent](value, "route_removal_intent")
	if err != nil {
		return RouteRemovalIntent{}, err
	}
	if err := validateRouteRemovalIntent(intent); err != nil {
		return RouteRemovalIntent{}, corruptRouteRemovalIntent()
	}
	return intent, nil
}

func validateRouteRemovalIntent(intent RouteRemovalIntent) error {
	if validateStableID(ids.KindTask, intent.TaskID) != nil || validateStableID(ids.KindEnvironment, intent.EnvironmentID) != nil || validateStableID(ids.KindRoute, intent.RouteID) != nil || intent.RouteRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Route removal intent identity is invalid")
	}
	if err := validateTimestamp("Route removal intent created_at", intent.CreatedAt); err != nil {
		return err
	}
	if intent.Status == TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Route removal intent has a terminal timestamp")
		}
	} else if !isTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil || intent.TerminalAt.Before(intent.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Route removal intent terminal state is invalid")
	} else if err := validateTimestamp("Route removal intent terminal_at", *intent.TerminalAt); err != nil {
		return err
	}
	if intent.CurrentProjection == nil || intent.CandidateProjection == nil {
		if intent.CurrentProjection != nil || intent.CandidateProjection != nil || intent.CurrentProjectionRevision != 0 || intent.Provider != nil {
			return errs.New(errs.KindValidationFailed, "Route removal intent projection state is incomplete")
		}
		return nil
	}
	if intent.CurrentProjectionRevision <= 0 || intent.CurrentProjection.EnvironmentID != intent.EnvironmentID || intent.CandidateProjection.EnvironmentID != intent.EnvironmentID || validateEnvironmentComposeProjection(*intent.CurrentProjection) != nil || validateEnvironmentComposeProjection(*intent.CandidateProjection) != nil {
		return errs.New(errs.KindValidationFailed, "Route removal intent projection is invalid")
	}
	expected, changed, err := SuppressEnvironmentRoute(*intent.CurrentProjection, intent.RouteID)
	if err != nil || !changed || !sameRouteRemovalProjection(expected, *intent.CandidateProjection) {
		return errs.New(errs.KindValidationFailed, "Route removal candidate projection changed")
	}
	if intent.Provider != nil && validateRouteProviderPin(intent.Provider) != nil {
		return errs.New(errs.KindValidationFailed, "Route removal provider pin is invalid")
	}
	return nil
}

func sameRouteRemovalProjection(left, right EnvironmentComposeProjection) bool {
	normalize := func(value EnvironmentComposeProjection) EnvironmentComposeProjection {
		value = cloneEnvironmentComposeProjection(value)
		if len(value.Services) == 0 {
			value.Services = nil
		}
		if len(value.Networks) == 0 {
			value.Networks = nil
		}
		if len(value.Volumes) == 0 {
			value.Volumes = nil
		}
		if len(value.Routes) == 0 {
			value.Routes = nil
		}
		if len(value.SuppressedRoutes) == 0 {
			value.SuppressedRoutes = nil
		}
		if len(value.Components) == 0 {
			value.Components = nil
		}
		if len(value.Entries) == 0 {
			value.Entries = nil
		}
		return value
	}
	return reflect.DeepEqual(normalize(left), normalize(right))
}

func cloneRouteRemovalIntent(source RouteRemovalIntent) RouteRemovalIntent {
	clone := source
	if source.CurrentProjection != nil {
		value := cloneEnvironmentComposeProjection(*source.CurrentProjection)
		clone.CurrentProjection = &value
	}
	if source.CandidateProjection != nil {
		value := cloneEnvironmentComposeProjection(*source.CandidateProjection)
		clone.CandidateProjection = &value
	}
	if source.Provider != nil {
		value := cloneRouteProviderPin(*source.Provider)
		clone.Provider = &value
	}
	clone.TerminalAt = cloneTimePointer(source.TerminalAt)
	return clone
}

func corruptRouteRemovalIntent() error {
	return errs.New(errs.KindInternal, "Route removal intent is corrupt")
}
