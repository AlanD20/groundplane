package etcd

import (
	"context"
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
		candidate, changed, err := removeEnvironmentDesiredRoute(projection.Record, routeID)
		if err != nil {
			return RouteRemovalIntent{}, err
		}
		if changed {
			candidate.RevisionID = taskID
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
	if intent.CurrentProjectionRevision <= 0 || intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID ||
		validateRouteRemovalDesiredProjection(*intent.CurrentProjection) != nil ||
		validateRouteRemovalDesiredProjection(*intent.CandidateProjection) != nil {
		return errs.New(errs.KindValidationFailed, "Route removal intent projection is invalid")
	}
	expected, changed, err := removeEnvironmentDesiredRoute(*intent.CurrentProjection, intent.RouteID)
	if err != nil || !changed {
		return errs.New(errs.KindValidationFailed, "Route removal candidate projection changed")
	}
	expected.RevisionID = intent.CandidateProjection.RevisionID
	if !sameRouteRemovalProjection(expected, *intent.CandidateProjection) {
		return errs.New(errs.KindValidationFailed, "Route removal candidate projection changed")
	}
	if intent.Provider != nil && validateRouteProviderPin(intent.Provider) != nil {
		return errs.New(errs.KindValidationFailed, "Route removal provider pin is invalid")
	}
	return nil
}

func sameRouteRemovalProjection(left, right EnvironmentComposeProjection) bool {
	return sameServiceRemovalProjection(left, right)
}

func removeEnvironmentDesiredRoute(
	current EnvironmentComposeProjection,
	routeID string,
) (EnvironmentComposeProjection, bool, error) {
	if err := validateEnvironmentComposeProjection(current); err != nil {
		return EnvironmentComposeProjection{}, false, err
	}
	if validateStableID(ids.KindRoute, routeID) != nil {
		return EnvironmentComposeProjection{}, false, errs.New(
			errs.KindValidationFailed,
			"removed Environment Route id is invalid",
		)
	}
	next := cloneEnvironmentComposeProjection(current)
	next.DesiredRoutes = nil
	removed := false
	for _, desired := range current.DesiredRoutes {
		if desired.Desired.ID == routeID {
			if removed {
				return EnvironmentComposeProjection{}, false, errs.New(
					errs.KindInternal,
					"Environment Route projection contains duplicate desired identity",
				)
			}
			removed = true
			continue
		}
		next.DesiredRoutes = append(next.DesiredRoutes, desired)
	}
	if !removed {
		return next, false, nil
	}
	next.RenderGeneration++
	return next, true, nil
}

func validateRouteRemovalDesiredProjection(projection EnvironmentComposeProjection) error {
	if validateStableID(ids.KindEnvironment, projection.EnvironmentID) != nil ||
		validateStableID(ids.KindTask, projection.RevisionID) != nil || projection.RenderGeneration == 0 ||
		len(projection.ComposeArtifact) == 0 || validateEnvironmentNormalizedCompose(projection.NormalizedCompose) != nil {
		return errs.New(errs.KindValidationFailed, "Route removal desired projection is invalid")
	}
	previousMatch := ""
	seen := make(map[string]struct{}, len(projection.DesiredRoutes))
	for _, desired := range projection.DesiredRoutes {
		match := desired.Desired.Host + "\x00" + desired.Desired.Path
		record := RouteRecord{
			EnvironmentID: desired.EnvironmentID, Desired: desired.Desired,
			DesiredGeneration: desired.DesiredGeneration,
			Observed: RouteObservation{
				Status: RouteObservedUnserved, DesiredGeneration: desired.DesiredGeneration,
			},
		}
		if desired.EnvironmentID != projection.EnvironmentID || match <= previousMatch ||
			validateRouteRecord(record) != nil {
			return errs.New(errs.KindValidationFailed, "Route removal desired projection is invalid")
		}
		if _, duplicate := seen[desired.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Route removal desired projection is invalid")
		}
		seen[desired.Desired.ID] = struct{}{}
		previousMatch = match
	}
	return nil
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
