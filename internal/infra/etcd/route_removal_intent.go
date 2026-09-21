package etcd

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const routeRemovalIntentPrefix = "/v1/records/route-removal-intents/"

// RouteRemovalIntent is the private immutable candidate owned by one removal
// Task. Public Route and applied projection state stay active until successful
// terminal acknowledgement promotes CandidateProjection and removes Route.
type RouteRemovalIntent struct {
	TaskID                    string                                         `json:"task_id"`
	EnvironmentID             string                                         `json:"environment_id"`
	RouteID                   string                                         `json:"route_id"`
	RouteRevision             int64                                          `json:"route_revision"`
	CurrentProjectionRevision int64                                          `json:"current_projection_revision,omitempty"`
	CurrentProjection         *projectionrecord.EnvironmentComposeProjection `json:"current_projection,omitempty"`
	CandidateProjection       *projectionrecord.EnvironmentComposeProjection `json:"candidate_projection,omitempty"`
	Provider                  *RouteProviderPin                              `json:"provider,omitempty"`
	Status                    TaskStatus                                     `json:"status"`
	CreatedAt                 time.Time                                      `json:"created_at"`
	TerminalAt                *time.Time                                     `json:"terminal_at,omitempty"`
}

type RouteRemovalTaskPreparation struct {
	Intent RouteRemovalIntent
	Task   TaskRecord
}

func NewRouteRemovalIntent(
	taskID, environmentID, routeID string,
	routeRevision int64,
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	createdAt time.Time,
) (RouteRemovalIntent, error) {
	intent := RouteRemovalIntent{
		TaskID:        taskID,
		EnvironmentID: environmentID,
		RouteID:       routeID,
		RouteRevision: routeRevision,
		Status:        TaskStatusPending,
		CreatedAt:     createdAt,
	}
	if projection != nil {
		candidate, changed, err := removeEnvironmentDesiredRoute(projection.Record, routeID)
		if err != nil {
			return RouteRemovalIntent{}, err
		}
		if changed {
			candidate.RevisionID = taskID
			current := projectionrecord.CloneEnvironmentComposeProjection(projection.Record)
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

func (repository *HierarchyRepository) GetRouteRemovalIntent(
	ctx context.Context,
	taskID string,
) (etcdstore.Versioned[RouteRemovalIntent], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[RouteRemovalIntent]{}, false, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[RouteRemovalIntent]{}, false, errs.New(
			errs.KindValidationFailed,
			"Route removal intent Task id is invalid",
		)
	}
	result, err := repository.store.Get(ctx, routeRemovalIntentKey(taskID))
	if err != nil {
		return etcdstore.Versioned[RouteRemovalIntent]{}, false, err
	}
	if result == nil {
		return etcdstore.Versioned[RouteRemovalIntent]{}, false, errs.New(errs.KindInternal, "Route removal intent read is empty")
	}
	if result.Entry == nil {
		return etcdstore.Versioned[RouteRemovalIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeRouteRemovalIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return etcdstore.Versioned[RouteRemovalIntent]{}, false, corruptRouteRemovalIntent()
	}
	return etcdstore.Versioned[RouteRemovalIntent]{
		Record:       intent,
		Revision:     result.Entry.ModRevision,
		ReadRevision: result.ReadRevision,
	}, true, nil
}

func terminalRouteRemovalIntent(
	intent RouteRemovalIntent,
	status TaskStatus,
	terminalAt time.Time,
) (RouteRemovalIntent, error) {
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
	return recordcodec.Encode("route_removal_intent", intent)
}

func decodeRouteRemovalIntent(value []byte) (RouteRemovalIntent, error) {
	intent, err := recordcodec.Decode[RouteRemovalIntent](value, "route_removal_intent")
	if err != nil {
		return RouteRemovalIntent{}, err
	}
	if err := validateRouteRemovalIntent(intent); err != nil {
		return RouteRemovalIntent{}, corruptRouteRemovalIntent()
	}
	return intent, nil
}

func validateRouteRemovalIntent(intent RouteRemovalIntent) error {
	if recordcodec.ValidateID(ids.KindTask, intent.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindRoute, intent.RouteID) != nil ||
		intent.RouteRevision <= 0 {
		return errs.New(errs.KindValidationFailed, "Route removal intent identity is invalid")
	}
	if err := recordcodec.ValidateTimestamp("Route removal intent created_at", intent.CreatedAt); err != nil {
		return err
	}
	if intent.Status == TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Route removal intent has a terminal timestamp")
		}
	} else if !isTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil || intent.TerminalAt.Before(intent.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Route removal intent terminal state is invalid")
	} else if err := recordcodec.ValidateTimestamp("Route removal intent terminal_at", *intent.TerminalAt); err != nil {
		return err
	}
	if intent.CurrentProjection == nil || intent.CandidateProjection == nil {
		if intent.CurrentProjection != nil || intent.CandidateProjection != nil ||
			intent.CurrentProjectionRevision != 0 ||
			intent.Provider != nil {
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

func sameRouteRemovalProjection(left, right projectionrecord.EnvironmentComposeProjection) bool {
	return sameServiceRemovalProjection(left, right)
}

func removeEnvironmentDesiredRoute(
	current projectionrecord.EnvironmentComposeProjection,
	routeID string,
) (projectionrecord.EnvironmentComposeProjection, bool, error) {
	if err := projectionrecord.ValidateEnvironmentComposeProjection(current); err != nil {
		return projectionrecord.EnvironmentComposeProjection{}, false, err
	}
	if recordcodec.ValidateID(ids.KindRoute, routeID) != nil {
		return projectionrecord.EnvironmentComposeProjection{}, false, errs.New(
			errs.KindValidationFailed,
			"removed Environment Route id is invalid",
		)
	}
	next := projectionrecord.CloneEnvironmentComposeProjection(current)
	next.DesiredRoutes = nil
	removed := false
	for _, desired := range current.DesiredRoutes {
		if desired.Desired.ID == routeID {
			if removed {
				return projectionrecord.EnvironmentComposeProjection{}, false, errs.New(
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

func validateRouteRemovalDesiredProjection(projection projectionrecord.EnvironmentComposeProjection) error {
	if recordcodec.ValidateID(ids.KindEnvironment, projection.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, projection.RevisionID) != nil || projection.RenderGeneration == 0 ||
		len(
			projection.ComposeArtifact,
		) == 0 || projectionrecord.ValidateEnvironmentNormalizedCompose(projection.NormalizedCompose) != nil {
		return errs.New(errs.KindValidationFailed, "Route removal desired projection is invalid")
	}
	previousMatch := ""
	seen := make(map[string]struct{}, len(projection.DesiredRoutes))
	for _, desired := range projection.DesiredRoutes {
		match := desired.Desired.Host + "\x00" + desired.Desired.Path
		record := routerecord.Record{
			EnvironmentID: desired.EnvironmentID, Desired: desired.Desired,
			DesiredGeneration: desired.DesiredGeneration,
			Observed: routerecord.Observation{
				Status: routerecord.ObservedUnserved, DesiredGeneration: desired.DesiredGeneration,
			},
		}
		if desired.EnvironmentID != projection.EnvironmentID || match <= previousMatch ||
			routerecord.ValidateRecord(record) != nil {
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
		value := projectionrecord.CloneEnvironmentComposeProjection(*source.CurrentProjection)
		clone.CurrentProjection = &value
	}
	if source.CandidateProjection != nil {
		value := projectionrecord.CloneEnvironmentComposeProjection(*source.CandidateProjection)
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
