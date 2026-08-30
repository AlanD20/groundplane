package etcd

import (
	"context"
	"time"

	componentsdk "github.com/AlanD20/groundplane-component-sdk/component"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const routeMutationIntentPrefix = "/v1/records/route-mutation-intents/"

// RouteMutationKind identifies the desired Route operation pinned by a Task.
type RouteMutationKind string

const (
	RouteMutationCreate RouteMutationKind = "create"
	RouteMutationEdit   RouteMutationKind = "edit"
)

// RouteMutationIntent is the immutable reconciliation input for one Route
// create/edit Task. Route and target data are copied here so restart/retry
// never rebases rendering on mutable desired state.
type RouteMutationIntent struct {
	TaskID                    string                        `json:"task_id"`
	OperationID               string                        `json:"operation_id"`
	EnvironmentID             string                        `json:"environment_id"`
	RouteID                   string                        `json:"route_id"`
	Kind                      RouteMutationKind             `json:"kind"`
	RouteRevision             int64                         `json:"route_revision,omitempty"`
	Route                     RouteRecord                   `json:"route"`
	Previous                  *Versioned[RouteRecord]       `json:"previous,omitempty"`
	CurrentProjectionRevision int64                         `json:"current_projection_revision,omitempty"`
	CurrentProjection         *EnvironmentComposeProjection `json:"current_projection,omitempty"`
	CandidateProjection       *EnvironmentComposeProjection `json:"candidate_projection,omitempty"`
	Provider                  *RouteProviderPin             `json:"provider,omitempty"`
	Status                    TaskStatus                    `json:"status"`
	CreatedAt                 time.Time                     `json:"created_at"`
	TerminalAt                *time.Time                    `json:"terminal_at,omitempty"`
}

type RouteProviderPin struct {
	ComponentID      string                       `json:"component_id"`
	DefinitionDigest string                       `json:"definition_digest"`
	CatalogDigest    string                       `json:"catalog_digest"`
	InputRevision    int64                        `json:"input_revision"`
	InputGeneration  uint64                       `json:"input_generation"`
	Destination      string                       `json:"destination"`
	ActionID         string                       `json:"action_id"`
	ServiceID        string                       `json:"service_id"`
	Input            componentsdk.HTTPRouterInput `json:"input"`
}

// RouteMutationProcedureIDs are allocated before publication by the planner.
// Keeping these ids in the etcd-facing contract makes replay deterministic.
type RouteMutationProcedureIDs struct {
	ArtifactID        string
	MaterializationID string
	MaterializeStepID string
	ApplyStepID       string
	ActivateStepID    string
}

type RouteMutationTaskPreparation struct {
	Intent RouteMutationIntent
	Task   TaskRecord
}

func NewRouteMutationIntent(
	taskID string,
	operationID string,
	environmentID string,
	route RouteRecord,
	previous *Versioned[RouteRecord],
	projection *Versioned[EnvironmentComposeProjection],
	createdAt time.Time,
) (RouteMutationIntent, error) {
	intent := RouteMutationIntent{
		TaskID: taskID, OperationID: operationID, EnvironmentID: environmentID,
		RouteID: route.Desired.ID, Kind: RouteMutationCreate, Route: route,
		Status: TaskStatusPending, CreatedAt: createdAt,
	}
	if previous != nil {
		intent.Kind = RouteMutationEdit
		intent.RouteRevision = previous.Revision
		prior := Versioned[RouteRecord]{
			Record: cloneRouteRecord(previous.Record), Revision: previous.Revision, ReadRevision: previous.ReadRevision,
		}
		intent.Previous = &prior
	}
	if projection != nil {
		current := cloneEnvironmentComposeProjection(projection.Record)
		intent.CurrentProjectionRevision = projection.Revision
		intent.CurrentProjection = &current
	}
	// The planner seals the provider candidate after validating the immutable
	// registered input. Validate the Route operation now, while
	// allowing that one intentionally unsealed planning seam.
	identity := intent
	identity.CurrentProjection = nil
	identity.CandidateProjection = nil
	identity.CurrentProjectionRevision = 0
	identity.Provider = nil
	if err := validateRouteMutationIntent(identity); err != nil {
		return RouteMutationIntent{}, err
	}
	return cloneRouteMutationIntent(intent), nil
}

func routeMutationIntentKey(taskID string) string { return routeMutationIntentPrefix + taskID }

func (repository *HierarchyRepository) GetRouteMutationIntent(
	ctx context.Context,
	taskID string,
) (Versioned[RouteMutationIntent], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[RouteMutationIntent]{}, false, err
	}
	if validateStableID(ids.KindTask, taskID) != nil {
		return Versioned[RouteMutationIntent]{}, false, errs.New(errs.KindValidationFailed, "Route mutation intent Task id is invalid")
	}
	result, err := repository.store.Get(ctx, routeMutationIntentKey(taskID))
	if err != nil {
		return Versioned[RouteMutationIntent]{}, false, err
	}
	if result == nil {
		return Versioned[RouteMutationIntent]{}, false, errs.New(errs.KindInternal, "Route mutation intent read is empty")
	}
	if result.Entry == nil {
		return Versioned[RouteMutationIntent]{ReadRevision: result.ReadRevision}, false, nil
	}
	intent, err := decodeRouteMutationIntent(result.Entry.Value)
	if err != nil || intent.TaskID != taskID {
		return Versioned[RouteMutationIntent]{}, false, corruptRouteMutationIntent()
	}
	return Versioned[RouteMutationIntent]{Record: intent, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision}, true, nil
}

func terminalRouteMutationIntent(intent RouteMutationIntent, status TaskStatus, terminalAt time.Time) (RouteMutationIntent, error) {
	if intent.Status != TaskStatusPending || !isTerminalTaskStatus(status) {
		return RouteMutationIntent{}, errs.New(errs.KindStateConflict, "Route mutation intent is not pending")
	}
	terminal := cloneRouteMutationIntent(intent)
	terminal.Status = status
	terminal.TerminalAt = timePointer(terminalAt)
	if err := validateRouteMutationIntent(terminal); err != nil {
		return RouteMutationIntent{}, err
	}
	return terminal, nil
}

func encodeRouteMutationIntent(intent RouteMutationIntent) ([]byte, error) {
	if err := validateRouteMutationIntent(intent); err != nil {
		return nil, err
	}
	return encodeEnvelope("route_mutation_intent", intent)
}

func decodeRouteMutationIntent(value []byte) (RouteMutationIntent, error) {
	intent, err := decodeEnvelope[RouteMutationIntent](value, "route_mutation_intent")
	if err != nil {
		return RouteMutationIntent{}, err
	}
	if err := validateRouteMutationIntent(intent); err != nil {
		return RouteMutationIntent{}, corruptRouteMutationIntent()
	}
	return intent, nil
}

func validateRouteMutationIntent(intent RouteMutationIntent) error {
	if validateStableID(ids.KindTask, intent.TaskID) != nil ||
		validateStableID(ids.KindOperation, intent.OperationID) != nil ||
		validateStableID(ids.KindEnvironment, intent.EnvironmentID) != nil ||
		validateStableID(ids.KindRoute, intent.RouteID) != nil ||
		intent.Route.EnvironmentID != intent.EnvironmentID || intent.Route.Desired.ID != intent.RouteID {
		return errs.New(errs.KindValidationFailed, "Route mutation intent identity is invalid")
	}
	if err := validateRouteRecord(intent.Route); err != nil {
		return err
	}
	if err := validateTimestamp("Route mutation intent created_at", intent.CreatedAt); err != nil {
		return err
	}
	switch intent.Kind {
	case RouteMutationCreate:
		if intent.Previous != nil || intent.RouteRevision != 0 {
			return errs.New(errs.KindValidationFailed, "Route create intent has edit state")
		}
	case RouteMutationEdit:
		if intent.Previous == nil || intent.RouteRevision <= 0 || intent.Previous.Record.EnvironmentID != intent.EnvironmentID ||
			intent.Previous.Record.Desired.ID != intent.RouteID || intent.Previous.Revision != intent.RouteRevision {
			return errs.New(errs.KindValidationFailed, "Route edit intent has incomplete prior state")
		}
		if err := validateRouteRecord(intent.Previous.Record); err != nil {
			return err
		}
		if intent.Previous.Record.Desired.Host != intent.Route.Desired.Host || intent.Previous.Record.Desired.Path != intent.Route.Desired.Path ||
			intent.Previous.Record.Desired.TargetServiceID != intent.Route.Desired.TargetServiceID || intent.Previous.Record.Desired.TargetPort != intent.Route.Desired.TargetPort {
			return errs.New(errs.KindValidationFailed, "Route edit intent changed immutable route fields")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Route mutation intent kind is invalid")
	}
	if intent.Status == TaskStatusPending {
		if intent.TerminalAt != nil {
			return errs.New(errs.KindValidationFailed, "pending Route mutation intent has a terminal timestamp")
		}
	} else if !isTerminalTaskStatus(intent.Status) || intent.TerminalAt == nil || intent.TerminalAt.Before(intent.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Route mutation intent terminal state is invalid")
	}
	if intent.CurrentProjection == nil || intent.CandidateProjection == nil {
		if intent.CurrentProjection != nil || intent.CandidateProjection != nil || intent.CurrentProjectionRevision != 0 || intent.Provider != nil {
			return errs.New(errs.KindValidationFailed, "Route mutation intent projection state is incomplete")
		}
		return nil
	}
	if intent.CurrentProjectionRevision <= 0 || intent.CurrentProjection.EnvironmentID != intent.EnvironmentID ||
		intent.CandidateProjection.EnvironmentID != intent.EnvironmentID || validateEnvironmentComposeProjection(*intent.CurrentProjection) != nil ||
		validateEnvironmentComposeProjection(*intent.CandidateProjection) != nil || intent.CandidateProjection.RenderGeneration <= intent.CurrentProjection.RenderGeneration ||
		validateRouteProviderPin(intent.Provider) != nil {
		return errs.New(errs.KindValidationFailed, "Route mutation intent projection is invalid")
	}
	return nil
}

func validateRouteProviderPin(provider *RouteProviderPin) error {
	if provider == nil {
		return errs.New(errs.KindValidationFailed, "Route provider pin is missing")
	}
	if validateRouteProviderObservation(&RouteProviderObservation{
		ComponentID: provider.ComponentID, DefinitionDigest: provider.DefinitionDigest,
		CatalogDigest: provider.CatalogDigest, InputRevision: provider.InputRevision,
		InputGeneration: provider.InputGeneration,
	}) != nil || provider.Destination == "" || provider.ActionID == "" ||
		validateStableID(ids.KindService, provider.ServiceID) != nil ||
		provider.Input.ComponentID != provider.ComponentID ||
		componentsdk.ValidateHTTPRouterInput(provider.Input) != nil {
		return errs.New(errs.KindValidationFailed, "Route provider pin is invalid")
	}
	return nil
}

func cloneRouteProviderPin(source RouteProviderPin) RouteProviderPin {
	clone := source
	clone.Input = componentsdk.CloneHTTPRouterInput(source.Input)
	return clone
}

func cloneRouteMutationIntent(source RouteMutationIntent) RouteMutationIntent {
	clone := source
	clone.Route = cloneRouteRecord(source.Route)
	if source.Previous != nil {
		previous := Versioned[RouteRecord]{Record: cloneRouteRecord(source.Previous.Record), Revision: source.Previous.Revision, ReadRevision: source.Previous.ReadRevision}
		clone.Previous = &previous
	}
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

func cloneRouteRecord(source RouteRecord) RouteRecord {
	return source
}

func corruptRouteMutationIntent() error {
	return errs.New(errs.KindInternal, "Route mutation intent is corrupt")
}
