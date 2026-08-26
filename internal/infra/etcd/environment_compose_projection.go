package etcd

import (
	"context"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentComposeIdentity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type EnvironmentRouteIdentity struct {
	ID   string `json:"id"`
	Host string `json:"host,omitempty"`
	Path string `json:"path"`
}

// EnvironmentComposeProjection is the sorted durable input for one Environment render.
type EnvironmentComposeProjection struct {
	EnvironmentID       string                       `json:"environment_id"`
	BlueprintRevisionID string                       `json:"blueprint_revision_id"`
	RenderGeneration    uint64                       `json:"render_generation"`
	Services            []EnvironmentComposeIdentity `json:"services,omitempty"`
	Networks            []EnvironmentComposeIdentity `json:"networks,omitempty"`
	Volumes             []EnvironmentComposeIdentity `json:"volumes,omitempty"`
	Routes              []EnvironmentRouteIdentity   `json:"routes,omitempty"`
	SuppressedRoutes    []EnvironmentRouteIdentity   `json:"suppressed_routes,omitempty"`
	Components          []ComponentRecord            `json:"components,omitempty"`
	Entries             []EntryRecord                `json:"entries,omitempty"`
	core.ServiceDependencyPlans
}

func environmentComposeProjectionKey(environmentID string) string {
	return "/v1/records/environment-compose-projections/" + environmentID
}

func (repository *HierarchyRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (Versioned[EnvironmentComposeProjection], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	result, err := repository.store.Get(ctx, environmentComposeProjectionKey(environmentID))
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if result == nil {
		return Versioned[EnvironmentComposeProjection]{}, false, errs.New(
			errs.KindInternal,
			"Environment Compose projection read is empty",
		)
	}
	if result.Entry == nil {
		return Versioned[EnvironmentComposeProjection]{ReadRevision: result.ReadRevision}, false, nil
	}
	projection, err := decodeEnvironmentComposeProjection(result.Entry.Value)
	if err != nil || projection.EnvironmentID != environmentID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	return Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

func encodeEnvironmentComposeProjection(projection EnvironmentComposeProjection) ([]byte, error) {
	if err := validateEnvironmentComposeProjection(projection); err != nil {
		return nil, err
	}
	return encodeEnvelope("environment-compose-projection", projection)
}

func decodeEnvironmentComposeProjection(value []byte) (EnvironmentComposeProjection, error) {
	projection, err := decodeEnvelope[EnvironmentComposeProjection](value, "environment-compose-projection")
	if err != nil {
		return EnvironmentComposeProjection{}, err
	}
	if err := validateEnvironmentComposeProjection(projection); err != nil {
		return EnvironmentComposeProjection{}, corruptEnvironmentComposeProjection()
	}
	return projection, nil
}

func validateEnvironmentComposeProjection(projection EnvironmentComposeProjection) error {
	if validateStableID(ids.KindEnvironment, projection.EnvironmentID) != nil ||
		validateStableID(ids.KindTask, projection.BlueprintRevisionID) != nil || projection.RenderGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "Environment Compose projection identity is invalid")
	}
	if err := validateEnvironmentComposeIdentities(ids.KindService, projection.Services); err != nil {
		return err
	}
	names := make([]string, len(projection.Services))
	for index, service := range projection.Services {
		names[index] = service.Name
	}
	if err := projection.ServiceDependencyPlans.Validate(names); err != nil {
		return err
	}
	if err := validateEnvironmentComposeIdentities(ids.KindNetwork, projection.Networks); err != nil {
		return err
	}
	if err := validateEnvironmentComposeIdentities(ids.KindVolume, projection.Volumes); err != nil {
		return err
	}
	if err := validateEnvironmentRouteIdentities(projection.EnvironmentID, projection.Routes); err != nil {
		return err
	}
	if err := validateEnvironmentRouteIdentities(projection.EnvironmentID, projection.SuppressedRoutes); err != nil {
		return err
	}
	if err := validateEnvironmentRouteIdentitySets(projection.Routes, projection.SuppressedRoutes); err != nil {
		return err
	}
	if err := validateEnvironmentComponentProjection(projection.EnvironmentID, projection.Components); err != nil {
		return err
	}
	return validateEnvironmentEntryProjection(projection.EnvironmentID, projection.Entries)
}

// SuppressEnvironmentRoute removes a Route while pinning its old match and advancing generation.
func SuppressEnvironmentRoute(
	current EnvironmentComposeProjection,
	routeID string,
) (EnvironmentComposeProjection, bool, error) {
	if err := validateEnvironmentComposeProjection(current); err != nil {
		return EnvironmentComposeProjection{}, false, err
	}
	if validateStableID(ids.KindRoute, routeID) != nil {
		return EnvironmentComposeProjection{}, false, errs.New(
			errs.KindValidationFailed,
			"suppressed Environment Route id is invalid",
		)
	}
	index := -1
	for candidate := range current.Routes {
		if current.Routes[candidate].ID == routeID {
			index = candidate
			break
		}
	}
	if index < 0 {
		return cloneEnvironmentComposeProjection(current), false, nil
	}
	next := cloneEnvironmentComposeProjection(current)
	removed := next.Routes[index]
	next.Routes = append(next.Routes[:index], next.Routes[index+1:]...)
	next.SuppressedRoutes = append(next.SuppressedRoutes, removed)
	sort.Slice(next.SuppressedRoutes, func(left int, right int) bool {
		return environmentRouteMatch(next.SuppressedRoutes[left]) < environmentRouteMatch(next.SuppressedRoutes[right])
	})
	next.RenderGeneration++
	if err := validateEnvironmentComposeProjectionAdvance(current, true, next); err != nil {
		return EnvironmentComposeProjection{}, false, err
	}
	return next, true, nil
}

// RemoveEnvironmentEntry prepares the next applied projection for Entry removal.
// The removed generation remains until terminal acknowledgement promotes next.
func RemoveEnvironmentEntry(
	current EnvironmentComposeProjection,
	entryID string,
) (EnvironmentComposeProjection, bool, error) {
	if err := validateEnvironmentComposeProjection(current); err != nil {
		return EnvironmentComposeProjection{}, false, err
	}
	if validateStableID(ids.KindEnvEntry, entryID) != nil {
		return EnvironmentComposeProjection{}, false, errs.New(
			errs.KindValidationFailed,
			"removed Environment Entry id is invalid",
		)
	}
	index := -1
	for candidate := range current.Entries {
		if current.Entries[candidate].Entry.ID == entryID {
			index = candidate
			break
		}
	}
	if index < 0 {
		return cloneEnvironmentComposeProjection(current), false, nil
	}
	next := cloneEnvironmentComposeProjection(current)
	next.Entries = append(next.Entries[:index], next.Entries[index+1:]...)
	next.RenderGeneration++
	if err := validateEnvironmentComposeProjectionAdvance(current, true, next); err != nil {
		return EnvironmentComposeProjection{}, false, err
	}
	return next, true, nil
}

func validateEnvironmentEntryProjection(environmentID string, values []EntryRecord) error {
	previousID := ""
	for _, value := range values {
		if value.Entry.ID <= previousID || value.EnvironmentID != environmentID || validateEntryRecord(value) != nil {
			return errs.New(errs.KindValidationFailed, "Environment Entry projection is invalid or unsorted")
		}
		previousID = value.Entry.ID
	}
	return nil
}

func validateEnvironmentComponentProjection(environmentID string, values []ComponentRecord) error {
	if len(values) == 0 {
		return nil
	}
	if len(values) != 2 {
		return errs.New(errs.KindValidationFailed, "Environment Component projection must contain both singletons")
	}
	previousKind := core.ComponentKind("")
	seenIDs := make(map[string]struct{}, len(values))
	for _, value := range values {
		kind := value.Desired.Kind
		if validateComponentRecord(value) != nil || value.Desired.Owner != core.ComponentOwnerEnvironment ||
			value.Desired.OwnerID != environmentID || kind <= previousKind ||
			(kind != core.ComponentKindIngressCaddy && kind != core.ComponentKindEdgeCloudflare) {
			return errs.New(errs.KindValidationFailed, "Environment Component projection is invalid or unsorted")
		}
		if _, duplicate := seenIDs[value.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Component projection id is duplicated")
		}
		seenIDs[value.Desired.ID] = struct{}{}
		previousKind = kind
	}
	return nil
}

func validateEnvironmentRouteIdentities(environmentID string, values []EnvironmentRouteIdentity) error {
	previousMatch := ""
	idsSeen := make(map[string]struct{}, len(values))
	for _, value := range values {
		match := environmentRouteMatch(value)
		record := RouteRecord{EnvironmentID: environmentID, Desired: core.Route{
			ID: value.ID, Host: value.Host, Path: value.Path,
			TargetServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", TargetPort: 1, Exposure: "internal",
		}}
		if match <= previousMatch || validateRouteRecord(record) != nil {
			return errs.New(errs.KindValidationFailed, "Environment Route identities are invalid or unsorted")
		}
		if _, duplicate := idsSeen[value.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Route identity id is duplicated")
		}
		idsSeen[value.ID] = struct{}{}
		previousMatch = match
	}
	return nil
}

func validateEnvironmentRouteIdentitySets(
	active []EnvironmentRouteIdentity,
	suppressed []EnvironmentRouteIdentity,
) error {
	idsSeen := make(map[string]struct{}, len(active))
	matches := make(map[string]struct{}, len(active))
	for _, value := range active {
		idsSeen[value.ID] = struct{}{}
		matches[environmentRouteMatch(value)] = struct{}{}
	}
	for _, value := range suppressed {
		if _, duplicate := idsSeen[value.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Route identity sets repeat an id")
		}
		if _, duplicate := matches[environmentRouteMatch(value)]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Route identity sets repeat a match")
		}
	}
	return nil
}

func environmentRouteMatch(value EnvironmentRouteIdentity) string {
	return value.Host + "\x00" + value.Path
}

func cloneEnvironmentComposeProjection(source EnvironmentComposeProjection) EnvironmentComposeProjection {
	clone := source
	clone.ServiceDependencyPlans = source.ServiceDependencyPlans.Clone()
	clone.Services = append([]EnvironmentComposeIdentity(nil), source.Services...)
	clone.Networks = append([]EnvironmentComposeIdentity(nil), source.Networks...)
	clone.Volumes = append([]EnvironmentComposeIdentity(nil), source.Volumes...)
	clone.Routes = append([]EnvironmentRouteIdentity(nil), source.Routes...)
	clone.SuppressedRoutes = append([]EnvironmentRouteIdentity(nil), source.SuppressedRoutes...)
	clone.Components = make([]ComponentRecord, len(source.Components))
	for index, component := range source.Components {
		clone.Components[index] = cloneComponentTaskRecord(component)
	}
	clone.Entries = make([]EntryRecord, len(source.Entries))
	for index, entry := range source.Entries {
		clone.Entries[index] = cloneEntryRecord(entry)
	}
	return clone
}

func validateEnvironmentComposeIdentities(kind ids.Kind, values []EnvironmentComposeIdentity) error {
	previousName := ""
	idsSeen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if validateStableID(kind, value.ID) != nil || value.Name <= previousName ||
			!core.ValidEnvironmentComposeName(value.Name) {
			return errs.New(errs.KindValidationFailed, "Environment Compose identities are invalid or unsorted")
		}
		if _, duplicate := idsSeen[value.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment Compose identity id is duplicated")
		}
		idsSeen[value.ID] = struct{}{}
		previousName = value.Name
	}
	return nil
}

func validateEnvironmentComposeProjectionAdvance(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
) error {
	if err := validateEnvironmentComposeProjection(next); err != nil {
		return err
	}
	if !hasPrevious {
		if next.RenderGeneration != 1 {
			return errs.New(errs.KindStateConflict, "first Environment render generation must be one")
		}
		return nil
	}
	if previous.EnvironmentID != next.EnvironmentID || next.RenderGeneration != previous.RenderGeneration+1 {
		return errs.New(errs.KindStateConflict, "Environment render generation did not advance exactly once")
	}
	if err := preserveEnvironmentComposeIdentities(
		"service",
		previous.Services,
		next.Services,
		removedComponentGeneratedServiceIDs(previous.Components, next.Components),
	); err != nil {
		return err
	}
	if err := preserveEnvironmentComposeIdentities("network", previous.Networks, next.Networks, nil); err != nil {
		return err
	}
	return preserveEnvironmentComposeIdentities("volume", previous.Volumes, next.Volumes, nil)
}

func removedComponentGeneratedServiceIDs(
	previous []ComponentRecord,
	next []ComponentRecord,
) map[string]struct{} {
	retained := make(map[string]struct{})
	for _, component := range next {
		for _, serviceID := range component.Runtime.GeneratedServices {
			retained[serviceID] = struct{}{}
		}
	}
	removed := make(map[string]struct{})
	for _, component := range previous {
		for _, serviceID := range component.Runtime.GeneratedServices {
			if _, keep := retained[serviceID]; !keep {
				removed[serviceID] = struct{}{}
			}
		}
	}
	return removed
}

func preserveEnvironmentComposeIdentities(
	kind string,
	previous []EnvironmentComposeIdentity,
	next []EnvironmentComposeIdentity,
	allowedRemovedIDs map[string]struct{},
) error {
	byName := make(map[string]string, len(next))
	for _, identity := range next {
		byName[identity.Name] = identity.ID
	}
	for _, identity := range previous {
		nextID, exists := byName[identity.Name]
		if !exists {
			if _, allowed := allowedRemovedIDs[identity.ID]; allowed {
				continue
			}
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing %s %s; remove it explicitly before apply",
				kind,
				identity.Name,
			)
		}
		if nextID != identity.ID {
			return errs.New(errs.KindStateConflict, "Environment Compose stable identity changed")
		}
	}
	return nil
}

func corruptEnvironmentComposeProjection() error {
	return errs.New(errs.KindInternal, "Environment Compose projection is corrupt")
}
