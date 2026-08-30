package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type EnvironmentComposeIdentity struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type EnvironmentVolumeIdentity struct {
	ID   string `json:"id"`
	Slug string `json:"slug"`
	Key  string `json:"key"`
}

type EnvironmentServiceVolumeMount struct {
	ServiceID string `json:"service_id"`
	VolumeID  string `json:"volume_id"`
	Target    string `json:"target"`
	ReadOnly  bool   `json:"read_only"`
}

type EnvironmentRouteIdentity struct {
	ID   string `json:"id"`
	Host string `json:"host,omitempty"`
	Path string `json:"path"`
}

// EnvironmentComposeProjection is the sorted durable input for one Environment render.
type EnvironmentComposeProjection struct {
	EnvironmentID    string                          `json:"environment_id"`
	RevisionID       string                          `json:"blueprint_revision_id"`
	RenderGeneration uint64                          `json:"render_generation"`
	ComposeArtifact  []byte                          `json:"compose_artifact"`
	Services         []EnvironmentComposeIdentity    `json:"services,omitempty"`
	Networks         []EnvironmentComposeIdentity    `json:"networks,omitempty"`
	Volumes          []EnvironmentVolumeIdentity     `json:"volumes,omitempty"`
	VolumeMounts     []EnvironmentServiceVolumeMount `json:"volume_mounts,omitempty"`
	Routes           []EnvironmentRouteIdentity      `json:"routes,omitempty"`
	SuppressedRoutes []EnvironmentRouteIdentity      `json:"suppressed_routes,omitempty"`
	Components       []ComponentRecord               `json:"components,omitempty"`
	Entries          []EntryRecord                   `json:"entries,omitempty"`
	core.ServiceDependencyPlans
}

const environmentComposeProjectionPrefix = "/v1/records/environment-compose-projections/"

func environmentComposeProjectionKey(environmentID string) string {
	return environmentComposeProjectionPrefix + environmentID
}

func (repository *HierarchyRepository) ListEnvironmentAppliedComposeProjections(
	ctx context.Context,
) ([]Versioned[EnvironmentComposeProjection], error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	result := make([]Versioned[EnvironmentComposeProjection], 0)
	start := ""
	var revision int64
	for {
		page, err := repository.store.Range(ctx, RangeRequest{
			Prefix: environmentComposeProjectionPrefix, StartExclusive: start,
			Limit: 128, Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil {
			return nil, errs.New(errs.KindInternal, "Environment applied projection scan is empty")
		}
		if revision == 0 {
			revision = page.ReadRevision
		}
		if page.More && len(page.Values) == 0 {
			return nil, errs.New(errs.KindInternal, "Environment applied projection scan did not advance")
		}
		for _, value := range page.Values {
			if !strings.HasPrefix(value.Key, environmentComposeProjectionPrefix) {
				clearRangeKeyValues(page.Values)
				return nil, corruptEnvironmentComposeProjection()
			}
			environmentID := strings.TrimPrefix(value.Key, environmentComposeProjectionPrefix)
			if ids.Validate(ids.KindEnvironment, environmentID) != nil {
				clearRangeKeyValues(page.Values)
				return nil, corruptEnvironmentComposeProjection()
			}
			projection, err := decodeEnvironmentComposeProjection(value.Value)
			if err != nil || projection.EnvironmentID != environmentID {
				clearRangeKeyValues(page.Values)
				return nil, corruptEnvironmentComposeProjection()
			}
			result = append(result, Versioned[EnvironmentComposeProjection]{
				Record: projection, Revision: value.ModRevision, ReadRevision: revision,
			})
			start = value.Key
		}
		clearRangeKeyValues(page.Values)
		if !page.More {
			return result, nil
		}
	}
}

// GetEnvironmentAppliedComposeProjection returns the mutable projection last
// acknowledged by the runtime. It is distinct from the immutable desired
// Blueprint head returned by GetEnvironmentComposeProjection.
func (repository *HierarchyRepository) GetEnvironmentAppliedComposeProjection(
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
	if result == nil || result.Entry == nil {
		readRevision := int64(0)
		if result != nil {
			readRevision = result.ReadRevision
		}
		return Versioned[EnvironmentComposeProjection]{ReadRevision: readRevision}, false, nil
	}
	projection, err := decodeEnvironmentComposeProjection(result.Entry.Value)
	if err != nil || projection.EnvironmentID != environmentID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	return Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
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
	head, err := repository.store.Get(ctx, environmentBlueprintHeadKey(environmentID))
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if head == nil {
		return Versioned[EnvironmentComposeProjection]{}, false, errs.New(
			errs.KindInternal,
			"Environment desired head read is empty",
		)
	}
	if head.Entry == nil {
		return Versioned[EnvironmentComposeProjection]{ReadRevision: head.ReadRevision}, false, nil
	}
	revisionID, err := decodeTaskReference(head.Entry.Value)
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	root, err := repository.store.Get(ctx, environmentBlueprintRootKey(environmentID, revisionID))
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if root == nil || root.Entry == nil {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	seal, err := decodeEnvironmentBlueprintSeal(root.Entry.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	stream, readRevision, err := repository.readEnvironmentBlueprintStream(ctx, seal, "projection")
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	defer clear(stream)
	projection, err := decodeEnvironmentComposeProjection(stream)
	if err != nil || projection.EnvironmentID != environmentID || projection.RevisionID != revisionID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	return Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: head.Entry.ModRevision, ReadRevision: readRevision,
	}, true, nil
}

// GetEnvironmentComposeProjectionRevision resolves one immutable published
// revision directly. Task execution uses this method and never substitutes the
// Environment's newer current head as render input.
func (repository *HierarchyRepository) GetEnvironmentComposeProjectionRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (Versioned[EnvironmentComposeProjection], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if err := validateID(ids.KindTask, revisionID); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	root, err := repository.store.Get(ctx, environmentBlueprintRootKey(environmentID, revisionID))
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if root == nil || root.Entry == nil {
		readRevision := int64(0)
		if root != nil {
			readRevision = root.ReadRevision
		}
		return Versioned[EnvironmentComposeProjection]{ReadRevision: readRevision}, false, nil
	}
	seal, err := decodeEnvironmentBlueprintSeal(root.Entry.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	stream, readRevision, err := repository.readEnvironmentBlueprintStream(ctx, seal, "projection")
	if err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	defer clear(stream)
	projection, err := decodeEnvironmentComposeProjection(stream)
	if err != nil || projection.EnvironmentID != environmentID || projection.RevisionID != revisionID {
		return Versioned[EnvironmentComposeProjection]{}, false, corruptEnvironmentComposeProjection()
	}
	return Versioned[EnvironmentComposeProjection]{
		Record: projection, Revision: root.Entry.ModRevision, ReadRevision: readRevision,
	}, true, nil
}

// FindEnvironmentVolume resolves a stable Volume id from the sole published
// desired-state authority. The MVP deliberately prefers a bounded sequential
// head scan over a second synchronously writable identity authority.
func (repository *HierarchyRepository) FindEnvironmentVolume(
	ctx context.Context,
	volumeID string,
) (Versioned[EnvironmentComposeProjection], EnvironmentVolumeIdentity, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, err
	}
	if err := validateID(ids.KindVolume, volumeID); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, err
	}
	const headsPrefix = "/v1/records/environment-blueprints/"
	start := ""
	for {
		page, err := repository.store.Range(ctx, RangeRequest{
			Prefix: headsPrefix, StartExclusive: start, Limit: 128,
		})
		if err != nil {
			return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, err
		}
		if page == nil {
			return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, errs.New(
				errs.KindInternal, "Environment desired-head scan is empty",
			)
		}
		for _, entry := range page.Values {
			start = entry.Key
			if !strings.HasSuffix(entry.Key, "/current") {
				continue
			}
			environmentID := strings.TrimSuffix(strings.TrimPrefix(entry.Key, headsPrefix), "/current")
			if validateStableID(ids.KindEnvironment, environmentID) != nil {
				return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, corruptEnvironmentComposeProjection()
			}
			revisionID, err := decodeTaskReference(entry.Value)
			if err != nil {
				return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, corruptEnvironmentComposeProjection()
			}
			projection, found, err := repository.GetEnvironmentComposeProjectionRevision(ctx, environmentID, revisionID)
			if err != nil {
				return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, err
			}
			if !found {
				return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, corruptEnvironmentComposeProjection()
			}
			for _, volume := range projection.Record.Volumes {
				if volume.ID == volumeID {
					projection.Revision = entry.ModRevision
					return projection, volume, nil
				}
			}
		}
		if !page.More {
			break
		}
		if len(page.Values) == 0 {
			return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, errs.New(
				errs.KindInternal, "Environment desired-head pagination did not advance",
			)
		}
	}
	return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, errs.New(
		errs.KindVolumeNotFound, "volume was not found",
	)
}

func encodeEnvironmentComposeProjection(projection EnvironmentComposeProjection) ([]byte, error) {
	if err := validateEnvironmentComposeProjection(projection); err != nil {
		return nil, err
	}
	value, err := encodeEnvelope("environment-compose-projection", projection)
	if err != nil {
		return nil, err
	}
	if len(value) > EnvironmentBlueprintProjectionMaxBytes {
		clear(value)
		return nil, errs.New(
			errs.KindValidationFailed,
			"Blueprint normalized projection exceeds the 2 MiB ceiling",
		)
	}
	return value, nil
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
		validateStableID(ids.KindTask, projection.RevisionID) != nil || projection.RenderGeneration == 0 {
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
	if err := validateEnvironmentVolumeIdentities(projection.Volumes); err != nil {
		return err
	}
	if err := validateEnvironmentServiceVolumeMounts(projection); err != nil {
		return err
	}
	if err := validateEnvironmentProjectionArtifact(projection); err != nil {
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
func ApplyEnvironmentRoute(
	current EnvironmentComposeProjection,
	route RouteRecord,
) (EnvironmentComposeProjection, error) {
	if err := validateEnvironmentComposeProjection(current); err != nil {
		return EnvironmentComposeProjection{}, err
	}
	if err := validateRouteRecord(route); err != nil || route.EnvironmentID != current.EnvironmentID {
		return EnvironmentComposeProjection{}, errs.New(errs.KindValidationFailed, "applied Environment Route is invalid")
	}
	next := cloneEnvironmentComposeProjection(current)
	identity := EnvironmentRouteIdentity{ID: route.Desired.ID, Host: route.Desired.Host, Path: route.Desired.Path}
	replaced := false
	for index := range next.Routes {
		if next.Routes[index].ID == identity.ID {
			next.Routes[index] = identity
			replaced = true
			break
		}
	}
	if !replaced {
		next.Routes = append(next.Routes, identity)
	}
	for index := 0; index < len(next.SuppressedRoutes); index++ {
		if next.SuppressedRoutes[index].ID == identity.ID {
			next.SuppressedRoutes = append(next.SuppressedRoutes[:index], next.SuppressedRoutes[index+1:]...)
			break
		}
	}
	sort.Slice(next.Routes, func(left, right int) bool {
		return environmentRouteMatch(next.Routes[left]) < environmentRouteMatch(next.Routes[right])
	})
	next.RenderGeneration++
	if err := validateEnvironmentComposeProjectionAdvance(current, true, next); err != nil {
		return EnvironmentComposeProjection{}, err
	}
	return next, nil
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
	serviceOwners := make(map[string]string)
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
		for _, serviceID := range value.Runtime.GeneratedServices {
			if owner, exists := serviceOwners[serviceID]; exists && owner != value.Desired.ID {
				return errs.New(errs.KindValidationFailed, "Environment generated Service ownership is duplicated")
			}
			serviceOwners[serviceID] = value.Desired.ID
		}
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
		}, DesiredGeneration: 1, Observed: RouteObservation{
			Status: RouteObservedUnserved, DesiredGeneration: 1,
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
	clone.ComposeArtifact = append([]byte(nil), source.ComposeArtifact...)
	clone.Services = append([]EnvironmentComposeIdentity(nil), source.Services...)
	clone.Networks = append([]EnvironmentComposeIdentity(nil), source.Networks...)
	clone.Volumes = append([]EnvironmentVolumeIdentity(nil), source.Volumes...)
	clone.VolumeMounts = append([]EnvironmentServiceVolumeMount(nil), source.VolumeMounts...)
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

func validateEnvironmentDependencyPlans(
	services []EnvironmentComposeIdentity,
	deploy core.ServiceDependencyPhasePlan,
	rollback core.ServiceDependencyPhasePlan,
) error {
	names := make([]string, len(services))
	for index, service := range services {
		names[index] = service.Name
	}
	for _, selected := range []struct {
		phase core.ServiceLifecyclePhase
		plan  core.ServiceDependencyPhasePlan
	}{
		{phase: core.ServiceLifecycleDeploy, plan: deploy},
		{phase: core.ServiceLifecycleRollback, plan: rollback},
	} {
		if selected.plan.Phase == "" && len(selected.plan.OrderedServices) == 0 && len(selected.plan.Edges) == 0 {
			continue
		}
		if selected.plan.Phase != selected.phase {
			return errs.New(errs.KindValidationFailed, "Environment dependency projection phase is invalid")
		}
		if err := core.ValidateServiceDependencyPhasePlan(names, selected.plan); err != nil {
			return err
		}
	}
	return nil
}

func validateEnvironmentProjectionArtifact(projection EnvironmentComposeProjection) error {
	if len(projection.ComposeArtifact) == 0 || len(projection.ComposeArtifact) > EnvironmentBlueprintProjectionMaxBytes {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact is missing or oversized")
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		projection.ComposeArtifact,
		artifact,
	); err != nil {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact is invalid")
	}
	canonical, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(canonical, projection.ComposeArtifact) {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact is not canonical")
	}
	if validateStableID(ids.KindConfig, artifact.GetArtifactId()) != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != projection.EnvironmentID || artifact.GetAuthorizedVolumeDir() == "" ||
		len(artifact.GetCanonicalYaml()) == 0 || len(artifact.GetYamlSha256()) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact identity is invalid")
	}
	digest := sha256.Sum256(artifact.GetCanonicalYaml())
	if subtle.ConstantTimeCompare(digest[:], artifact.GetYamlSha256()) != 1 {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact digest is invalid")
	}
	if len(artifact.GetServices()) != len(projection.Services) ||
		len(artifact.GetVolumes()) != len(projection.Volumes) {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact coverage is incomplete")
	}
	services := make(map[string]string, len(artifact.GetServices()))
	for _, service := range artifact.GetServices() {
		if service == nil {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Service is invalid")
		}
		if _, duplicate := services[service.GetServiceId()]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Service is duplicated")
		}
		services[service.GetServiceId()] = service.GetComposeName()
	}
	for _, identity := range projection.Services {
		if services[identity.ID] != identity.Name {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Service identity changed")
		}
	}
	volumes := make(map[string]string, len(artifact.GetVolumes()))
	for _, volume := range artifact.GetVolumes() {
		if volume == nil {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Volume is invalid")
		}
		if _, duplicate := volumes[volume.GetVolumeId()]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Volume is duplicated")
		}
		volumes[volume.GetVolumeId()] = volume.GetComposeName()
	}
	for _, identity := range projection.Volumes {
		if volumes[identity.ID] != identity.Key {
			return errs.New(errs.KindValidationFailed, "Environment normalized Compose Volume identity changed")
		}
	}
	return nil
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
	return validateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, "",
	)
}

func validateEnvironmentComposeProjectionPublicationAdvance(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
	task TaskRecord,
) error {
	removedVolumeID := ""
	if task.Type == TaskRemove && task.Params[TaskResourceKindParam] == TaskResourceVolume {
		removedVolumeID = task.Target
		if validateStableID(ids.KindVolume, removedVolumeID) != nil {
			return errs.New(errs.KindValidationFailed, "Volume removal Task target is invalid")
		}
	}
	return validateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, removedVolumeID,
	)
}
