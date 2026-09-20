package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

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
type EnvironmentZoneProjection struct {
	EnvironmentID string    `json:"environment_id"`
	Desired       core.Zone `json:"desired"`
}
type EnvironmentServiceProjection struct {
	EnvironmentID    string       `json:"environment_id"`
	BackingNetworkID string       `json:"backing_network_id,omitempty"`
	Desired          core.Service `json:"desired"`
}
type EnvironmentRouteProjection struct {
	EnvironmentID     string     `json:"environment_id"`
	Desired           core.Route `json:"desired"`
	DesiredGeneration uint64     `json:"desired_generation"`
}

// ManagedComponentRuntimeSource pins the one Compose service that may still
// exist for an Environment Component after the source revision was attempted.
// A projection contains at most one source per fixed Component singleton.
type ManagedComponentRuntimeSource struct {
	ComponentKind  core.ComponentKind `json:"component_kind"`
	ComponentID    string             `json:"component_id"`
	ServiceID      string             `json:"service_id"`
	ComposeName    string             `json:"compose_service_name"`
	RevisionID     string             `json:"source_revision_id"`
	ArtifactID     string             `json:"source_artifact_id"`
	ArtifactSHA256 string             `json:"source_artifact_sha256"`
}

// EnvironmentComposeProjection is the sorted durable input for one Environment render.
type EnvironmentComposeProjection struct {
	EnvironmentID                  string                               `json:"environment_id"`
	RevisionID                     string                               `json:"blueprint_revision_id"`
	RenderGeneration               uint64                               `json:"render_generation"`
	ComposeArtifact                []byte                               `json:"compose_artifact"`
	NormalizedCompose              []byte                               `json:"normalized_compose"`
	RuntimeFiles                   []core.BlueprintFile                 `json:"runtime_files,omitempty"`
	ServiceExtensions              map[string]core.ServiceExtensionSpec `json:"service_extensions,omitempty"`
	DesiredZones                   []EnvironmentZoneProjection          `json:"desired_zones,omitempty"`
	DesiredServices                []EnvironmentServiceProjection       `json:"desired_services,omitempty"`
	DesiredRoutes                  []EnvironmentRouteProjection         `json:"desired_routes,omitempty"`
	Volumes                        []EnvironmentVolumeIdentity          `json:"volumes,omitempty"`
	VolumeMounts                   []EnvironmentServiceVolumeMount      `json:"volume_mounts,omitempty"`
	Components                     []ComponentRecord                    `json:"components,omitempty"`
	ManagedComponentRuntimeSources []ManagedComponentRuntimeSource      `json:"managed_component_runtime_sources,omitempty"`
	Entries                        []entryrecord.Record                 `json:"entries,omitempty"`
	Backup                         *EnvironmentBlueprintBackupPolicy    `json:"backup,omitempty"`
	core.ServiceDependencyPlans
	core.BlueprintRequirements
}

const environmentComposeProjectionPrefix = "/v1/records/environment-compose-projections/"

func environmentComposeProjectionKey(environmentID string) string {
	return environmentComposeProjectionPrefix + environmentID
}

func EnvironmentComposeProjectionStorageKey(environmentID string) string {
	return environmentComposeProjectionKey(environmentID)
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
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
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
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
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

// EnvironmentZoneRemovalAuthorities binds the desired revision being edited
// to the independently mutable projection last acknowledged by the runtime.
type EnvironmentZoneRemovalAuthorities struct {
	Desired Versioned[EnvironmentComposeProjection]
	Applied Versioned[EnvironmentComposeProjection]
}

// GetEnvironmentZoneRemovalAuthorities reads both authorities required to
// fence a Zone removal. Each returned revision belongs to its own etcd key.
func (repository *HierarchyRepository) GetEnvironmentZoneRemovalAuthorities(
	ctx context.Context,
	environmentID string,
) (EnvironmentZoneRemovalAuthorities, bool, error) {
	desired, found, err := repository.GetEnvironmentComposeProjection(ctx, environmentID)
	if err != nil || !found {
		return EnvironmentZoneRemovalAuthorities{}, found, err
	}
	applied, found, err := repository.GetEnvironmentAppliedComposeProjection(ctx, environmentID)
	if err != nil {
		return EnvironmentZoneRemovalAuthorities{}, false, err
	}
	if !found {
		return EnvironmentZoneRemovalAuthorities{}, false, errs.New(
			errs.KindStateConflict,
			"Environment applied projection is missing",
		)
	}
	return EnvironmentZoneRemovalAuthorities{Desired: desired, Applied: applied}, true, nil
}

func (repository *HierarchyRepository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (Versioned[EnvironmentComposeProjection], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
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
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revisionID); err != nil {
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
	if err := recordcodec.ValidateID(ids.KindVolume, volumeID); err != nil {
		return Versioned[EnvironmentComposeProjection]{}, EnvironmentVolumeIdentity{}, err
	}
	const headsPrefix = "/v1/records/environment-blueprints/"
	start := ""
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
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
			if recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
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
	value, err := recordcodec.Encode("environment-compose-projection", projection)
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

func EncodeEnvironmentComposeProjectionStorage(projection EnvironmentComposeProjection) ([]byte, error) {
	return encodeEnvironmentComposeProjection(projection)
}

func decodeEnvironmentComposeProjection(value []byte) (EnvironmentComposeProjection, error) {
	projection, err := recordcodec.Decode[EnvironmentComposeProjection](value, "environment-compose-projection")
	if err != nil {
		return EnvironmentComposeProjection{}, err
	}
	if err := validateEnvironmentComposeProjection(projection); err != nil {
		return EnvironmentComposeProjection{}, corruptEnvironmentComposeProjection()
	}
	return projection, nil
}

func DecodeEnvironmentComposeProjectionStorage(value []byte) (EnvironmentComposeProjection, error) {
	return decodeEnvironmentComposeProjection(value)
}

func validateEnvironmentComposeProjection(projection EnvironmentComposeProjection) error {
	return validateEnvironmentProjection(projection, environmentArtifactDesired)
}

// ApplyEnvironmentRoute replaces the lossless desired Route and advances generation.
func ApplyEnvironmentRoute(
	current EnvironmentComposeProjection,
	route routerecord.Record,
) (EnvironmentComposeProjection, error) {
	if err := validateEnvironmentComposeProjection(current); err != nil {
		return EnvironmentComposeProjection{}, err
	}
	if err := routerecord.ValidateRecord(route); err != nil || route.EnvironmentID != current.EnvironmentID {
		return EnvironmentComposeProjection{}, errs.New(
			errs.KindValidationFailed,
			"applied Environment Route is invalid",
		)
	}
	next := cloneEnvironmentComposeProjection(current)
	desired := EnvironmentRouteProjection{
		EnvironmentID: route.EnvironmentID, Desired: route.Desired,
		DesiredGeneration: route.DesiredGeneration,
	}
	desiredReplaced := false
	for index := range next.DesiredRoutes {
		if next.DesiredRoutes[index].Desired.ID == desired.Desired.ID {
			next.DesiredRoutes[index] = desired
			desiredReplaced = true
			break
		}
	}
	if !desiredReplaced {
		next.DesiredRoutes = append(next.DesiredRoutes, desired)
	}
	sort.Slice(next.DesiredRoutes, func(left, right int) bool {
		return next.DesiredRoutes[left].Desired.Host+"\x00"+next.DesiredRoutes[left].Desired.Path <
			next.DesiredRoutes[right].Desired.Host+"\x00"+next.DesiredRoutes[right].Desired.Path
	})
	next.RenderGeneration++
	if err := validateEnvironmentComposeProjectionAdvance(current, true, next); err != nil {
		return EnvironmentComposeProjection{}, err
	}
	return next, nil
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
	if recordcodec.ValidateID(ids.KindEnvEntry, entryID) != nil {
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

func validateEnvironmentEntryProjection(environmentID string, values []entryrecord.Record) error {
	previousID := ""
	for _, value := range values {
		if value.Entry.ID <= previousID || value.EnvironmentID != environmentID || entryrecord.ValidateRecord(value) != nil {
			return errs.New(errs.KindValidationFailed, "Environment Entry projection is invalid or unsorted")
		}
		previousID = value.Entry.ID
	}
	return nil
}

func validateEnvironmentZoneProjections(
	environmentID string,
	values []EnvironmentZoneProjection,
) error {
	previousName := ""
	seenIDs := make(map[string]struct{}, len(values))
	for _, value := range values {
		if value.EnvironmentID != environmentID || value.Desired.Name <= previousName ||
			zonerecord.ValidateRecord(zonerecord.Record(value)) != nil {
			return errs.New(errs.KindValidationFailed, "Environment desired Zone projection is invalid or unsorted")
		}
		if _, duplicate := seenIDs[value.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired Zone projection id is duplicated")
		}
		seenIDs[value.Desired.ID] = struct{}{}
		previousName = value.Desired.Name
	}
	return nil
}

func validateEnvironmentServiceProjections(
	environmentID string,
	values []EnvironmentServiceProjection,
) error {
	previousName := ""
	seenIDs := make(map[string]struct{}, len(values))
	for _, value := range values {
		record := ServiceRecord{
			EnvironmentID: value.EnvironmentID, BackingNetworkID: value.BackingNetworkID, Desired: value.Desired,
			Runtime: core.ServiceRuntime{
				ServiceID: value.Desired.ID, RuntimeIntent: core.ServiceRuntimeIntentRunning,
			},
		}
		if value.EnvironmentID != environmentID || value.Desired.Name <= previousName ||
			validateServiceRecord(record) != nil {
			return errs.New(errs.KindValidationFailed, "Environment desired Service projection is invalid or unsorted")
		}
		if _, duplicate := seenIDs[value.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired Service projection id is duplicated")
		}
		seenIDs[value.Desired.ID] = struct{}{}
		previousName = value.Desired.Name
	}
	return nil
}

func validateEnvironmentRouteProjections(
	environmentID string,
	values []EnvironmentRouteProjection,
) error {
	previousMatch := ""
	seenIDs := make(map[string]struct{}, len(values))
	for _, value := range values {
		match := value.Desired.Host + "\x00" + value.Desired.Path
		record := routerecord.Record{
			EnvironmentID: value.EnvironmentID, Desired: value.Desired, DesiredGeneration: value.DesiredGeneration,
			Observed: routerecord.Observation{Status: routerecord.ObservedUnserved, DesiredGeneration: value.DesiredGeneration},
		}
		if value.EnvironmentID != environmentID || match <= previousMatch || routerecord.ValidateRecord(record) != nil {
			return errs.New(errs.KindValidationFailed, "Environment desired Route projection is invalid or unsorted")
		}
		if _, duplicate := seenIDs[value.Desired.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Environment desired Route projection id is duplicated")
		}
		seenIDs[value.Desired.ID] = struct{}{}
		previousMatch = match
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

func cloneEnvironmentComposeProjection(source EnvironmentComposeProjection) EnvironmentComposeProjection {
	clone := source
	clone.ServiceDependencyPlans = source.ServiceDependencyPlans.Clone()
	clone.BlueprintRequirements = source.BlueprintRequirements.Clone()
	clone.ComposeArtifact = append([]byte(nil), source.ComposeArtifact...)
	clone.NormalizedCompose = append([]byte(nil), source.NormalizedCompose...)
	if source.RuntimeFiles != nil {
		clone.RuntimeFiles = make([]core.BlueprintFile, len(source.RuntimeFiles))
		for index, file := range source.RuntimeFiles {
			clone.RuntimeFiles[index] = core.BlueprintFile{
				Path:    file.Path,
				Content: append([]byte(nil), file.Content...),
			}
		}
	}
	clone.ServiceExtensions = cloneEnvironmentServiceExtensions(source.ServiceExtensions)
	clone.DesiredZones = append([]EnvironmentZoneProjection(nil), source.DesiredZones...)
	clone.DesiredServices = append([]EnvironmentServiceProjection(nil), source.DesiredServices...)
	clone.DesiredRoutes = append([]EnvironmentRouteProjection(nil), source.DesiredRoutes...)
	clone.Volumes = append([]EnvironmentVolumeIdentity(nil), source.Volumes...)
	clone.VolumeMounts = append([]EnvironmentServiceVolumeMount(nil), source.VolumeMounts...)
	clone.Backup = CloneEnvironmentBlueprintBackupPolicy(source.Backup)
	if source.Components != nil {
		clone.Components = make([]ComponentRecord, len(source.Components))
		for index, component := range source.Components {
			clone.Components[index] = cloneComponentTaskRecord(component)
		}
	}
	clone.ManagedComponentRuntimeSources = append(
		[]ManagedComponentRuntimeSource(nil), source.ManagedComponentRuntimeSources...,
	)
	if source.Entries != nil {
		clone.Entries = make([]entryrecord.Record, len(source.Entries))
		for index, entry := range source.Entries {
			clone.Entries[index] = entryrecord.CloneRecord(entry)
		}
	}
	return clone
}

func validateEnvironmentNormalizedCompose(value []byte) error {
	if len(value) == 0 || len(value) > EnvironmentBlueprintProjectionMaxBytes {
		return errs.New(errs.KindValidationFailed, "Environment normalized authored Compose is missing or oversized")
	}
	var document yaml.Node
	if yaml.Unmarshal(value, &document) != nil || len(document.Content) != 1 ||
		document.Content[0].Kind != yaml.MappingNode {
		return errs.New(errs.KindValidationFailed, "Environment normalized authored Compose is invalid")
	}
	return nil
}

func validateEnvironmentServiceExtensions(
	serviceNames []string,
	extensions map[string]core.ServiceExtensionSpec,
) error {
	services := make(map[string]struct{}, len(serviceNames))
	for _, name := range serviceNames {
		services[name] = struct{}{}
	}
	for name, extension := range extensions {
		if _, exists := services[name]; !exists {
			return errs.New(errs.KindValidationFailed, "Environment Service extension target is absent")
		}
		if extension.Release != nil {
			switch extension.Release.DefaultStrategy {
			case "", core.StrategyBlueGreen, core.StrategyRecreate:
			default:
				return errs.New(errs.KindValidationFailed, "Environment Service release strategy is invalid")
			}
			switch extension.Release.OnFailure {
			case "", core.OnFailureSwitchBack, core.OnFailureLeaveActive:
			default:
				return errs.New(errs.KindValidationFailed, "Environment Service release failure policy is invalid")
			}
		}
	}
	for _, phase := range []core.ServiceLifecyclePhase{
		core.ServiceLifecycleStart,
		core.ServiceLifecycleDeploy,
		core.ServiceLifecycleRollback,
	} {
		if _, err := core.BuildServiceDependencyPhasePlan(serviceNames, extensions, phase); err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	return nil
}

func cloneEnvironmentServiceExtensions(
	source map[string]core.ServiceExtensionSpec,
) map[string]core.ServiceExtensionSpec {
	if source == nil {
		return nil
	}
	result := make(map[string]core.ServiceExtensionSpec, len(source))
	for name, extension := range source {
		clone := extension
		if extension.Release != nil {
			release := *extension.Release
			clone.Release = &release
		}
		if extension.DependsOn != nil {
			clone.DependsOn = make(map[string]core.ServiceDependency, len(extension.DependsOn))
			for dependency, decision := range extension.DependsOn {
				decision.Phases = append([]core.ServiceDependencyPhase(nil), decision.Phases...)
				clone.DependsOn[dependency] = decision
			}
		}
		result[name] = clone
	}
	return result
}

func validateEnvironmentProjectionArtifact(
	projection EnvironmentComposeProjection,
	kind environmentArtifactKind,
) error {
	if len(projection.ComposeArtifact) == 0 ||
		len(projection.ComposeArtifact) > EnvironmentBlueprintProjectionMaxBytes {
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
	if recordcodec.ValidateID(ids.KindConfig, artifact.GetArtifactId()) != nil ||
		artifact.GetOwnerKind() != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		artifact.GetOwnerId() != projection.EnvironmentID || artifact.GetAuthorizedVolumeDir() == "" ||
		len(artifact.GetCanonicalYaml()) == 0 || len(artifact.GetYamlSha256()) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact identity is invalid")
	}
	digest := sha256.Sum256(artifact.GetCanonicalYaml())
	if subtle.ConstantTimeCompare(digest[:], artifact.GetYamlSha256()) != 1 {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact digest is invalid")
	}
	expectedServices := make(map[string]environmentArtifactServiceIdentity, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		expectedServices[service.Desired.ID] = environmentArtifactServiceIdentity{
			name: service.Desired.Name, renderGeneration: projection.RenderGeneration,
			capturedRuntime: kind == environmentArtifactCapturedRuntime,
		}
	}
	for _, component := range projection.Components {
		if !component.Desired.Enabled {
			continue
		}
		for _, serviceID := range component.Runtime.GeneratedServices {
			expected := expectedServices[serviceID]
			if expected.componentID != "" {
				return errs.New(
					errs.KindValidationFailed,
					"Environment normalized Compose Service identity is duplicated",
				)
			}
			expected.componentID = component.Desired.ID
			expectedServices[serviceID] = expected
		}
	}
	if len(artifact.GetVolumes()) != len(projection.Volumes) {
		return errs.New(errs.KindValidationFailed, "Environment normalized Compose artifact coverage is incomplete")
	}
	if err := validateEnvironmentArtifactServices(artifact, expectedServices); err != nil {
		return err
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

func validateEnvironmentComposeProjectionAdvance(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
) error {
	if err := validateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, "",
	); err != nil {
		return err
	}
	return preserveEnvironmentNonEntryDesiredResources(previous, hasPrevious, next)
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
		if recordcodec.ValidateID(ids.KindVolume, removedVolumeID) != nil {
			return errs.New(errs.KindValidationFailed, "Volume removal Task target is invalid")
		}
	}
	if err := validateEnvironmentComposeProjectionAdvanceAllowingVolumeRemoval(
		previous, hasPrevious, next, removedVolumeID,
	); err != nil {
		return err
	}
	return preserveEnvironmentNonEntryDesiredResources(previous, hasPrevious, next)
}

// preserveEnvironmentNonEntryDesiredResources prevents a publication from
// treating omission as deletion. Service, Zone, and Route removals publish
// their candidate head only from their resource-specific successful terminal
// transaction, so a pending Remove-shaped publication is not an exception.
func preserveEnvironmentNonEntryDesiredResources(
	previous EnvironmentComposeProjection,
	hasPrevious bool,
	next EnvironmentComposeProjection,
) error {
	if !hasPrevious {
		return nil
	}
	nextIDs := make(map[string]struct{},
		len(next.DesiredServices)+len(next.DesiredZones)+len(next.DesiredRoutes),
	)
	for _, service := range next.DesiredServices {
		nextIDs[service.Desired.ID] = struct{}{}
	}
	for _, zone := range next.DesiredZones {
		nextIDs[zone.Desired.ID] = struct{}{}
	}
	for _, route := range next.DesiredRoutes {
		nextIDs[route.Desired.ID] = struct{}{}
	}
	for _, service := range previous.DesiredServices {
		if _, retained := nextIDs[service.Desired.ID]; !retained {
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing Service %s; remove it explicitly before apply",
				service.Desired.Name,
			)
		}
	}
	for _, zone := range previous.DesiredZones {
		if _, retained := nextIDs[zone.Desired.ID]; !retained {
			return errs.Newf(
				errs.KindResourceInUse,
				"Blueprint omits existing Zone %s; remove it explicitly before apply",
				zone.Desired.Name,
			)
		}
	}
	for _, route := range previous.DesiredRoutes {
		if _, retained := nextIDs[route.Desired.ID]; !retained {
			return errs.New(
				errs.KindResourceInUse,
				"Blueprint omits an existing Route; remove it explicitly before apply",
			)
		}
	}
	return nil
}
