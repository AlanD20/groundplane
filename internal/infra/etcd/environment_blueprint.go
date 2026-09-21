package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"net/netip"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	environmentBlueprintMaxFiles      = 64
	environmentBlueprintMaxFileBytes  = 256 * 1024
	environmentBlueprintMaxTotalBytes = 768 * 1024
	environmentBlueprintMaxPathBytes  = 240

	EnvironmentDesiredRevisionParam = "desired_revision_id"
)

var environmentBlueprintInterpolationKey = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// EnvironmentBlueprintRevision is one immutable, verified desired-state
// input. RevisionID is the Task id that first reconciles it, so queued work can
// never be retargeted when a later apply advances the current pointer.
type EnvironmentBlueprintRevision struct {
	EnvironmentID  string
	RevisionID     string
	RootPath       string
	ComposeSources []string
	Interpolation  map[string]string
	Files          []EnvironmentBlueprintFile
	CreatedAt      time.Time
}
type EnvironmentBlueprintFile struct {
	Path    string
	Content []byte
}
type EnvironmentBlueprintHead struct {
	EnvironmentID string
	RevisionID    string
}

// EnvironmentBlueprintZoneChange is one immutable existing Zone fence or one
// new Zone and subnet reservation committed with the Blueprint head.
type EnvironmentBlueprintZoneChange struct {
	Current *etcdstore.Versioned[zonerecord.Record]
	Record  zonerecord.Record
}

// EnvironmentBlueprintServiceChange is one desired-only Service replacement
// committed with the Blueprint head. Current is nil only when the Blueprint
// first introduces the stable Service id.
type EnvironmentBlueprintServiceChange struct {
	Current *etcdstore.Versioned[servicerecord.ServiceRecord]
	Record  servicerecord.ServiceRecord
}

// EnvironmentBlueprintRouteChange is one exposure-only Route replacement or
// one new stable Route committed with its target Service desired state.
type EnvironmentBlueprintRouteChange struct {
	Current *etcdstore.Versioned[routerecord.Record]
	Record  routerecord.Record
}
type environmentBlueprintManifest struct {
	EnvironmentID  string                             `json:"environment_id"`
	RevisionID     string                             `json:"revision_id"`
	RootPath       string                             `json:"root"`
	ComposeSources []string                           `json:"compose_sources"`
	Interpolation  map[string]string                  `json:"interpolation"`
	Files          []environmentBlueprintManifestFile `json:"files"`
	CreatedAt      string                             `json:"created_at"`
}
type environmentBlueprintManifestFile struct {
	Path   string `json:"path"`
	Size   int    `json:"size"`
	SHA256 string `json:"sha256"`
}

func environmentBlueprintHeadKey(environmentID string) string {
	return "/v1/records/environment-blueprints/" + environmentID + "/current"
}
func environmentBlueprintRevisionsPrefix(environmentID string) string {
	return "/v1/records/environment-blueprints/" + environmentID + "/revisions/"
}
func environmentBlueprintRevisionPrefix(environmentID string, revisionID string) string {
	return environmentBlueprintRevisionsPrefix(environmentID) + revisionID + "/"
}
func environmentBlueprintManifestKey(environmentID string, revisionID string) string {
	return environmentBlueprintRevisionPrefix(environmentID, revisionID) + "manifest"
}
func environmentBlueprintFileKey(environmentID string, revisionID string, index int) string {
	return environmentBlueprintRevisionPrefix(
		environmentID,
		revisionID,
	) + fmt.Sprintf(
		"files/%06d",
		index+1,
	)
}

// GetEnvironmentBlueprintHead returns the current immutable revision pointer.
// A missing head is normal before an Environment's first successful apply.
func (repository *HierarchyRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[EnvironmentBlueprintHead], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	result, err := repository.store.Get(ctx, environmentBlueprintHeadKey(environmentID))
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	if result.Entry == nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{ReadRevision: result.ReadRevision}, false, nil
	}
	revisionID, err := idempotencyrecord.DecodeTaskReference(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	return etcdstore.Versioned[EnvironmentBlueprintHead]{
		Record:   EnvironmentBlueprintHead{EnvironmentID: environmentID, RevisionID: revisionID},
		Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, true, nil
}

// GetEnvironmentBlueprintRevision reconstructs verified file bytes at the
// manifest's pinned MVCC revision. Missing immutable state is reported as not
// found; mismatched bytes or metadata are durable corruption.
func (repository *HierarchyRepository) GetEnvironmentBlueprintRevision(
	ctx context.Context,
	environmentID string,
	revisionID string,
) (etcdstore.Versioned[EnvironmentBlueprintRevision], bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, environmentID); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revisionID); err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	rootResult, err := repository.store.Get(ctx, environmentBlueprintRootKey(environmentID, revisionID))
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if rootResult.Entry == nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{
			ReadRevision: rootResult.ReadRevision,
		}, false, nil
	}
	seal, err := decodeEnvironmentBlueprintSeal(rootResult.Entry.Value)
	if err != nil || seal.EnvironmentID != environmentID || seal.RevisionID != revisionID {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if seal.SourceKind == EnvironmentBlueprintSourceMutation {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{ReadRevision: rootResult.ReadRevision}, false, nil
	}
	stream, readRevision, err := repository.readEnvironmentBlueprintStream(ctx, seal, "audit")
	if err != nil {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	defer clear(stream)
	revision, err := decodeEnvironmentBlueprintAuditStream(stream)
	if err != nil || revision.EnvironmentID != environmentID || revision.RevisionID != revisionID {
		return etcdstore.Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	return etcdstore.Versioned[EnvironmentBlueprintRevision]{
		Record: revision, Revision: rootResult.Entry.ModRevision,
		ReadRevision: readRevision,
	}, true, nil
}

type preparedEnvironmentBlueprintPoolChange struct {
	environment      etcdstore.Versioned[hierarchyrecord.EnvironmentRecord]
	registryRevision int64
	environmentValue []byte
	registryValue    []byte
}

func (change preparedEnvironmentBlueprintPoolChange) changed() bool {
	return len(change.environmentValue) != 0
}
func clearPreparedEnvironmentBlueprintPoolChange(change preparedEnvironmentBlueprintPoolChange) {
	clear(change.environmentValue)
	clear(change.registryValue)
}
func (repository *HierarchyRepository) prepareEnvironmentBlueprintPoolChangeAtRevision(
	ctx context.Context,
	root netip.Prefix,
	current etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	desiredNetworkPool string,
	revision int64,
) (preparedEnvironmentBlueprintPoolChange, error) {
	prepared := preparedEnvironmentBlueprintPoolChange{environment: current}
	if desiredNetworkPool == current.Record.NetworkPool {
		return prepared, nil
	}
	if !root.IsValid() || !root.Addr().Is4() || root != root.Masked() {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindValidationFailed,
			"Environment pool root must be a canonical IPv4 CIDR",
		)
	}
	prepared.environment.Record.NetworkPool = desiredNetworkPool
	if err := hierarchyrecord.ValidateEnvironment(prepared.environment.Record); err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	registries, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentPoolRegistryKey}, Revision: revision,
	})
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	if registries == nil || len(registries.Values) != 1 || registries.Values[0] == nil ||
		registries.Values[0].Key != environmentPoolRegistryKey {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindInternal,
			"Environment pool reservation registry is missing",
		)
	}
	defer clearKeyValues(registries.Values)
	global, err := recordcodec.Decode[EnvironmentPoolRegistry](
		registries.Values[0].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(global) != nil {
		return preparedEnvironmentBlueprintPoolChange{}, corruptEnvironmentPoolRegistry()
	}
	nextGlobal, canonical, err := global.Replace(
		root,
		current.Record.ID,
		current.Record.NetworkPool,
		desiredNetworkPool,
	)
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	if canonical != desiredNetworkPool {
		return preparedEnvironmentBlueprintPoolChange{}, errs.New(
			errs.KindValidationFailed,
			"x-gp-network-pool must be a canonical IPv4 CIDR",
		)
	}
	prepared.environmentValue, err = hierarchyrecord.EncodeEnvironment(prepared.environment.Record)
	if err != nil {
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	prepared.registryValue, err = recordcodec.Encode("environment_pool_registry", nextGlobal)
	if err != nil {
		clear(prepared.environmentValue)
		return preparedEnvironmentBlueprintPoolChange{}, err
	}
	prepared.registryRevision = registries.Values[0].ModRevision
	return prepared, nil
}

func classifyEnvironmentBlueprintBaseConflict(
	values []*etcdstore.KeyValue,
	fileCount int,
	expectedHeadRevision int64,
	operationID string,
) error {
	if values[2] != nil {
		activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[2].Value)
		if err != nil {
			return err
		}
		return errs.Newf(
			errs.KindStateConflict,
			"operation %s already has active task %s",
			operationID,
			activeTaskID,
		)
	}
	for _, index := range []int{0, 1, 3} {
		if values[index] != nil {
			return errs.New(errs.KindInternal, "Blueprint apply collided with durable Task state")
		}
	}
	for index := 4; index < 5+fileCount; index++ {
		if values[index] != nil {
			return errs.New(
				errs.KindInternal,
				"Blueprint apply collided with immutable revision state",
			)
		}
	}
	headIndex := 5 + fileCount
	if (expectedHeadRevision == 0 && values[headIndex] != nil) ||
		(expectedHeadRevision > 0 && (values[headIndex] == nil || values[headIndex].ModRevision != expectedHeadRevision)) {
		return errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	projectionIndex := headIndex + 1
	if (expectedHeadRevision == 0 && values[projectionIndex] != nil) ||
		(expectedHeadRevision > 0 &&
			(values[projectionIndex] == nil || values[projectionIndex].ModRevision != expectedHeadRevision)) {
		return errs.New(errs.KindStateConflict, "Environment Compose projection changed")
	}
	return nil
}

func (repository *HierarchyRepository) loadEnvironmentBlueprintMutationFence(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
) (environmentMutationFenceEvidence, error) {
	keys := []string{hierarchyrecord.EnvironmentKey(environment.Record.ID), hierarchyrecord.ProjectKey(project.Record.ID)}
	anchor, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return environmentMutationFenceEvidence{}, err
	}
	if anchor == nil || anchor.ReadRevision <= 0 || len(anchor.Values) != len(keys) ||
		anchor.Values[0] == nil || anchor.Values[1] == nil {
		return environmentMutationFenceEvidence{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint hierarchy is unavailable",
		)
	}
	defer clearKeyValues(anchor.Values)
	if anchor.Values[0].ModRevision != environment.Revision ||
		anchor.Values[1].ModRevision != project.Revision {
		return environmentMutationFenceEvidence{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint hierarchy changed",
		)
	}
	return loadOrdinaryEnvironmentMutationFence(
		ctx, repository.store, environment.Record.ID, anchor.ReadRevision,
	)
}

func (repository *HierarchyRepository) getEnvironmentBlueprintProjectionAtRevision(
	ctx context.Context,
	environmentID string,
	readRevision int64,
) (etcdstore.Versioned[EnvironmentComposeProjection], bool, error) {
	return repository.getEnvironmentComposeProjectionAtRevision(ctx, environmentID, readRevision)
}

type preparedEnvironmentBlueprintZonePool struct {
	currentRevision int64
	value           []byte
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintZonePoolAtRevision(
	ctx context.Context,
	environment hierarchyrecord.EnvironmentRecord,
	desired []EnvironmentZoneProjection,
	readRevision int64,
) (preparedEnvironmentBlueprintZonePool, error) {
	current, err := repository.getEnvironmentBlueprintZoneRegistryAtRevision(
		ctx, environment.ID, readRevision,
	)
	if err != nil {
		return preparedEnvironmentBlueprintZonePool{}, err
	}
	next := zonePoolRegistry{Reservations: make(map[string]string, len(desired))}
	for _, projection := range desired {
		zone := zonerecord.Record(projection)
		if projection.EnvironmentID != environment.ID {
			return preparedEnvironmentBlueprintZonePool{}, errs.New(
				errs.KindValidationFailed, "Blueprint Zone does not belong to its Environment",
			)
		}
		next, err = next.reserve(environment, zone)
		if err != nil {
			return preparedEnvironmentBlueprintZonePool{}, err
		}
	}
	value, err := recordcodec.Encode("zone_pool_registry", next)
	if err != nil {
		return preparedEnvironmentBlueprintZonePool{}, err
	}
	return preparedEnvironmentBlueprintZonePool{currentRevision: current.Revision, value: value}, nil
}

func (repository *HierarchyRepository) getEnvironmentBlueprintZoneRegistryAtRevision(
	ctx context.Context,
	environmentID string,
	readRevision int64,
) (etcdstore.Versioned[zonePoolRegistry], error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{zonePoolRegistryKey(environmentID)}, Revision: readRevision,
	})
	if err != nil {
		return etcdstore.Versioned[zonePoolRegistry]{}, err
	}
	if result == nil || result.ReadRevision != readRevision || len(result.Values) != 1 {
		return etcdstore.Versioned[zonePoolRegistry]{}, errs.New(
			errs.KindInternal,
			"Zone pool registry read is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[zonePoolRegistry]{
			Record: zonePoolRegistry{Reservations: map[string]string{}}, ReadRevision: readRevision,
		}, nil
	}
	registry, err := recordcodec.Decode[zonePoolRegistry](result.Values[0].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(registry) != nil {
		return etcdstore.Versioned[zonePoolRegistry]{}, corruptZonePoolRegistry()
	}
	return etcdstore.Versioned[zonePoolRegistry]{
		Record: registry, Revision: result.Values[0].ModRevision, ReadRevision: readRevision,
	}, nil
}

type preparedEnvironmentBlueprintRoute struct {
	change        EnvironmentBlueprintRouteChange
	value         []byte
	ownerRevision int64
	matchRevision int64
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintRouteChanges(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	services []EnvironmentBlueprintServiceChange,
	changes []EnvironmentBlueprintRouteChange,
) ([]preparedEnvironmentBlueprintRoute, error) {
	return repository.prepareEnvironmentBlueprintRouteChangesAtRevision(
		ctx,
		environment,
		services,
		changes,
		0,
	)
}

func (repository *HierarchyRepository) prepareEnvironmentBlueprintRouteChangesAtRevision(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	services []EnvironmentBlueprintServiceChange,
	changes []EnvironmentBlueprintRouteChange,
	readRevision int64,
) ([]preparedEnvironmentBlueprintRoute, error) {
	targets := make(map[string]struct{}, len(services))
	for _, service := range services {
		targets[service.Record.Desired.ID] = struct{}{}
	}
	seenIDs := make(map[string]struct{}, len(changes))
	seenMatches := make(map[string]struct{}, len(changes))
	prepared := make([]preparedEnvironmentBlueprintRoute, 0, len(changes))
	for _, change := range changes {
		if err := routerecord.ValidateRecord(change.Record); err != nil {
			clearPreparedEnvironmentBlueprintRoutes(prepared)
			return nil, err
		}
		routeID := change.Record.Desired.ID
		matchKey := routerecord.MatchKey(
			change.Record.EnvironmentID,
			change.Record.Desired.Host,
			change.Record.Desired.Path,
		)
		_, targetExists := targets[change.Record.Desired.TargetServiceID]
		_, duplicateID := seenIDs[routeID]
		_, duplicateMatch := seenMatches[matchKey]
		if change.Record.EnvironmentID != environment.Record.ID || !targetExists || duplicateID ||
			duplicateMatch {
			clearPreparedEnvironmentBlueprintRoutes(prepared)
			return nil, errs.New(
				errs.KindValidationFailed,
				"Blueprint Route change does not match its Environment Service projection",
			)
		}
		seenIDs[routeID] = struct{}{}
		seenMatches[matchKey] = struct{}{}
		item := preparedEnvironmentBlueprintRoute{change: change}
		if change.Current != nil {
			if err := validateRouteVersion(*change.Current); err != nil {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, err
			}
			replacement, err := routerecord.ReplaceDesired(change.Current.Record, change.Record.Desired)
			if err != nil || replacement != change.Record {
				clearPreparedEnvironmentBlueprintRoutes(prepared)
				return nil, errs.New(
					errs.KindValidationFailed,
					"Blueprint Route replacement changed immutable identity, match, or target",
				)
			}
		}
		prepared = append(prepared, item)
	}
	return prepared, nil
}

func clearPreparedEnvironmentBlueprintRoutes(routes []preparedEnvironmentBlueprintRoute) {
	for index := range routes {
		clear(routes[index].value)
	}
}

func encodeEnvironmentBlueprintManifest(revision EnvironmentBlueprintRevision) ([]byte, error) {
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
		return nil, err
	}
	manifest := environmentBlueprintManifest{
		EnvironmentID:  revision.EnvironmentID,
		RevisionID:     revision.RevisionID,
		RootPath:       revision.RootPath,
		ComposeSources: append([]string(nil), revision.ComposeSources...),
		Interpolation:  cloneEnvironmentBlueprintInterpolation(revision.Interpolation),
		Files:          make([]environmentBlueprintManifestFile, len(revision.Files)),
		CreatedAt:      revision.CreatedAt.Format(time.RFC3339Nano),
	}
	for index, file := range revision.Files {
		digest := sha256.Sum256(file.Content)
		manifest.Files[index] = environmentBlueprintManifestFile{
			Path: file.Path, Size: len(file.Content), SHA256: hex.EncodeToString(digest[:]),
		}
	}
	return recordcodec.Encode("environment-blueprint-revision", manifest)
}

func decodeEnvironmentBlueprintManifest(value []byte) (environmentBlueprintManifest, error) {
	manifest, err := recordcodec.Decode[environmentBlueprintManifest](
		value,
		"environment-blueprint-revision",
	)
	if err != nil {
		return environmentBlueprintManifest{}, err
	}
	createdAt, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt)
	if err != nil || manifest.CreatedAt != createdAt.UTC().Format(time.RFC3339Nano) {
		return environmentBlueprintManifest{}, corruptEnvironmentBlueprint()
	}
	manifest.CreatedAt = createdAt.UTC().Format(time.RFC3339Nano)
	if err := validateEnvironmentBlueprintManifest(manifest); err != nil {
		return environmentBlueprintManifest{}, corruptEnvironmentBlueprint()
	}
	return manifest, nil
}

func validateEnvironmentBlueprintRevision(revision EnvironmentBlueprintRevision) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, revision.EnvironmentID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindTask, revision.RevisionID); err != nil {
		return err
	}
	if err := recordcodec.ValidateTimestamp("Blueprint revision created_at", revision.CreatedAt); err != nil {
		return err
	}
	manifest := environmentBlueprintManifest{
		EnvironmentID:  revision.EnvironmentID,
		RevisionID:     revision.RevisionID,
		RootPath:       revision.RootPath,
		ComposeSources: revision.ComposeSources,
		Interpolation:  revision.Interpolation,
		CreatedAt:      revision.CreatedAt.Format(time.RFC3339Nano),
		Files:          make([]environmentBlueprintManifestFile, len(revision.Files)),
	}
	for index, file := range revision.Files {
		digest := sha256.Sum256(file.Content)
		manifest.Files[index] = environmentBlueprintManifestFile{
			Path: file.Path, Size: len(file.Content), SHA256: hex.EncodeToString(digest[:]),
		}
	}
	return validateEnvironmentBlueprintManifest(manifest)
}

func validateEnvironmentBlueprintManifest(manifest environmentBlueprintManifest) error {
	if recordcodec.ValidateID(ids.KindEnvironment, manifest.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, manifest.RevisionID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint revision identity is invalid")
	}
	if err := validateEnvironmentBlueprintPath(manifest.RootPath); err != nil {
		return err
	}
	if len(manifest.Files) == 0 || len(manifest.Files) > environmentBlueprintMaxFiles ||
		len(manifest.ComposeSources) == 0 || manifest.ComposeSources[0] != manifest.RootPath {
		return errs.New(errs.KindValidationFailed, "Blueprint revision file namespace is invalid")
	}
	declared := make(map[string]struct{}, len(manifest.Files))
	totalBytes := 0
	previous := ""
	for index, file := range manifest.Files {
		if validateEnvironmentBlueprintPath(file.Path) != nil ||
			(index > 0 && file.Path <= previous) ||
			file.Size < 0 ||
			file.Size > environmentBlueprintMaxFileBytes ||
			len(file.SHA256) != sha256.Size*2 {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision file metadata is invalid",
			)
		}
		decoded, err := hex.DecodeString(file.SHA256)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != file.SHA256 {
			return errs.New(errs.KindValidationFailed, "Blueprint revision file digest is invalid")
		}
		totalBytes += file.Size
		if totalBytes > environmentBlueprintMaxTotalBytes {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision exceeds its total size limit",
			)
		}
		declared[file.Path] = struct{}{}
		previous = file.Path
	}
	seenSources := make(map[string]struct{}, len(manifest.ComposeSources))
	for _, source := range manifest.ComposeSources {
		if validateEnvironmentBlueprintPath(source) != nil {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose source is invalid",
			)
		}
		if _, exists := declared[source]; !exists {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose source is undeclared",
			)
		}
		if _, duplicate := seenSources[source]; duplicate {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision Compose sources are duplicated",
			)
		}
		seenSources[source] = struct{}{}
	}
	for key, value := range manifest.Interpolation {
		if !environmentBlueprintInterpolationKey.MatchString(key) || !utf8.ValidString(value) ||
			strings.ContainsRune(value, 0) {
			return errs.New(
				errs.KindValidationFailed,
				"Blueprint revision interpolation is invalid",
			)
		}
	}
	return nil
}

func validateEnvironmentBlueprintPath(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) ||
		len(
			value,
		) > environmentBlueprintMaxPathBytes || strings.Contains(value, `\`) || path.IsAbs(value) {
		return errs.New(errs.KindValidationFailed, "Blueprint revision path is invalid")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned != value || strings.HasPrefix(cleaned, "../") {
		return errs.New(errs.KindValidationFailed, "Blueprint revision path is invalid")
	}
	return nil
}

func cloneEnvironmentBlueprintInterpolation(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func corruptEnvironmentBlueprint() error {
	return errs.New(errs.KindInternal, "Environment Blueprint revision is corrupt")
}
