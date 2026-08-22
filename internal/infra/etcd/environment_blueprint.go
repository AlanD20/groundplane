package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
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

	EnvironmentBlueprintRevisionParam = "blueprint_revision_id"
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
	return environmentBlueprintRevisionPrefix(environmentID, revisionID) + fmt.Sprintf("files/%06d", index+1)
}

// GetEnvironmentBlueprintHead returns the current immutable revision pointer.
// A missing head is normal before an Environment's first successful apply.
func (repository *HierarchyRepository) GetEnvironmentBlueprintHead(
	ctx context.Context,
	environmentID string,
) (Versioned[EnvironmentBlueprintHead], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	result, err := repository.store.Get(ctx, environmentBlueprintHeadKey(environmentID))
	if err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	if result.Entry == nil {
		return Versioned[EnvironmentBlueprintHead]{ReadRevision: result.ReadRevision}, false, nil
	}
	revisionID, err := decodeTaskReference(result.Entry.Value)
	if err != nil {
		return Versioned[EnvironmentBlueprintHead]{}, false, err
	}
	return Versioned[EnvironmentBlueprintHead]{
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
) (Versioned[EnvironmentBlueprintRevision], bool, error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if err := validateID(ids.KindEnvironment, environmentID); err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if err := validateID(ids.KindTask, revisionID); err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	manifestResult, err := repository.store.Get(ctx, environmentBlueprintManifestKey(environmentID, revisionID))
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if manifestResult.Entry == nil {
		return Versioned[EnvironmentBlueprintRevision]{ReadRevision: manifestResult.ReadRevision}, false, nil
	}
	manifest, err := decodeEnvironmentBlueprintManifest(manifestResult.Entry.Value)
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if manifest.EnvironmentID != environmentID || manifest.RevisionID != revisionID {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	createdAt, err := time.Parse(time.RFC3339Nano, manifest.CreatedAt)
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	keys := make([]string, len(manifest.Files))
	for index := range manifest.Files {
		keys[index] = environmentBlueprintFileKey(environmentID, revisionID, index)
	}
	fileResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: keys, Revision: manifestResult.ReadRevision,
	})
	if err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, err
	}
	if len(fileResult.Values) != len(keys) || fileResult.ReadRevision != manifestResult.ReadRevision {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	revision := EnvironmentBlueprintRevision{
		EnvironmentID: environmentID, RevisionID: revisionID, RootPath: manifest.RootPath,
		ComposeSources: append([]string(nil), manifest.ComposeSources...),
		Interpolation:  cloneEnvironmentBlueprintInterpolation(manifest.Interpolation),
		Files:          make([]EnvironmentBlueprintFile, len(manifest.Files)),
		CreatedAt:      createdAt,
	}
	for index, metadata := range manifest.Files {
		value := fileResult.Values[index]
		if value == nil || len(value.Value) != metadata.Size {
			return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
		}
		digest := sha256.Sum256(value.Value)
		if hex.EncodeToString(digest[:]) != metadata.SHA256 {
			return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
		}
		revision.Files[index] = EnvironmentBlueprintFile{
			Path: metadata.Path, Content: append([]byte(nil), value.Value...),
		}
	}
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
		return Versioned[EnvironmentBlueprintRevision]{}, false, corruptEnvironmentBlueprint()
	}
	return Versioned[EnvironmentBlueprintRevision]{
		Record: revision, Revision: manifestResult.Entry.ModRevision,
		ReadRevision: manifestResult.ReadRevision,
	}, true, nil
}

// ApplyEnvironmentBlueprintWithTask atomically publishes immutable desired
// input, advances the current pointer, and enqueues its sealed reconcile Task.
func (repository *HierarchyRepository) ApplyEnvironmentBlueprintWithTask(
	ctx context.Context,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	expectedHeadRevision int64,
	revision EnvironmentBlueprintRevision,
	projection EnvironmentComposeProjection,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironment(environment.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if project.Record.Kind != ProjectKindTenant || project.Revision <= 0 || environment.Revision <= 0 ||
		project.ReadRevision < project.Revision || environment.ReadRevision < environment.Revision ||
		environment.Record.ProjectID != project.Record.ID ||
		environment.Record.ProvisioningState != EnvironmentProvisioningReady || expectedHeadRevision < 0 {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment is not ready for Blueprint apply")
	}
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	previousProjection, hasPreviousProjection, err := repository.GetEnvironmentComposeProjection(
		ctx,
		environment.Record.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if (expectedHeadRevision == 0 && hasPreviousProjection) ||
		(expectedHeadRevision > 0 && (!hasPreviousProjection || previousProjection.Revision != expectedHeadRevision)) {
		return IdempotencyTransactionResult{}, errs.New(errs.KindStateConflict, "Environment desired state changed")
	}
	if projection.EnvironmentID != environment.Record.ID || projection.BlueprintRevisionID != revision.RevisionID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment Compose projection does not identify its Blueprint revision",
		)
	}
	if err := validateEnvironmentComposeProjectionAdvance(
		previousProjection.Record,
		hasPreviousProjection,
		projection,
	); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if revision.EnvironmentID != environment.Record.ID || revision.RevisionID != task.ID ||
		!revision.CreatedAt.Equal(task.CreatedAt) || task.Type != TaskUpdate ||
		task.Target != environment.Record.ID || task.Status != TaskStatusPending ||
		task.Params[EnvironmentBlueprintRevisionParam] != revision.RevisionID ||
		task.Params[TaskMaterializationEnvironmentParam] != environment.Record.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Blueprint apply Task does not own its immutable revision",
		)
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Blueprint apply marker does not match its Environment-scoped Task",
		)
	}
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	manifestValue, err := encodeEnvironmentBlueprintManifest(revision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(manifestValue)
	projectionValue, err := encodeEnvironmentComposeProjection(projection)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(projectionValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	conditions := []Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: environmentBlueprintManifestKey(revision.EnvironmentID, revision.RevisionID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: environmentBlueprintManifestKey(revision.EnvironmentID, revision.RevisionID), Value: manifestValue},
	}
	for index, file := range revision.Files {
		key := environmentBlueprintFileKey(revision.EnvironmentID, revision.RevisionID, index)
		conditions = append(conditions, Condition{Key: key})
		mutations = append(mutations, Mutation{Type: MutationPut, Key: key, Value: append([]byte(nil), file.Content...)})
	}
	conditions = append(conditions,
		Condition{Key: environmentBlueprintHeadKey(revision.EnvironmentID), ModRevision: expectedHeadRevision},
		Condition{Key: environmentComposeProjectionKey(revision.EnvironmentID), ModRevision: expectedHeadRevision},
		Condition{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		Condition{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		Condition{Key: deletionTombstoneKey("environment", environment.Record.ID)},
		Condition{Key: deletionTombstoneKey("project", project.Record.ID)},
		Condition{Key: deletionTombstoneKey("tenant", project.Record.TenantID)},
	)
	mutations = append(mutations,
		Mutation{Type: MutationPut, Key: environmentBlueprintHeadKey(revision.EnvironmentID), Value: reference},
		Mutation{Type: MutationPut, Key: environmentComposeProjectionKey(revision.EnvironmentID), Value: projectionValue},
	)
	plan, err := newTaskIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyEnvironmentBlueprintApplyConflict(
			len(revision.Files), expectedHeadRevision, project, environment, task.OperationID,
		),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyEnvironmentBlueprintApplyConflict(
	fileCount int,
	expectedHeadRevision int64,
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		if len(values) != 12+fileCount {
			return errs.New(errs.KindInternal, "Blueprint apply compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, err := decodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(errs.KindStateConflict, "operation %s already has active task %s", operationID, activeTaskID)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Blueprint apply collided with durable Task state")
			}
		}
		for index := 4; index < 5+fileCount; index++ {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Blueprint apply collided with immutable revision state")
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
		environmentIndex := headIndex + 2
		projectIndex := headIndex + 3
		if values[environmentIndex] == nil {
			return errs.New(errs.KindEnvironmentNotFound, "environment was not found")
		}
		if values[environmentIndex].ModRevision != environment.Revision {
			return stateConflict("environment", environment.Record.ID)
		}
		if values[projectIndex] == nil {
			return errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if values[projectIndex].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		if values[headIndex+4] != nil {
			return errs.New(errs.KindResourceInUse, "Environment deletion is in progress")
		}
		if values[headIndex+5] != nil {
			return errs.New(errs.KindResourceInUse, "Project deletion is in progress")
		}
		if values[headIndex+6] != nil {
			return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
		}
		return errs.New(errs.KindStateConflict, "Environment Blueprint apply changed")
	}
}

func encodeEnvironmentBlueprintManifest(revision EnvironmentBlueprintRevision) ([]byte, error) {
	if err := validateEnvironmentBlueprintRevision(revision); err != nil {
		return nil, err
	}
	manifest := environmentBlueprintManifest{
		EnvironmentID: revision.EnvironmentID, RevisionID: revision.RevisionID,
		RootPath: revision.RootPath, ComposeSources: append([]string(nil), revision.ComposeSources...),
		Interpolation: cloneEnvironmentBlueprintInterpolation(revision.Interpolation),
		Files:         make([]environmentBlueprintManifestFile, len(revision.Files)),
		CreatedAt:     revision.CreatedAt.Format(time.RFC3339Nano),
	}
	for index, file := range revision.Files {
		digest := sha256.Sum256(file.Content)
		manifest.Files[index] = environmentBlueprintManifestFile{
			Path: file.Path, Size: len(file.Content), SHA256: hex.EncodeToString(digest[:]),
		}
	}
	return encodeEnvelope("environment-blueprint-revision", manifest)
}

func decodeEnvironmentBlueprintManifest(value []byte) (environmentBlueprintManifest, error) {
	manifest, err := decodeEnvelope[environmentBlueprintManifest](value, "environment-blueprint-revision")
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
	if err := validateID(ids.KindEnvironment, revision.EnvironmentID); err != nil {
		return err
	}
	if err := validateID(ids.KindTask, revision.RevisionID); err != nil {
		return err
	}
	if err := validateTimestamp("Blueprint revision created_at", revision.CreatedAt); err != nil {
		return err
	}
	manifest := environmentBlueprintManifest{
		EnvironmentID: revision.EnvironmentID, RevisionID: revision.RevisionID,
		RootPath: revision.RootPath, ComposeSources: revision.ComposeSources,
		Interpolation: revision.Interpolation, CreatedAt: revision.CreatedAt.Format(time.RFC3339Nano),
		Files: make([]environmentBlueprintManifestFile, len(revision.Files)),
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
	if validateID(ids.KindEnvironment, manifest.EnvironmentID) != nil ||
		validateID(ids.KindTask, manifest.RevisionID) != nil {
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
		if validateEnvironmentBlueprintPath(file.Path) != nil || (index > 0 && file.Path <= previous) ||
			file.Size < 0 || file.Size > environmentBlueprintMaxFileBytes ||
			len(file.SHA256) != sha256.Size*2 {
			return errs.New(errs.KindValidationFailed, "Blueprint revision file metadata is invalid")
		}
		decoded, err := hex.DecodeString(file.SHA256)
		if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != file.SHA256 {
			return errs.New(errs.KindValidationFailed, "Blueprint revision file digest is invalid")
		}
		totalBytes += file.Size
		if totalBytes > environmentBlueprintMaxTotalBytes {
			return errs.New(errs.KindValidationFailed, "Blueprint revision exceeds its total size limit")
		}
		declared[file.Path] = struct{}{}
		previous = file.Path
	}
	seenSources := make(map[string]struct{}, len(manifest.ComposeSources))
	for _, source := range manifest.ComposeSources {
		if validateEnvironmentBlueprintPath(source) != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint revision Compose source is invalid")
		}
		if _, exists := declared[source]; !exists {
			return errs.New(errs.KindValidationFailed, "Blueprint revision Compose source is undeclared")
		}
		if _, duplicate := seenSources[source]; duplicate {
			return errs.New(errs.KindValidationFailed, "Blueprint revision Compose sources are duplicated")
		}
		seenSources[source] = struct{}{}
	}
	for key, value := range manifest.Interpolation {
		if !environmentBlueprintInterpolationKey.MatchString(key) || !utf8.ValidString(value) ||
			strings.ContainsRune(value, 0) {
			return errs.New(errs.KindValidationFailed, "Blueprint revision interpolation is invalid")
		}
	}
	return nil
}

func validateEnvironmentBlueprintPath(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, 0) ||
		len(value) > environmentBlueprintMaxPathBytes || strings.Contains(value, `\`) || path.IsAbs(value) {
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
