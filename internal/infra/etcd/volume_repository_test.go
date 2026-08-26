package etcd

import (
	"context"
	"net/http"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestVolumeRepositoryCreatesResolvesAndPagesScopedRecords(t *testing.T) {
	// Rationale: Volume identity used by Compose and Backup sources requires one
	// atomic primary/name/owner publication and fixed-revision scoped reads.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := volumeRepositoryTestHierarchy(t)
	records := []VolumeRecord{
		volumeRepositoryTestRecord(t, environment.Record.ID, 960, "app-data"),
		volumeRepositoryTestRecord(t, environment.Record.ID, 961, "uploads"),
	}
	for _, record := range records {
		if _, err := repository.CreateVolume(ctx, environment, project, record); err != nil {
			t.Fatalf("CreateVolume(%s) error = %v", record.Slug, err)
		}
	}
	stored, err := repository.GetVolume(ctx, records[0].ID)
	if err != nil || stored.Record != records[0] {
		t.Fatalf("GetVolume() = %#v, %v", stored, err)
	}
	resolved, err := repository.GetVolumeBySlug(ctx, environment.Record.ID, records[1].Slug)
	if err != nil || resolved.Record != records[1] {
		t.Fatalf("GetVolumeBySlug() = %#v, %v", resolved, err)
	}
	first, err := repository.ListVolumes(ctx, environment.Record.ID, PageRequest{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("ListVolumes(first) = %#v, %v", first, err)
	}
	second, err := repository.ListVolumes(ctx, environment.Record.ID, PageRequest{
		Limit: 1, Cursor: first.NextCursor,
	})
	if err != nil || len(second.Items) != 1 || second.NextCursor != "" || second.Revision != first.Revision {
		t.Fatalf("ListVolumes(second) = %#v, %v", second, err)
	}
}

func TestVolumeRepositoryEnforcesScopedNameAndAncestorFences(t *testing.T) {
	// Rationale: no two Volume ids may map one Environment Compose key, and
	// descendant creation must lose atomically once Environment deletion starts.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := volumeRepositoryTestHierarchy(t)
	first := volumeRepositoryTestRecord(t, environment.Record.ID, 970, "app-data")
	if _, err := repository.CreateVolume(ctx, environment, project, first); err != nil {
		t.Fatalf("CreateVolume(first) error = %v", err)
	}
	duplicate := volumeRepositoryTestRecord(t, environment.Record.ID, 971, "app-data")
	if _, err := repository.CreateVolume(ctx, environment, project, duplicate); !isKind(err, errs.KindNameConflict) {
		t.Fatalf("CreateVolume(duplicate) error = %v", err)
	}
	for name, target := range map[string]func(Versioned[EnvironmentRecord], Versioned[ProjectRecord], VolumeRecord) string{
		"Volume": func(_ Versioned[EnvironmentRecord], _ Versioned[ProjectRecord], record VolumeRecord) string {
			return deletionTombstoneKey("volume", record.ID)
		},
		"Environment": func(environment Versioned[EnvironmentRecord], _ Versioned[ProjectRecord], _ VolumeRecord) string {
			return deletionTombstoneKey("environment", environment.Record.ID)
		},
		"Project": func(_ Versioned[EnvironmentRecord], project Versioned[ProjectRecord], _ VolumeRecord) string {
			return deletionTombstoneKey("project", project.Record.ID)
		},
		"Tenant": func(_ Versioned[EnvironmentRecord], project Versioned[ProjectRecord], _ VolumeRecord) string {
			return deletionTombstoneKey("tenant", project.Record.TenantID)
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			repository, store, environment, project := volumeRepositoryTestHierarchy(t)
			fenced := volumeRepositoryTestRecord(t, environment.Record.ID, 972, "cache")
			key := target(environment, project, fenced)
			transaction, err := store.Transact(
				ctx,
				[]Condition{{Key: key}},
				[]Mutation{{Type: MutationPut, Key: key, Value: []byte("fenced")}},
			)
			if err != nil || !transaction.Succeeded {
				t.Fatalf("install %s fence = %#v, %v", name, transaction, err)
			}
			if _, err := repository.CreateVolume(
				ctx,
				environment,
				project,
				fenced,
			); !isKind(
				err,
				errs.KindResourceInUse,
			) {
				t.Fatalf("CreateVolume(%s fenced) error = %v", name, err)
			}
			if _, err := repository.GetVolume(ctx, fenced.ID); !isKind(err, errs.KindVolumeNotFound) {
				t.Fatalf("GetVolume(after failed create) error = %v", err)
			}
			if _, err := repository.GetVolumeBySlug(
				ctx,
				environment.Record.ID,
				fenced.Slug,
			); !isKind(
				err,
				errs.KindVolumeNotFound,
			) {
				t.Fatalf("GetVolumeBySlug(after failed create) error = %v", err)
			}
		})
	}
}

func TestVolumeRepositoryRejectsDescendantsUntilEnvironmentIsReady(t *testing.T) {
	t.Parallel()
	repository, _, environment, project := volumeRepositoryTestHierarchy(t)
	environment.Record.ProvisioningState = EnvironmentProvisioningProvisioning
	record := volumeRepositoryTestRecord(t, environment.Record.ID, 975, "app-data")
	if _, err := repository.CreateVolume(
		context.Background(),
		environment,
		project,
		record,
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("CreateVolume(provisioning Environment) error = %v", err)
	}
}

func TestVolumeRepositoryIdempotentCreateKeepsExactStableIdentity(t *testing.T) {
	// Rationale: a retried synchronous add must replay one response without
	// allocating another Volume id or publishing records without its marker.
	t.Parallel()
	ctx := context.Background()
	repository, _, environment, project := volumeRepositoryTestHierarchy(t)
	record := volumeRepositoryTestRecord(t, environment.Record.ID, 980, "app-data")
	marker := testDirectMarker()
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodPost, Route: "/volumes", Key: "volume-create-key-0001",
	}
	marker.Response.Status = http.StatusCreated
	first, err := repository.CreateVolumeIdempotent(ctx, environment, project, record, marker)
	if err != nil {
		t.Fatalf("CreateVolumeIdempotent() error = %v", err)
	}
	firstOutcome, firstMarker, firstDomainErr, firstClassifyErr := first.Classify()
	if firstClassifyErr != nil || firstDomainErr != nil || firstOutcome != IdempotencyKnownApplied ||
		firstMarker.Kind != "" {
		t.Fatalf(
			"CreateVolumeIdempotent() classification = %v/%#v/%v/%v",
			firstOutcome,
			firstMarker,
			firstDomainErr,
			firstClassifyErr,
		)
	}
	replayed, err := repository.CreateVolumeIdempotent(ctx, environment, project, record, marker)
	if err != nil {
		t.Fatalf("CreateVolumeIdempotent(replay) error = %v", err)
	}
	replayOutcome, replayMarker, replayDomainErr, replayClassifyErr := replayed.Classify()
	if replayClassifyErr != nil || replayDomainErr != nil || replayOutcome != IdempotencyKnownExisting ||
		replayMarker.Response.Status != http.StatusCreated {
		t.Fatalf(
			"CreateVolumeIdempotent(replay) classification = %v/%#v/%v/%v",
			replayOutcome,
			replayMarker,
			replayDomainErr,
			replayClassifyErr,
		)
	}
	stored, err := repository.GetVolume(ctx, record.ID)
	if err != nil || stored.Record != record {
		t.Fatalf("GetVolume() = %#v, %v", stored, err)
	}
}

// Rationale: Environment visibility and Volume owner membership must come
// from one revision even when deletion commits between the range and proof.
func TestVolumeEnvironmentProjectionFencesDeletionRace(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	repository, store, environment, project := volumeRepositoryTestHierarchy(t)
	record := volumeRepositoryTestRecord(t, environment.Record.ID, 981, "uploads")
	if _, err := repository.CreateVolume(ctx, environment, project, record); err != nil {
		t.Fatalf("CreateVolume() error = %v", err)
	}
	raced := false
	racing := &volumeProjectionRaceStore{hierarchyStore: store}
	racing.afterRange = func() {
		raced = true
		result, err := store.Transact(ctx, nil, []Mutation{
			{Type: MutationDelete, Key: environmentKey(environment.Record.ID)},
			{
				Type:  MutationPut,
				Key:   deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID),
				Value: []byte("deleting"),
			},
		})
		if err != nil || !result.Succeeded {
			t.Fatalf("delete Environment during Volume projection = %#v, %v", result, err)
		}
	}
	projection, err := newVolumeRepository(racing)
	if err != nil {
		t.Fatal(err)
	}
	page, err := projection.ListEnvironmentVolumes(ctx, environment.Record.ID, PageRequest{})
	if err != nil || !raced || len(page.Items) != 1 || page.Items[0].Record != record {
		t.Fatalf("ListEnvironmentVolumes(raced) = %#v, %v; raced=%v", page, err, raced)
	}
	if _, err := projection.ListEnvironmentVolumes(
		ctx, environment.Record.ID, PageRequest{},
	); !isKind(err, errs.KindEnvironmentNotFound) {
		t.Fatalf("ListEnvironmentVolumes(after deletion) error = %v", err)
	}
}

type volumeProjectionRaceStore struct {
	hierarchyStore
	afterRange func()
}

func (store *volumeProjectionRaceStore) Range(
	ctx context.Context,
	request RangeRequest,
) (*RangeResult, error) {
	result, err := store.hierarchyStore.Range(ctx, request)
	if err == nil && store.afterRange != nil {
		after := store.afterRange
		store.afterRange = nil
		after()
	}
	return result, err
}

func volumeRepositoryTestHierarchy(
	t *testing.T,
) (*VolumeRepository, *memoryHierarchyStore, Versioned[EnvironmentRecord], Versioned[ProjectRecord]) {
	t.Helper()
	_, store, environment, project := zoneRepositoryTestHierarchy(t)
	environment.Record.ProvisioningState = EnvironmentProvisioningReady
	repository, err := newVolumeRepository(store)
	if err != nil {
		t.Fatalf("newVolumeRepository() error = %v", err)
	}
	return repository, store, environment, project
}

func volumeRepositoryTestRecord(
	t *testing.T,
	environmentID string,
	offset int64,
	volumeSlug string,
) VolumeRecord {
	t.Helper()
	record, err := NewVolumeRecord(
		environmentID,
		ids.NewAt(ids.KindVolume, serviceRecordTestTime(), offset),
		volumeSlug,
		volumeSlug,
	)
	if err != nil {
		t.Fatalf("NewVolumeRecord() error = %v", err)
	}
	return record
}
