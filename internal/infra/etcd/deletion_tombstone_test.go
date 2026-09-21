package etcd

import (
	"errors"
	"reflect"
	"testing"
	"time"

	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: restart recovery depends on a closed tombstone retaining the exact target revision, Task, phase, and
// stable-id checkpoint without a compatibility or inferred representation.
func TestDeletionTombstoneCodecRoundTripsClosedState(t *testing.T) {
	at := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	record := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetEnvironment, TargetID: "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TargetRevision: 41, TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Phase: testdeletions.DeletionPhaseFinalizing,
		Checkpoint: testdeletions.DeletionCheckpoint{
			ResourceKind: "service",
			StableID:     "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		},
		CreatedAt: at, UpdatedAt: at.Add(time.Second),
	}
	encoded, err := testdeletions.EncodeDeletionTombstone(record)
	if err != nil {
		t.Fatalf("encodeDeletionTombstone() error = %v", err)
	}
	decoded, err := testdeletions.DecodeDeletionTombstone(encoded)
	if err != nil {
		t.Fatalf("decodeDeletionTombstone() error = %v", err)
	}
	if !reflect.DeepEqual(decoded, record) {
		t.Fatalf("decoded = %#v, want %#v", decoded, record)
	}
}

// Rationale: a partial checkpoint or wrong-kind target cannot safely fence or resume destructive postorder work.
func TestDeletionTombstoneRejectsAmbiguousIdentity(t *testing.T) {
	at := time.Date(2026, 8, 22, 10, 0, 0, 0, time.UTC)
	record := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetEnvironment, TargetID: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TargetRevision: 1, TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Phase: testdeletions.DeletionPhaseHostEffects, Checkpoint: testdeletions.DeletionCheckpoint{ResourceKind: "service"},
		CreatedAt: at, UpdatedAt: at,
	}
	if err := testdeletions.ValidateDeletionTombstone(record); !errors.Is(
		err,
		errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateDeletionTombstone() error = %v, want validation failure", err)
	}
}

// Rationale: Connector finalization participates in the closed deletion catalog and must retain a Connector stable id.
func TestDeletionTombstoneAcceptsConnectorTarget(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 23, 10, 0, 0, 0, time.UTC)
	record := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetConnector, TargetID: "con_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TargetRevision: 1, TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Phase: testdeletions.DeletionPhaseFinalizing, CreatedAt: at, UpdatedAt: at,
	}
	if err := testdeletions.ValidateDeletionTombstone(record); err != nil {
		t.Fatalf("validateDeletionTombstone() error = %v", err)
	}
}

func TestDeletionTombstoneAcceptsRunnerTarget(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 8, 29, 2, 20, 0, 0, time.UTC)
	record := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetRunner, TargetID: "run_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TargetRevision: 1, TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Phase: testdeletions.DeletionPhaseFinalizing, CreatedAt: at, UpdatedAt: at,
	}
	if err := testdeletions.ValidateDeletionTombstone(record); err != nil {
		t.Fatalf("validateDeletionTombstone() error = %v", err)
	}
}

// Rationale: restart-safe Blueprint finalization must derive its checkpoint only from the closed Environment revision key grammar.
func TestEnvironmentBlueprintRevisionIDFromKey(t *testing.T) {
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	revisionID := "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	prefix := testblueprints.EnvironmentBlueprintRevisionsPrefix(environmentID)
	got, err := environmentBlueprintRevisionIDFromKey(
		prefix,
		prefix+revisionID+"/manifest",
	)
	if err != nil || got != revisionID {
		t.Fatalf("environmentBlueprintRevisionIDFromKey() = %q, %v", got, err)
	}
	if _, err := environmentBlueprintRevisionIDFromKey(prefix, prefix+"invalid/manifest"); err == nil {
		t.Fatal("environmentBlueprintRevisionIDFromKey() accepted an invalid revision id")
	}
}
