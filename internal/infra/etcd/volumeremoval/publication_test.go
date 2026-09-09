package volumeremoval

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the closed publication input must be directly resumable by the
// real runtime owner, without Create or separately advancing checkpoints.
func TestVolumeRemovalInitialPublicationResumesSharedRecords(t *testing.T) {
	store, repository, runtime, task, _ := environmentVolumeRemovalRuntimeFixture(t)
	runtime.Checkpoint = removalrecord.DesiredPublished
	runtime.DesiredRevisionID = task.ID
	publication, err := removalrecord.PrepareInitialPublication(runtime)
	if err != nil {
		t.Fatalf("prepare initial record set: %v", err)
	}
	preparedRuntime, attempt, progress, err := publication.Records()
	if err != nil {
		t.Fatal(err)
	}
	if preparedRuntime != runtime || attempt.OperationID != runtime.OperationID ||
		attempt.OriginTaskID != task.ID || attempt.TaskID != task.ID ||
		attempt.PredecessorTaskID != "" || attempt.Ordinal != 1 || attempt.CreatedAt != runtime.CreatedAt ||
		progress.OperationID != runtime.OperationID || progress.NextRequestOrdinal != 1 ||
		len(progress.Cursor) != 0 || len(progress.ComponentStack) != 0 || progress.DirectoryAbsent ||
		progress.UpdatedAt != runtime.CreatedAt {
		t.Fatal("publication did not derive the original attempt and empty traversal")
	}
	runtimeValue, err := removalrecord.EncodeRuntime(preparedRuntime)
	if err != nil {
		t.Fatal(err)
	}
	attemptValue, err := removalrecord.EncodeAttempt(attempt)
	if err != nil {
		t.Fatal(err)
	}
	progressValue, err := removalrecord.EncodeProgress(progress)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(runtimeValue)
	defer clear(attemptValue)
	defer clear(progressValue)
	// Storage is the double here. This proves record interoperability only, not
	// the complete desired/Task/ownership publication transaction.
	result, err := store.Transact(context.Background(), nil, []etcd.Mutation{
		{Type: etcd.MutationPut, Key: removalrecord.RuntimeKey(runtime.OperationID), Value: runtimeValue},
		{Type: etcd.MutationPut, Key: removalrecord.AttemptKey(runtime.OperationID, 1), Value: attemptValue},
		{Type: etcd.MutationPut, Key: removalrecord.ProgressKey(runtime.OperationID), Value: progressValue},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("persist prepared record set: %v", err)
	}
	resumed, err := repository.Resume(context.Background(), runtime.OperationID)
	if err != nil {
		t.Fatalf("resume published initial records: %v", err)
	}
	if resumed.Runtime.Record != runtime || resumed.Attempt != attempt ||
		!sameEnvironmentVolumeRemovalProgress(resumed.Progress.Record, progress) || resumed.Pending != nil ||
		resumed.Runtime.Revision != result.Revision || resumed.Progress.Revision != result.Revision {
		t.Fatal("runtime owner did not resume the exact prepared initial state")
	}
	preparedRuntime.Key = "changed"
	progress.Cursor = []byte("changed")
	againRuntime, _, againProgress, err := publication.Records()
	if err != nil || againRuntime != runtime || len(againProgress.Cursor) != 0 {
		t.Fatal("returned records can mutate the preparation")
	}
}

// Rationale: independently valid runtime states are not all initial publication
// inputs. A retry, later checkpoint, or different DELETE must not be smuggled in.
func TestVolumeRemovalInitialPublicationRejectsNonInitialState(t *testing.T) {
	_, _, runtime, task, _ := environmentVolumeRemovalRuntimeFixture(t)
	runtime.Checkpoint = removalrecord.DesiredPublished
	runtime.DesiredRevisionID = task.ID
	for name, change := range map[string]func(*removalrecord.Runtime){
		"zero":             func(record *removalrecord.Runtime) { *record = removalrecord.Runtime{} },
		"unpublished":      func(record *removalrecord.Runtime) { record.Checkpoint = removalrecord.IntentSealed },
		"only staged":      func(record *removalrecord.Runtime) { record.Checkpoint = removalrecord.RevisionStaged },
		"already detached": func(record *removalrecord.Runtime) { record.Checkpoint = removalrecord.ConsumersDetached },
		"already absent":   func(record *removalrecord.Runtime) { record.Checkpoint = removalrecord.DirectoryAbsent },
		"finalized":        func(record *removalrecord.Runtime) { record.Checkpoint = removalrecord.RuntimeFinalized },
		"successor": func(record *removalrecord.Runtime) {
			record.AttemptOrdinal = 2
			record.PredecessorTaskID = record.OriginTaskID
			record.CurrentTaskID = ids.NewAt(ids.KindTask, record.CreatedAt, 99)
		},
		"different revision": func(record *removalrecord.Runtime) {
			record.DesiredRevisionID = ids.NewAt(ids.KindTask, record.CreatedAt, 99)
		},
		"advanced time":  func(record *removalrecord.Runtime) { record.UpdatedAt = record.CreatedAt.Add(time.Second) },
		"other method":   func(record *removalrecord.Runtime) { record.RootLocator.Method = "POST" },
		"other route":    func(record *removalrecord.Runtime) { record.RootLocator.Route = "/environments/{id}" },
		"missing impact": func(record *removalrecord.Runtime) { clear(record.ImpactSHA256[:]) },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := runtime
			change(&candidate)
			publication, err := removalrecord.PrepareInitialPublication(candidate)
			if !isKind(err, errs.KindValidationFailed) {
				t.Fatalf("non-initial state accepted: %v", err)
			}
			if _, _, _, err := publication.Records(); !isKind(err, errs.KindValidationFailed) {
				t.Fatalf("failed preparation exposed usable records: %v", err)
			}
		})
	}
	if _, _, _, err := (removalrecord.InitialPublication{}).Records(); !isKind(err, errs.KindValidationFailed) {
		t.Fatalf("zero preparation accepted: %v", err)
	}
}
