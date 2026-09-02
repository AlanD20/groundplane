package etcd

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"time"
)

func TestBlueprintReleasePublicationCarriesPreparedSourceRootAndAbandonsExactly(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, authority, operationID, members := scriptSourceReferenceFixture(t)
	prepared, err := authority.Prepare(ctx, operationID, members)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	fragment, err := authority.FinalPublicationFragment(ctx, prepared)
	if err != nil {
		t.Fatalf("FinalPublicationFragment() error = %v", err)
	}
	publication, err := newBlueprintReleasePublication(
		blueprintReleasePublicationInput{
			EnvironmentID:   members[0].Reference.SourceOwnerID,
			OperationID:     operationID,
			SourceFragment:  fragment,
			SourceAuthority: authority,
			SourceMembers:   members,
		},
	)
	if err != nil {
		t.Fatalf("newBlueprintReleasePublication() error = %v", err)
	}
	if publication.IsZero() || len(publication.mutations) != len(fragment.mutations) {
		t.Fatalf("publication = %#v", publication)
	}
	if err := publication.Abandon(ctx); err != nil {
		t.Fatalf("Abandon() error = %v", err)
	}
	if read, readErr := store.Get(ctx, scriptSourcePreparationPrefix+operationID); readErr != nil || read.Entry != nil {
		t.Fatalf("prepared source descriptor after abandon = %#v, %v", read, readErr)
	}
}

func TestBlueprintReleaseHookPublicationStoresExecutionAndSnapshot(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 13, 0, 0, 0, time.UTC)
	record := scriptCheckpointTestRecord(at)
	task := TaskRecord{
		ID: record.CurrentTaskID, OperationID: record.OperationID, PlanHash: record.PlanHash,
		Params: map[string]string{ReleaseHookStepExecutionParam(record.StepID): record.ID},
	}
	fragment, err := prepareBlueprintReleaseHookPublicationFragment(
		task,
		[]ReleaseHookExecutionPublication{{Execution: record}},
	)
	if err != nil {
		t.Fatalf("prepareBlueprintReleaseHookPublicationFragment() error = %v", err)
	}
	defer clearReleaseHookPublicationFragment(fragment)
	if len(fragment.mutations) != 2 || fragment.mutations[0].Key != scriptExecutionKey(record.ID) ||
		fragment.mutations[1].Key != scriptRunnerSnapshotKey(record.SnapshotID) {
		t.Fatalf("Blueprint hook mutations = %#v", fragment.mutations)
	}
	stored, err := decodeEnvelope[ScriptExecutionRecord](fragment.mutations[0].Value, "script-execution")
	if err != nil || stored.ID != record.ID || stored.CurrentTaskID != task.ID {
		t.Fatalf("stored Blueprint Script execution = %#v, error = %v", stored, err)
	}
	snapshot, err := decodeEnvelope[storedScriptRunnerSnapshot](fragment.mutations[1].Value, "script-runner-snapshot")
	if err != nil || snapshot.ExecutionID != record.ID || snapshot.SnapshotID != record.SnapshotID {
		t.Fatalf("stored Blueprint runner snapshot = %#v, error = %v", snapshot, err)
	}
}

func TestBlueprintStagedSourceEvidenceRejectsChangedCandidateBytes(t *testing.T) {
	t.Parallel()
	at := time.Date(2026, 9, 2, 14, 0, 0, 0, time.UTC)
	environmentID := ids.NewAt(ids.KindEnvironment, at, 1)
	revisionID := ids.NewAt(ids.KindTask, at, 2)
	expected := []byte("candidate-service-bytes")
	digest := sha256.Sum256(expected)
	authority := &agentpb.ScriptSourceAuthority{
		Staged: &agentpb.ScriptStagedSourceAuthority{
			EnvironmentId:        environmentID,
			RevisionId:           revisionID,
			RenderGeneration:     7,
			FixedReadRevision:    19,
			CanonicalValueSha256: digest[:],
		},
	}
	if _, err := blueprintStagedSourceEvidence(
		authority,
		serviceRuntimeKey(ids.NewAt(ids.KindService, at, 3)),
		[]byte("changed-service-bytes"),
		environmentID,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("blueprintStagedSourceEvidence(changed bytes) error = %v", err)
	}
}
