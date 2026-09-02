package etcd

import (
	"bytes"
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

// Rationale: the owning Blueprint transaction must publish each deduplicated
// candidate source under the exact predecessor-or-absence state selected at its fixed read.
func TestBlueprintReleasePublicationPutsSharedStagedSourcesWithPredecessorFences(t *testing.T) {
	t.Parallel()
	for _, raceKey := range []string{"", "projection", "service"} {
		raceKey := raceKey
		t.Run(raceKey, func(t *testing.T) {
			t.Parallel()
			store, operationID, members, stage, projectionKey, projectionValue :=
				scriptSharedProjectionSourceFixture(t)
			serviceID := ids.NewAt(ids.KindService, time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC), 168)
			serviceKey := serviceRuntimeKey(serviceID)
			serviceValue := scriptSourceServiceValue(t, stage.EnvironmentID, serviceID)
			serviceStage := stage
			serviceStage.CanonicalValueSHA256 = sha256.Sum256(serviceValue)
			members = append(members, ScriptSourcePreparationMember{
				Reference: ScriptSourceReference{
					OperationID: operationID, ScriptExecutionID: members[0].Reference.ScriptExecutionID,
					Source:        ScriptSourceIdentity{Kind: ScriptSourceService, ServiceID: serviceID},
					SourceOwnerID: stage.EnvironmentID,
				},
				Evidence: ScriptSourceEvidence{Staged: &ScriptStagedSourceEvidence{
					SourceKey: serviceKey, Stage: serviceStage, Value: serviceValue,
				}},
			})
			authority, err := newScriptSourceReferenceAuthority(store)
			if err != nil {
				t.Fatal(err)
			}
			prepared, err := authority.Prepare(context.Background(), operationID, members)
			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			sourceFragment, err := authority.FinalPublicationFragment(context.Background(), prepared)
			if err != nil {
				t.Fatalf("FinalPublicationFragment() error = %v", err)
			}
			publication, err := newBlueprintReleasePublication(blueprintReleasePublicationInput{
				EnvironmentID: stage.EnvironmentID, OperationID: operationID,
				SourceFragment: sourceFragment, SourceAuthority: authority, SourceMembers: members,
			})
			if err != nil {
				t.Fatalf("newBlueprintReleasePublication() error = %v", err)
			}
			defer publication.Clear()
			if len(publication.sources.StagedRequirements()) != 2 ||
				len(publication.conditions) != 4 || len(publication.mutations) != 4 {
				t.Fatalf(
					"publication staged shape = %d requirements, %d conditions, %d mutations",
					len(publication.sources.StagedRequirements()), len(publication.conditions), len(publication.mutations),
				)
			}
			selected := map[string]int64{}
			for _, condition := range publication.conditions {
				if condition.Key == projectionKey || condition.Key == serviceKey {
					selected[condition.Key] = condition.ModRevision
				}
			}
			if selected[projectionKey] != stage.FixedReadRevision || selected[serviceKey] != 0 {
				t.Fatalf("selected staged predecessors = %#v", selected)
			}
			claim := EnvironmentBlueprintStageClaim{
				EnvironmentID: stage.EnvironmentID, RevisionID: stage.RevisionID,
				RenderGeneration: stage.RenderGeneration,
			}
			if err = publication.sources.ValidateStagedMutations(claim, publication.mutations); err != nil {
				t.Fatalf("ValidateStagedMutations(production fragment) error = %v", err)
			}
			if raceKey != "" {
				key := projectionKey
				if raceKey == "service" {
					key = serviceKey
				}
				raced, raceErr := store.Transact(context.Background(), nil, []Mutation{{
					Type: MutationPut, Key: key, Value: []byte("concurrent"),
				}})
				if raceErr != nil || !raced.Succeeded {
					t.Fatalf("publish concurrent change = %#v, %v", raced, raceErr)
				}
			}
			result, err := store.Transact(context.Background(), publication.conditions, publication.mutations)
			if err != nil {
				t.Fatalf("publish Blueprint fragment error = %v", err)
			}
			if raceKey != "" {
				if result.Succeeded || store.valueAt(scriptSourceRootKey(operationID), store.revision) != nil {
					t.Fatalf("raced Blueprint publication = %#v; source root became active", result)
				}
				return
			}
			if !result.Succeeded {
				t.Fatal("exact Blueprint publication did not commit")
			}
			if stored := store.valueAt(projectionKey, store.revision); stored == nil ||
				!bytes.Equal(stored.Value, projectionValue) {
				t.Fatal("Blueprint publication omitted the candidate projection")
			}
			if stored := store.valueAt(serviceKey, store.revision); stored == nil ||
				!bytes.Equal(stored.Value, serviceValue) {
				t.Fatal("Blueprint publication omitted the candidate Service")
			}
		})
	}
}
