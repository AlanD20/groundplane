package etcd

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testplatformcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/platformcomponents"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// Rationale: automatic resolver replay must bind physical CAS to the exact
// final live artifact while keeping failed-attempt lineage out of the prior
// observation fence and suppressing successors when compensation is unproved.
func TestPlatformResolverFinalLiveLineageReplaysExactTerminalEvidence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 15, 0, 0, 0, time.UTC)
	componentID := ids.NewAt(ids.KindComponent, now, 1)
	serviceID := ids.NewAt(ids.KindService, now, 2)
	artifactID := ids.NewAt(ids.KindConfig, now, 3)
	priorArtifactID := ids.NewAt(ids.KindConfig, now, 6)
	predecessorID := ids.NewAt(ids.KindTask, now, 4)
	priorTaskID := ids.NewAt(ids.KindTask, now, 5)
	candidateSHA256 := strings.Repeat("a", 64)
	priorSHA256 := strings.Repeat("b", 64)
	input := testplatformcomponents.PlatformComponentTaskRenderInput{
		ComponentID: componentID, GeneratedServiceID: serviceID, ArtifactID: artifactID,
		ArtifactSHA256: candidateSHA256, PriorObservationModRevision: 41,
		PriorObservationRevision: 7, PredecessorTaskID: priorTaskID,
		ExpectedPreviousArtifactSHA256: priorSHA256,
		ExpectedPreviousArtifactID:     priorArtifactID, ExpectedPreviousGeneration: 2,
	}
	candidate := &testtaskjournal.TaskDNSResolverObservationEvidence{
		ComponentID: componentID, ServiceID: serviceID, ArtifactID: artifactID,
		ArtifactSHA256: candidateSHA256, RenderGeneration: 3,
	}
	rollback := &testtaskjournal.TaskDNSResolverObservationEvidence{
		ComponentID: componentID, ServiceID: serviceID, ArtifactID: priorArtifactID,
		ArtifactSHA256: priorSHA256, RenderGeneration: 2,
	}

	tests := []struct {
		name        string
		predecessor TaskRecord
		input       testplatformcomponents.PlatformComponentTaskRenderInput
		want        platformResolverLiveLineage
		wantProven  bool
	}{
		{
			name: "successful candidate",
			predecessor: TaskRecord{ID: predecessorID, Status: testtaskjournal.TaskStatusCompleted, RenderGeneration: 3,
				Result: &testtaskjournal.TaskResultRecord{
					Kind:                            testtaskjournal.TaskResultCompose,
					DNSResolverCandidateObservation: candidate,
				}},
			input: input,
			want: platformResolverLiveLineage{priorObservationRevision: 8, predecessorTaskID: predecessorID,
				expectedPreviousArtifactSHA256: candidateSHA256,
				expectedPreviousArtifactID:     artifactID, expectedPreviousGeneration: 3},
			wantProven: true,
		},
		{
			name: "compensated rollback to prior bytes",
			predecessor: TaskRecord{ID: predecessorID, Status: testtaskjournal.TaskStatusFailed, RenderGeneration: 3,
				Result: &testtaskjournal.TaskResultRecord{
					Kind:                           testtaskjournal.TaskResultCompose,
					DNSResolverRollbackObservation: rollback,
				}},
			input: input,
			want: platformResolverLiveLineage{priorObservationModRevision: 41, priorObservationRevision: 7,
				predecessorTaskID: priorTaskID, expectedPreviousArtifactSHA256: priorSHA256,
				expectedPreviousArtifactID: priorArtifactID, expectedPreviousGeneration: 2},
			wantProven: true,
		},
		{
			name: "compensated rollback to prior absence",
			predecessor: TaskRecord{ID: predecessorID, Status: testtaskjournal.TaskStatusAborted, RenderGeneration: 3,
				Result: &testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose}},
			input: testplatformcomponents.PlatformComponentTaskRenderInput{
				ComponentID:        componentID,
				GeneratedServiceID: serviceID,
				ArtifactID:         artifactID,
				ArtifactSHA256:     candidateSHA256,
			},
			want:       platformResolverLiveLineage{},
			wantProven: true,
		},
		{
			name: "unproved failed compensation",
			predecessor: TaskRecord{ID: predecessorID, Status: testtaskjournal.TaskStatusTimedOut, RenderGeneration: 3,
				Result: &testtaskjournal.TaskResultRecord{
					Kind:                   testtaskjournal.TaskResultCompose,
					ReconciliationRequired: true,
				}},
			input:      input,
			wantProven: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			first, firstProven := platformResolverFinalLiveLineage(test.predecessor, test.input)
			second, secondProven := platformResolverFinalLiveLineage(test.predecessor, test.input)
			if firstProven != test.wantProven || secondProven != test.wantProven || first != test.want ||
				second != test.want {
				t.Fatalf("replayed lineage = %#v/%t then %#v/%t, want %#v/%t",
					first, firstProven, second, secondProven, test.want, test.wantProven)
			}
		})
	}
}

// Rationale: retry creation validates the immutable origin; later readers must
// accept that pinned origin across more than one retry generation.
func TestPlatformResolverTaskInputBelongsToTaskAcceptsPinnedRetryOrigin(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 30, 16, 0, 0, 0, time.UTC)
	originID := ids.NewAt(ids.KindTask, now, 1)
	retryID := ids.NewAt(ids.KindTask, now, 2)
	retry := TaskRecord{ID: retryID, RetryOf: originID}

	if !platformResolverTaskInputBelongsToTask(
		retry,
		testplatformcomponents.PlatformComponentTaskRenderInput{TaskID: originID},
	) {
		t.Fatal("retry predecessor rejected its originating render input")
	}
	if platformResolverTaskInputBelongsToTask(
		TaskRecord{ID: retryID}, testplatformcomponents.PlatformComponentTaskRenderInput{TaskID: originID},
	) {
		t.Fatal("non-retry predecessor accepted another Task's render input")
	}
}
