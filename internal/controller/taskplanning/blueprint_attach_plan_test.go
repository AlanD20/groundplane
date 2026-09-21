package taskplanning

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters/postgres16"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestBlueprintAttachPlanReproducesPublishedProvisionThenCompose(t *testing.T) {
	registerAttachPlanPostgres.Do(postgres16.Register)
	now := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	reader, baseTask := blueprintPlanTestState(t)
	taskID := baseTask.ID
	attachID := ids.NewAt(ids.KindAttach, now, 1)
	backingServiceID := ids.NewAt(ids.KindService, now, 2)
	backingEnvironmentID := ids.NewAt(ids.KindEnvironment, now, 3)
	backingNetworkID := ids.NewAt(ids.KindNetwork, now, 4)
	record, err := testattachments.NewPendingAttachRecord(
		attachID, reader.environment.ID, "api-db", ids.NewAt(ids.KindProject, now, 5),
		backingEnvironmentID, backingServiceID, backingNetworkID, reader.projection.DesiredServices[0].Desired.ID,
		attachID, nil,
		[]testattachments.FactSetMetadata{{Facts: []testattachments.FactDefinition{{Key: "pg16_DATABASE"}}}},
		taskID, now,
	)
	if err != nil {
		t.Fatalf("NewPendingAttachRecord() error = %v", err)
	}
	identity := AttachPlanIdentity{Database: "api_123abc", Role: "api_123abc", Password: []byte("URL_safe-1")}
	intent := testattachments.BlueprintAttachTaskIntent{
		TaskID: taskID, EnvironmentID: reader.environment.ID, Status: testtaskjournal.TaskStatusPending,
		Candidates: []testattachments.Record{record}, CreatedAt: now,
	}
	state := &attachPlanTestState{
		attaches: map[string]testkeyvalue.Versioned[testattachments.Record]{attachID: {Record: record}},
		service: testkeyvalue.Versioned[testservices.ServiceRecord]{Record: testservices.ServiceRecord{
			EnvironmentID: backingEnvironmentID, BackingNetworkID: backingNetworkID,
			Desired: core.Service{ID: backingServiceID, Name: "postgres", Adapter: "postgres:16"},
			Runtime: core.ServiceRuntime{ServiceID: backingServiceID, RuntimeIntent: core.ServiceRuntimeIntentRunning},
		}},
		identity: identity, blueprintIntent: &intent,
	}
	resolver, err := NewTaskPlanResolverWithAttachments("/var/lib/groundplane/vol", reader, state, state, state, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithAttachments() error = %v", err)
	}
	baselineState := *state
	baselineState.blueprintIntent = nil
	baselineResolver, err := NewTaskPlanResolverWithAttachments(
		"/var/lib/groundplane/vol", reader, &baselineState, &baselineState, &baselineState,
		nil,
	)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithAttachments(baseline) error = %v", err)
	}
	baseline, err := baselineResolver.ResolveExecutionPlan(context.Background(), baseTask)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(baseline) error = %v", err)
	}
	procedureStepID := ids.NewAt(ids.KindStep, now, 6)
	task := baseTask
	task.Steps = append([]testtaskjournal.TaskStepRecord(nil), baseTask.Steps[:len(baseTask.Steps)-1]...)
	task.Steps = append(
		task.Steps,
		testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: procedureStepID},
	)
	task.Steps = append(task.Steps, baseTask.Steps[len(baseTask.Steps)-1])
	reproduced, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	procedureTask := task
	procedureTask.Type = testtaskjournal.TaskAttach
	procedureTask.Target = attachID
	procedureTask.Steps = []testtaskjournal.TaskStepRecord{
		{Kind: testtaskjournal.TaskStepOperation, ID: procedureStepID},
	}
	procedures, err := BuildAttachProvisionSteps(procedureTask, record, "postgres:16", identity)
	if err != nil {
		t.Fatalf("BuildAttachProvisionSteps() error = %v", err)
	}
	defer clearAdapterProcedurePasswords(procedures)
	publishedSteps := append([]*agentpb.ExecutionStep(nil), baseline.Steps[:len(baseline.Steps)-1]...)
	publishedSteps = append(publishedSteps, procedures...)
	publishedSteps = append(publishedSteps, baseline.Steps[len(baseline.Steps)-1])
	published, err := testtaskplan.Build(testtaskplan.BuildInput{
		VolumeRoot: "/var/lib/groundplane/vol", PlanID: baseTask.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE,
		TargetID: task.Target, Artifacts: baseline.Artifacts, Steps: publishedSteps,
	})
	if err != nil {
		t.Fatalf("BuildPlan(published) error = %v", err)
	}
	if !bytes.Equal(reproduced.PlanHash, published.PlanHash) || len(reproduced.Steps) != len(baseline.Steps)+1 ||
		reproduced.Steps[len(reproduced.Steps)-2].GetAdapterProcedure() == nil ||
		reproduced.Steps[len(reproduced.Steps)-1].GetComposeApply() == nil {
		t.Fatalf("reproduced Blueprint Attach plan = %#v, published hash %x", reproduced, published.PlanHash)
	}
	if _, err := executionplan.Validate(reproduced); err != nil {
		t.Fatalf("reconstructed Blueprint Attach plan is invalid after return: %v", err)
	}
}

// Rationale: Blueprint Attach replay must reject any persisted representation
// that differs from the published candidate, including nil-vs-empty slices and
// time.Time values that describe the same instant with different internals.
func TestBlueprintAttachRecordEqualPreservesStrictRecordEquality(t *testing.T) {
	createdAt := time.Date(2026, 8, 29, 14, 0, 0, 0, time.UTC)
	sameInstantDifferentLocation := createdAt.In(time.FixedZone("UTC", 0))
	cases := []struct {
		name  string
		left  testattachments.Record
		right testattachments.Record
		want  bool
	}{
		{
			name:  "matching zero values",
			left:  testattachments.Record{CreatedAt: createdAt},
			right: testattachments.Record{CreatedAt: createdAt},
			want:  true,
		},
		{
			name:  "nil grant IDs versus empty grant IDs",
			left:  testattachments.Record{CreatedAt: createdAt},
			right: testattachments.Record{CreatedAt: createdAt, GrantAttachIDs: []string{}},
			want:  false,
		},
		{
			name:  "nil fact sets versus empty fact sets",
			left:  testattachments.Record{CreatedAt: createdAt},
			right: testattachments.Record{CreatedAt: createdAt, FactSets: []testattachments.FactSetMetadata{}},
			want:  false,
		},
		{
			name: "nil facts versus empty facts",
			left: testattachments.Record{CreatedAt: createdAt, FactSets: []testattachments.FactSetMetadata{{}}},
			right: testattachments.Record{
				CreatedAt: createdAt, FactSets: []testattachments.FactSetMetadata{{Facts: []testattachments.FactDefinition{}}},
			},
			want: false,
		},
		{
			name:  "same instant with different time representation",
			left:  testattachments.Record{CreatedAt: createdAt},
			right: testattachments.Record{CreatedAt: sameInstantDifferentLocation},
			want:  false,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := blueprintAttachRecordEqual(test.left, test.right); got != test.want {
				t.Fatalf("blueprintAttachRecordEqual() = %t, want %t", got, test.want)
			}
		})
	}
}
