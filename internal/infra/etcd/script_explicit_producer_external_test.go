package etcd_test

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testlocalagents "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type explicitProducerPorts struct {
	t          *testing.T
	imageCalls int
	reference  string
}

func (ports *explicitProducerPorts) GetSingleton(
	context.Context,
) (testkeyvalue.Versioned[testlocalagents.LocalAgentRecord], error) {
	return testkeyvalue.Versioned[testlocalagents.LocalAgentRecord]{
		Record: testlocalagents.LocalAgentRecord{ID: "setup-agent"},
	}, nil
}

func (ports *explicitProducerPorts) ResolveWorkloadImages(
	_ context.Context, agentID string, selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	ports.imageCalls++
	if agentID != "setup-agent" || len(selectors) != 1 {
		ports.t.Fatal("unexpected explicit image request")
	}
	ports.reference = selectors[0].GetRequestedReference()
	return &agentpb.WorkloadImageResolutionResult{
		RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: []*agentpb.WorkloadImageResolution{
				{Selector: proto.CloneOf(selectors[0]), LocalImageId: "sha256:" + strings.Repeat("c", 64)},
			},
		}},
	}, nil
}

func (ports *explicitProducerPorts) ResolveTaskMaterializationSource(
	context.Context, string, testtaskmaterialization.Source,
) ([]byte, error) {
	ports.t.Fatal("grant-free setup resolved an Entry value")
	return nil, nil
}

// Rationale: the real Controller producer must emit a machine-valid separate
// image and pass actual stored-source publication, not just a hand-built DTO.
func TestScriptExplicitProducerPublishesSeparatePreparedImage(t *testing.T) {
	fixture, input, ports := explicitProducerInput(t)
	plan, err := testtaskplanning.BuildManualScriptPlan(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, projection := plan.ScriptRunnerSnapshots[0], plan.ScriptRunnerProjections[0]
	if snapshot.ExplicitExecution == nil || snapshot.LocalImageId != "sha256:"+strings.Repeat("c", 64) ||
		snapshot.ExplicitExecution.ReleaseLocalImageId != fixture.Sources.Release.Intent.CandidateWorkload.LocalImageID ||
		snapshot.ExplicitExecution.ScriptModRevision != uint64(fixture.Sources.Script.Revision) ||
		projection.Image != snapshot.LocalImageId || projection.Uid != 0 || projection.Gid != 0 || projection.WorkingDir != "/" ||
		plan.ScriptBodyArtifacts[0].Uid != 0 || plan.ScriptBodyArtifacts[0].Gid != 0 ||
		ports.imageCalls != 1 || ports.reference != fixture.Sources.Script.Record.Desired.Execution.Image {
		t.Fatal("Controller lost the separately prepared image, user or source identity")
	}
	fixture.Publish(t, plan)
	if ports.imageCalls != 1 {
		t.Fatal("publication re-resolved the image")
	}
	input.Preparation = testtaskplanning.ScriptRunnerPreparation{}
	if _, err := testtaskplanning.BuildManualScriptPlan(context.Background(), input); err == nil {
		t.Fatal("explicit producer accepted caller input without image preparation")
	}
}

// Rationale: restart/retry uses frozen hook material and must retain setup image
// authority without consulting an image registry or recapturing current context.
func TestScriptExplicitProducerFrozenHookPreservesPreparation(t *testing.T) {
	fixture, input, ports := explicitProducerInput(t)
	input.Sources.Script.Record.Desired.When = core.ScriptPreDeploy
	hook, err := testtaskplanning.BuildReleaseHookRenderInput(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := testtaskplanning.BuildReleaseHookPlan(testtaskplanning.ReleaseHookPlanInput{
		Operation: domain.OperationDeploy, CandidateReleaseID: fixture.Sources.Release.Intent.ID,
		PreStepIDs: []string{input.StepID}, Hooks: []testreleaserender.ReleaseHookRenderInput{hook},
	})
	if err != nil {
		t.Fatal(err)
	}
	plan := &agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: input.PlanID,
		RenderGeneration: fixture.Sources.RenderInput.Record.Projection.RenderGeneration,
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_SCRIPT, TargetId: fixture.Sources.Script.Record.Desired.ID,
		ScriptRunnerSnapshots: replayed.Snapshots, ScriptRunnerProjections: replayed.Projections,
		ScriptBodyArtifacts: replayed.Bodies, Steps: replayed.PreSteps,
	}
	if _, err := executionplan.Seal(plan); err != nil {
		t.Fatalf("frozen explicit hook lost machine validity: %v", err)
	}
	if ports.imageCalls != 1 || replayed.Snapshots[0].ExplicitExecution == nil ||
		replayed.Projections[0].Image != "sha256:"+strings.Repeat("c", 64) {
		t.Fatal("frozen hook changed or re-resolved its setup image")
	}
}

func explicitProducerInput(
	t *testing.T,
) (*etcd.ManualScriptAdmissionFixture, testtaskplanning.ManualScriptPlanInput, *explicitProducerPorts) {
	t.Helper()
	fixture := etcd.NewExplicitManualScriptAdmissionFixture(t, core.ScriptExecution{
		Mode: core.ScriptExecutionExplicit, Image: "example/setup@sha256:" + strings.Repeat("b", 64), User: "0:0",
	})
	ports := &explicitProducerPorts{t: t}
	artifacts, err := testtaskplanning.NewScriptArtifactService(fixture.Scripts, ports)
	if err != nil {
		t.Fatal(err)
	}
	preparation, err := testtaskplanning.NewScriptRunnerPreparationService(artifacts, ports, ports)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := preparation.Prepare(context.Background(), fixture.Sources)
	if err != nil {
		t.Fatal(err)
	}
	return fixture, testtaskplanning.ManualScriptPlanInput{
		TaskID: fixture.Task.ID, OperationID: fixture.Task.OperationID, PlanID: fixture.Task.PlanID,
		StepID: fixture.Execution.StepID, ExecutionID: fixture.Execution.ID, SnapshotID: fixture.Execution.SnapshotID,
		Sources: fixture.Sources, Preparation: prepared,
	}, ports
}
