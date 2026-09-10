package etcd_test

import (
	"context"
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: a completely valid, rehashed explicit machine plan cannot invent
// operator authority absent from the real stored Script selected for this run.
func TestScriptContextPublicationRejectsInventedExplicitAuthority(t *testing.T) {
	fixture := etcd.NewManualScriptAdmissionFixture(t)
	plan, err := controller.BuildManualScriptPlan(context.Background(), controller.ManualScriptPlanInput{
		TaskID: fixture.Task.ID, OperationID: fixture.Task.OperationID, PlanID: fixture.Task.PlanID,
		StepID: fixture.Execution.StepID, ExecutionID: fixture.Execution.ID,
		SnapshotID: fixture.Execution.SnapshotID, Sources: fixture.Sources,
	})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, projection := plan.ScriptRunnerSnapshots[0], plan.ScriptRunnerProjections[0]
	declared := &agentpb.ScriptExplicitExecutionContext{
		ImageReference: "example/setup@sha256:" + strings.Repeat("b", 64),
		User:           fmt.Sprintf("%d:%d", projection.Uid, projection.Gid),
	}
	digest, err := executionplan.ScriptExecutionContextDigest(declared)
	if err != nil {
		t.Fatal(err)
	}
	snapshot.ExplicitExecution = &agentpb.ScriptExplicitExecutionAuthority{
		Context: declared, ContextSha256: digest, ScriptModRevision: uint64(fixture.Sources.Script.Revision),
		ReleaseLocalImageId: snapshot.LocalImageId,
	}
	snapshot.LocalImageId = "sha256:" + strings.Repeat("b", 64)
	projection.Image, projection.WorkingDir = snapshot.LocalImageId, "/"
	projectionBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(projection)
	if err != nil {
		t.Fatal(err)
	}
	projectionDigest := sha256.Sum256(projectionBytes)
	snapshot.RunnerProjectionSha256 = projectionDigest[:]
	snapshotBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	snapshotDigest := sha256.Sum256(snapshotBytes)
	plan.Steps[0].GetRunScript().RunnerSnapshotSha256 = snapshotDigest[:]
	plan.PlanHash = nil
	plan, err = executionplan.Seal(plan)
	if err != nil {
		t.Fatalf("tampered plan must remain machine-valid: %v", err)
	}
	fixture.AssertContextPublicationRejected(t, plan)
}
