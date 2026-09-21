package etcd_test

import (
	"context"
	"errors"
	"testing"

	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// Rationale: valid Controller-produced bytes must be admitted while their
// prepared source root is active, and neither plan nor body may escape once
// that same root is changed, closing, retry-available, or absent.
func TestManualScriptArtifactAdmissionRequiresActiveMatchingRoot(t *testing.T) {
	for _, change := range []string{"digest", "count", "releasing", "retry_available", "absent"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			fixture := etcd.NewManualScriptAdmissionFixture(t)
			plan, err := testtaskplanning.BuildManualScriptPlan(ctx, testtaskplanning.ManualScriptPlanInput{
				TaskID: fixture.Task.ID, OperationID: fixture.Task.OperationID, PlanID: fixture.Task.PlanID,
				StepID: fixture.Execution.StepID, ExecutionID: fixture.Execution.ID,
				SnapshotID: fixture.Execution.SnapshotID, Sources: fixture.Sources,
			})
			if err != nil {
				t.Fatal(err)
			}
			fixture.Publish(t, plan)
			admitted, err := fixture.Scripts.GetScriptExecutionPlan(ctx, fixture.Task)
			if err != nil || !proto.Equal(admitted, plan) {
				t.Fatalf("active plan admission = %v", err)
			}
			artifacts, err := fixture.Scripts.ResolveScriptAssignmentArtifacts(ctx, fixture.Task, plan)
			if err != nil || artifacts == nil || len(artifacts.Bodies) != 1 ||
				string(artifacts.Bodies[0].Body) != "exit 0" {
				t.Fatalf("active body admission = %v", err)
			}
			clear(artifacts.Bodies[0].Body)
			// The durable execution must not turn an invalid caller Task into
			// an index-out-of-range panic at this admission boundary.
			invalid := fixture.Task
			invalid.Steps = nil
			if admitted, err := fixture.Scripts.GetScriptExecutionPlan(ctx, invalid); admitted != nil ||
				!errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Errorf("missing-step admission = %v", err)
			}
			fixture.ChangeRoot(t, change)
			if admitted, err := fixture.Scripts.GetScriptExecutionPlan(ctx, fixture.Task); err == nil ||
				admitted != nil {
				t.Errorf("changed root admitted a plan: error=%v", err)
			}
			artifacts, err = fixture.Scripts.ResolveScriptAssignmentArtifacts(ctx, fixture.Task, plan)
			if err == nil || artifacts != nil {
				t.Errorf("changed root admitted a body: error=%v", err)
			}
		})
	}
}
