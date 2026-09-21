package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// Rationale: Blueprint must compare Script context before its otherwise durable
// hook staging, not only when deriving source memberships after staging.
func TestScriptContextBlueprintPreparationRejectsSubstitutionBeforeWrites(t *testing.T) {
	for _, change := range []string{"invented explicit context", "missing captured context"} {
		t.Run(change, func(t *testing.T) {
			sources, snapshot := scriptContextSourcesForTest(t)
			if change == "invented explicit context" {
				sources.Script.Record.Desired.Execution = nil
			} else {
				snapshot.ExplicitExecution = nil
			}
			execution := scriptCheckpointTestRecord(time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC))
			snapshot.SnapshotId, snapshot.ScriptExecutionId = execution.SnapshotID, execution.ID
			snapshot.EnvironmentId, snapshot.ServiceId, snapshot.ReleaseId = execution.EnvironmentID, execution.ServiceID, execution.ReleaseID
			value, err := proto.Marshal(snapshot)
			if err != nil {
				t.Fatal(err)
			}
			execution.Snapshot, execution.SnapshotSHA256 = value, scriptSourceReferenceBytesDigest(value)
			task := TaskRecord{
				ID: execution.CurrentTaskID, OperationID: execution.OperationID, PlanHash: execution.PlanHash,
				Params: map[string]string{
					testreleaserender.ReleaseHookStepExecutionParam(execution.StepID): execution.ID,
				},
			}
			store := &releasePlanningTestStore{memoryHierarchyStore: newMemoryHierarchyStore()}
			before := store.revision
			_, err = (releaseLedgerFixture(t, store)).PrepareBlueprintReleaseHooks(
				context.Background(), task, []ReleaseHookExecutionPublication{{Sources: sources, Execution: execution}},
			)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || store.revision != before {
				t.Fatalf("invalid context was not rejected before Blueprint staging: %v", err)
			}
		})
	}
}
