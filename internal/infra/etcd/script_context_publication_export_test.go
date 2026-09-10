package etcd

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AssertContextPublicationRejected checks the real publication boundary without
// exposing the hermetic store to the external Controller-producer test.
func (fixture *ManualScriptAdmissionFixture) AssertContextPublicationRejected(
	t *testing.T,
	plan *agentpb.ExecutionPlan,
) {
	t.Helper()
	fixture.Task.PlanID, fixture.Task.PlanHash = plan.PlanId, hex.EncodeToString(plan.PlanHash)
	fixture.Task.RenderGeneration = int32(plan.RenderGeneration)
	execution, err := NewScriptExecutionRecord(fixture.Task, plan, fixture.Task.CreatedAt)
	if err != nil {
		t.Fatalf("valid machine plan did not reach source publication: %v", err)
	}
	before := fixture.store.revision
	_, err = fixture.Scripts.PublishExecutionWithTask(
		context.Background(), fixture.Sources, execution, fixture.Task, fixture.marker,
	)
	if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("publication accepted invented explicit Script context: %v", err)
	}
	if fixture.store.revision != before {
		t.Fatal("invalid context caused durable writes before rejection")
	}
	for _, key := range []string{
		taskKey(fixture.Task.ID), scriptExecutionKey(execution.ID), scriptRunnerSnapshotKey(execution.SnapshotID),
		scriptSourceRootKey(fixture.Task.OperationID), scriptSourcePreparationKey(fixture.Task.OperationID),
	} {
		if fixture.store.valueAt(key, fixture.store.revision) != nil {
			t.Fatal("invalid context left a Task, runner snapshot or source preparation")
		}
	}
}
