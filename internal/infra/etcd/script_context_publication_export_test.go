package etcd

import (
	"context"
	"encoding/hex"
	"errors"
	"testing"

	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
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
	for _, key := range []string{testtaskjournal.TaskStorageKey(fixture.Task.ID), testscriptexecutions.ScriptExecutionKey(execution.ID), testscriptexecutions.ScriptRunnerSnapshotKey(execution.SnapshotID), testscriptsourceevidence.ScriptSourceRootKey(fixture.Task.OperationID), testscriptsourcereference.PreparationKey(fixture.Task.OperationID)} {
		if fixture.store.valueAt(key, fixture.store.revision) != nil {
			t.Fatal("invalid context left a Task, runner snapshot or source preparation")
		}
	}
}
