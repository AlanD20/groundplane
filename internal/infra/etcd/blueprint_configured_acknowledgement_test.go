package etcd

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

// Rationale: BP-05/H33: Apply can execute Components without publishing native
// Releases. Its successful acknowledgement must retain the exact applied artifact
// for the next Apply, unlike a metadata-only edit or an unsuccessful Task.
func TestConfiguredBlueprintAcknowledgementRetainsAppliedArtifact(t *testing.T) {
	for _, test := range []struct {
		name      string
		blueprint bool
		status    TaskStatus
		applies   bool
	}{
		{"completed apply", true, TaskStatusCompleted, true},
		{"failed apply", true, TaskStatusFailed, false},
		{"metadata edit", false, TaskStatusCompleted, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryHierarchyStore()
			hierarchy, err := newHierarchyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			project, environment := createEnvironmentBlueprintOwners(t, hierarchy)
			task := environmentBlueprintTestTask(t, project.Record, environment.Record, 350)
			task.RenderGeneration = 2
			if test.blueprint {
				task.Params[componentTaskBlueprintProcedureParam] = componentTaskBlueprintProcedureNone
			}
			baseline := environmentBlueprintTestProjection(environment.Record.ID, task, 1)
			baseline.RevisionID = ids.NewAt(ids.KindTask, task.CreatedAt, 370)
			value, err := encodeEnvironmentComposeProjection(baseline)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Transact(ctx, nil, []Mutation{{Type: MutationPut,
				Key: environmentComposeProjectionKey(environment.Record.ID), Value: value}}); err != nil {
				t.Fatal(err)
			}
			candidate := environmentBlueprintTestProjection(environment.Record.ID, task, 2)
			stageEnvironmentBlueprintForPublicationTest(t, hierarchy, 0,
				environmentBlueprintTestRevision(environment.Record.ID, task, "services: {}\n"),
				candidate, environmentBlueprintTestMarker(task, environment.Record.ID))
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			read, err := store.Get(ctx, environmentComposeProjectionKey(environment.Record.ID))
			if err != nil {
				t.Fatal(err)
			}
			change, err := tasks.prepareTaskMaterializationProjectionAcknowledgement(
				ctx,
				task,
				test.status,
				read.ReadRevision,
			)
			if err != nil {
				t.Fatal(err)
			}
			defer clearTaskMaterializationProjectionChange(change)
			if change.applies != test.applies {
				t.Fatalf("applied publication = %t; want %t", change.applies, test.applies)
			}
			if !test.applies {
				if len(change.mutations) != 0 {
					t.Fatal("non-applying Task changed applied authority")
				}
				return
			}
			if len(change.mutations) != 1 || len(change.conditions) != 2 {
				t.Fatal("Apply lost its sealed-root and applied-predecessor guards")
			}
			applied, err := decodeEnvironmentComposeProjection(change.mutations[0].Value)
			if err != nil || applied.RevisionID != task.ID || applied.RenderGeneration != 2 ||
				!bytes.Equal(applied.ComposeArtifact, candidate.ComposeArtifact) {
				t.Fatalf("Apply failed to retain its exact sealed artifact: %v", err)
			}
		})
	}
}
