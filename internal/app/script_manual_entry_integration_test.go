package app

import (
	"bytes"
	"context"
	"testing"

	testtaskmaterializationowner "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// checkManualJourneyValueSubstitution injects corruption only after real source
// resolution. Actual plan admission and source-root checks remain in place.
func checkManualJourneyValueSubstitution(
	t *testing.T,
	scripts *etcd.ScriptRepository,
	materializer *testtaskmaterialization.TaskMaterializationResolver,
	task etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
) {
	t.Helper()
	repository := &retainingManualJourneyBodies{ScriptRepository: scripts}
	values := &substitutingManualJourneyValue{TaskMaterializationResolver: materializer}
	service, err := testtaskplanning.NewScriptArtifactService(repository, values)
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := service.ResolveScriptAssignmentArtifacts(context.Background(), task, plan)
	if err == nil || artifacts != nil {
		t.Fatal("assignment accepted Entry bytes outside the sealed digest")
	}
	if len(repository.body) == 0 || len(values.value) == 0 {
		t.Fatal("substitution test did not reach private body and Entry resolution")
	}
	for _, value := range [][]byte{repository.body, values.value} {
		if !bytes.Equal(value, make([]byte, len(value))) {
			t.Fatal("rejected assignment retained private plaintext")
		}
	}
}

type retainingManualJourneyBodies struct {
	*etcd.ScriptRepository
	body []byte
}

func (repository *retainingManualJourneyBodies) ResolveScriptAssignmentArtifacts(ctx context.Context,
	task etcd.TaskRecord, plan *agentpb.ExecutionPlan,
) (*agentpb.ScriptAssignmentArtifacts, error) {
	artifacts, err := repository.ScriptRepository.ResolveScriptAssignmentArtifacts(ctx, task, plan)
	if err == nil && artifacts != nil && len(artifacts.Bodies) == 1 {
		repository.body = artifacts.Bodies[0].Body
	}
	return artifacts, err
}

type substitutingManualJourneyValue struct {
	*testtaskmaterialization.TaskMaterializationResolver
	value []byte
}

func (resolver *substitutingManualJourneyValue) ResolveTaskMaterializationSource(ctx context.Context,
	environmentID string, source testtaskmaterializationowner.Source,
) ([]byte, error) {
	value, err := resolver.TaskMaterializationResolver.ResolveTaskMaterializationSource(ctx, environmentID, source)
	if err == nil && len(value) > 0 {
		value[0] ^= 1
		resolver.value = value
	}
	return value, err
}
