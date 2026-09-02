package agentchannel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestSendTaskAssignmentStreamsExactMaterializationRecords(t *testing.T) {
	// Rationale: assignment metadata must arrive before separately owned bytes,
	// and the transient channel must preserve bounded one-based record ordering.
	content := bytes.Repeat([]byte("a"), entrymaterialization.MaximumChunkBytes+17)
	task, plan := controllerMaterializationTask(t, content)
	source := newOwnedControllerSource(content)
	resolver := &fakeMaterializationResolver{source: source}
	stream := &scriptedStream{}
	server := NewWithMaterializations(
		authorizedAuthenticator(), NewRegistry(), nil, &fakePlanResolver{plan: plan}, resolver,
	)
	claim := controllerMaterializationClaim(task)
	if err := server.sendTaskAssignment(stream, claim); err != nil {
		t.Fatalf("sendTaskAssignment() error = %v", err)
	}
	if len(stream.sent) != 5 || stream.sent[0].GetTaskAssignment() == nil ||
		stream.sent[1].GetMaterializationTransfer().GetHeader() == nil ||
		stream.sent[2].GetMaterializationTransfer().GetChunk().GetSequence() != 1 ||
		stream.sent[3].GetMaterializationTransfer().GetChunk().GetSequence() != 2 ||
		stream.sent[4].GetMaterializationTransfer().GetEnd().GetChunkCount() != 2 {
		t.Fatalf("Controller materialization messages = %#v", stream.sent)
	}
	transfer := stream.sent[1].GetMaterializationTransfer()
	materialization := plan.Steps[0].GetMaterializeFile()
	if transfer.GetTaskId() != task.ID ||
		transfer.GetAssignmentId() != claim.Assignment.Record.AssignmentID ||
		transfer.GetStepId() != task.Steps[0].ID ||
		!bytes.Equal(transfer.GetPlanHash(), plan.PlanHash) ||
		transfer.GetHeader().GetMaterializationId() != materialization.GetMaterializationId() ||
		resolver.task.ID != task.ID || resolver.step.GetStepId() != task.Steps[0].ID {
		t.Fatalf("transfer/resolver correlation = %#v / %#v", transfer, resolver)
	}
	if source.content != nil || !source.closed {
		t.Fatal("materialization resolver source retained plaintext after send")
	}
}

func TestSendTaskAssignmentRejectsSourceDigestMismatchWithoutEnd(t *testing.T) {
	// Rationale: a stale or corrupt resolver result must close ownership and end
	// the stream without an End record that could authorize Agent publication.
	want := []byte("expected")
	task, plan := controllerMaterializationTask(t, want)
	source := newOwnedControllerSource([]byte("corrupt!"))
	stream := &scriptedStream{}
	server := NewWithMaterializations(
		authorizedAuthenticator(), NewRegistry(), nil, &fakePlanResolver{plan: plan},
		&fakeMaterializationResolver{source: source},
	)
	if err := server.sendTaskAssignment(stream, controllerMaterializationClaim(task)); err == nil {
		t.Fatal("sendTaskAssignment() error = nil, want digest mismatch")
	}
	for _, message := range stream.sent {
		if message.GetMaterializationTransfer().GetEnd() != nil {
			t.Fatal("digest mismatch emitted a terminal materialization record")
		}
	}
	if source.content != nil || !source.closed {
		t.Fatal("digest mismatch retained resolver plaintext")
	}
}

type fakeMaterializationResolver struct {
	source io.ReadCloser
	task   etcd.TaskRecord
	step   *agentpb.ExecutionStep
}

func (resolver *fakeMaterializationResolver) ResolveMaterialization(
	_ context.Context,
	task etcd.TaskRecord,
	_ *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) (io.ReadCloser, error) {
	resolver.task = task
	resolver.step = step
	return resolver.source, nil
}

type ownedControllerSource struct {
	content []byte
	reader  *bytes.Reader
	closed  bool
}

func newOwnedControllerSource(content []byte) *ownedControllerSource {
	owned := append([]byte(nil), content...)
	return &ownedControllerSource{content: owned, reader: bytes.NewReader(owned)}
}

func (source *ownedControllerSource) Read(destination []byte) (int, error) {
	return source.reader.Read(destination)
}

func (source *ownedControllerSource) Close() error {
	clear(source.content)
	source.content = nil
	source.reader = bytes.NewReader(nil)
	source.closed = true
	return nil
}

func controllerMaterializationTask(
	t *testing.T,
	content []byte,
) (etcd.TaskRecord, *agentpb.ExecutionPlan) {
	t.Helper()
	const (
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		operationID   = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		planID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		stepID        = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		materializeID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	yaml := []byte("services: {}\n")
	yamlDigest := sha256.Sum256(yaml)
	contentDigest := sha256.Sum256(content)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: planID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_RECONCILE, TargetId: environmentID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId: environmentID, ProjectName: "gp-" + strings.ToLower(environmentID),
			CanonicalYaml: yaml, YamlSha256: yamlDigest[:],
			AuthorizedVolumeDir: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
				"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID,
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: stepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_MaterializeFile{MaterializeFile: &agentpb.MaterializeFile{
				ArtifactId: artifactID, MaterializationId: materializeID, EnvironmentId: environmentID,
				Destination: "blueprints/" + planID + "/blueprint.yaml",
				OutputKind:  agentpb.MaterializationOutputKind_MATERIALIZATION_OUTPUT_KIND_PLAIN_FILE,
				Mode:        0o444, Length: uint64(len(content)), Sha256: contentDigest[:],
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	now := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	task := etcd.TaskRecord{
		ID: taskID, OperationID: operationID, PlanID: planID, PlanHash: hex.EncodeToString(plan.PlanHash),
		RenderGeneration: 7, Type: etcd.TaskUpdate, Target: environmentID,
		Steps: []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: stepID}}, TimeoutSeconds: 60,
		Status: etcd.TaskStatusRunning, NextEventSequence: 1, CreatedAt: now,
	}
	return task, plan
}

func controllerMaterializationClaim(task etcd.TaskRecord) etcd.TaskAssignment {
	return etcd.TaskAssignment{
		Assignment: etcd.Versioned[etcd.TaskAssignmentRecord]{Record: etcd.TaskAssignmentRecord{
			AssignmentID: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			TaskID:       task.ID, Executor: etcd.TaskExecutorAgent,
		}},
		Task: etcd.Versioned[etcd.TaskRecord]{Record: task},
	}
}
