package agent

import (
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestValidateAndCopyScriptArtifactsRejectsDuplicateReleaseBody(t *testing.T) {
	// Rationale: a resumed release must never substitute one hook body for another
	// when its multi-Script artifact set is reconstructed from durable inputs.
	bodyA, bodyB := []byte("first hook"), []byte("second hook")
	digestA, digestB := sha256.Sum256(bodyA), sha256.Sum256(bodyB)
	metadataA := &agentpb.ScriptBodyArtifactMetadata{
		ScriptExecutionId: "execution-a", Size: uint32(len(bodyA)), Sha256: digestA[:],
	}
	metadataB := &agentpb.ScriptBodyArtifactMetadata{
		ScriptExecutionId: "execution-b", Size: uint32(len(bodyB)), Sha256: digestB[:],
	}
	plan := &agentpb.ExecutionPlan{
		ScriptBodyArtifacts:   []*agentpb.ScriptBodyArtifactMetadata{metadataA, metadataB},
		ScriptRunnerSnapshots: []*agentpb.ResolvedRunnerSnapshot{{}, {}},
	}
	artifacts := &agentpb.ScriptAssignmentArtifacts{Bodies: []*agentpb.ScriptBodyArtifact{
		{Metadata: metadataA, Body: bodyA}, {Metadata: metadataB, Body: bodyB},
	}}
	if _, err := validateAndCopyScriptArtifacts(plan, artifacts); err != nil {
		t.Fatalf("distinct body control rejected: %v", err)
	}
	artifacts.Bodies[1] = artifacts.Bodies[0]
	_, err := validateAndCopyScriptArtifacts(plan, artifacts)
	if err == nil {
		t.Fatal("validateAndCopyScriptArtifacts() error = nil, want duplicate body rejection")
	}
}
