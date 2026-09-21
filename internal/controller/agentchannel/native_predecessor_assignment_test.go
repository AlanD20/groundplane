package agentchannel

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// QA: SVC-09/12, TASK-10; wire encoding and byte ownership, not runtime restoration or digest derivation.
// Rationale: the Controller wire encoder must preserve the durable authority
// digest and clone each native C/inactive artifact byte slice. It must not
// rederive a second digest format or alias the durable record.
func TestCandidateReleaseAssignmentAuthorityCarriesNativeWitnessAndDigest(t *testing.T) {
	const (
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		operationID   = "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		planID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environment   = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		candidateID   = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		publication   = "pub_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		durableDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	)
	planHash := bytes.Repeat([]byte{0x21}, 32)
	current := []byte{0x0a, 0x01, 0xc1}
	retained := []byte{0x0a, 0x01, 0xb1}
	authority := &testtaskassignments.ReleaseRestorationAuthority{
		Schema: 1, TaskID: taskID, OperationID: operationID,
		PlanHash: hex.EncodeToString(planHash), EnvironmentID: environment,
		CandidateArtifactID: artifactID,
		Candidates: []testtaskassignments.ReleaseRestorationCandidate{{
			ServiceID: serviceID, ReleaseID: candidateID, Target: testtaskassignments.ReleaseRestorationServingPredecessor,
		}},
		NativePredecessors: []testtaskassignments.ReleaseNativePredecessorAuthority{{
			ServiceID: serviceID, CurrentArtifact: current, RetainedPriorArtifact: retained,
		}},
	}
	claim := etcd.TaskAssignment{
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: etcd.TaskRecord{
			ID: taskID, OperationID: operationID, PlanHash: hex.EncodeToString(planHash),
			Params: map[string]string{testreleaserender.TaskReleasePublicationParam: publication},
		}},
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				ExecutionMode:        testtaskassignments.TaskExecutionModeForward,
				RestorationAuthority: authority, RestorationAuthoritySHA256: durableDigest,
			},
		},
	}
	plan := &agentpb.ExecutionPlan{
		PlanHash: planHash,
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{
			ServiceId: serviceID, CandidateReleaseId: candidateID, CandidateArtifactId: artifactID,
		}}},
	}
	encoded, err := candidateReleaseAssignmentAuthority(claim, plan)
	if err != nil {
		t.Fatalf("candidateReleaseAssignmentAuthority() error = %v", err)
	}
	if !bytes.Equal(encoded.restoration.AuthoritySha256, mustDecodeDigest(t, durableDigest)) {
		t.Fatal("wire authority omitted the durable authority digest")
	}
	if encoded.mode != agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD || len(encoded.recoveryDigest) != 0 ||
		encoded.restoration.GetTaskId() != taskID || encoded.restoration.GetOperationId() != operationID ||
		!bytes.Equal(encoded.restoration.GetPlanHash(), bytes.Repeat([]byte{0x21}, 32)) ||
		encoded.restoration.GetEnvironmentId() != environment || encoded.restoration.GetCandidateArtifactId() != artifactID {
		t.Fatalf("wire restoration identity = %v", encoded.restoration)
	}
	if len(encoded.restoration.Candidates) != 1 ||
		encoded.restoration.Candidates[0].GetServiceId() != serviceID ||
		encoded.restoration.Candidates[0].GetReleaseId() != candidateID ||
		encoded.restoration.Candidates[0].GetTarget() !=
			agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR {
		t.Fatalf("wire restoration candidates = %v", encoded.restoration.Candidates)
	}
	if len(encoded.restoration.NativePredecessors) != 1 ||
		encoded.restoration.NativePredecessors[0].GetServiceId() != serviceID ||
		!bytes.Equal(encoded.restoration.NativePredecessors[0].GetCurrentArtifact(), current) ||
		!bytes.Equal(encoded.restoration.NativePredecessors[0].GetRetainedPriorArtifact(), retained) {
		t.Fatalf("wire native authority = %#v", encoded.restoration.NativePredecessors)
	}
	current[0] ^= 0xff
	retained[0] ^= 0xff
	if !bytes.Equal(encoded.restoration.NativePredecessors[0].CurrentArtifact, []byte{0x0a, 0x01, 0xc1}) ||
		!bytes.Equal(encoded.restoration.NativePredecessors[0].RetainedPriorArtifact, []byte{0x0a, 0x01, 0xb1}) {
		t.Fatal("wire native authority aliases durable bytes")
	}
}

func mustDecodeDigest(t *testing.T, value string) []byte {
	t.Helper()
	digest, err := hex.DecodeString(value)
	if err != nil {
		t.Fatal(err)
	}
	return digest
}
