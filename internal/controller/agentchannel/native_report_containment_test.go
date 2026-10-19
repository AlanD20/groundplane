package agentchannel

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type nativeConflictingReportOwner struct {
	ScriptWorkCompleter
	blockedTask                  string
	blockedCalls, unrelatedCalls int
}

func (owner *nativeConflictingReportOwner) CompleteScript(_ context.Context,
	_ agentregistration.WorkAuthorization, request *agentpb.ScriptWorkCompletionRequest,
) (*agentpb.ScriptWorkCompletionAck, error) {
	if request.Attempt.TaskId == owner.blockedTask {
		owner.blockedCalls++
		return nil, errs.New(errs.KindStateConflict, "captured operation state changed")
	}
	owner.unrelatedCalls++
	return &agentpb.ScriptWorkCompletionAck{Attempt: proto.CloneOf(request.Attempt),
		CheckpointSequence: request.CheckpointSequence, CheckpointEvidence: proto.CloneOf(request.CheckpointEvidence),
		Outcome: string(
			workexecution.Succeeded,
		), FinishedAt: timestamppb.New(request.Attempt.Deadline.AsTime().Add(-time.Second))}, nil
}

// TASK-115: a domain conflict settling one correctly framed report must retain
// that operation's unresolved result, not terminate the whole channel before
// an unrelated terminal report. Faults are injected at the operation owner;
// no storage settlement or Script execution is claimed by this transport case.
func TestNativeReportConflictDoesNotInterruptUnrelatedSettlement(t *testing.T) {
	server, peer, authenticate := nativeConnectFixture(t)
	owner := &nativeConflictingReportOwner{blockedTask: peer.attempt.TaskId}
	server.scriptCompletion = owner
	blocked := &agentpb.ScriptWorkCompletionRequest{Attempt: proto.CloneOf(peer.attempt),
		CheckpointSequence: 2, CheckpointEvidence: proto.CloneOf(peer.attempt.Input)}
	unrelated := proto.CloneOf(blocked)
	unrelated.Attempt.TaskId = ids.New(ids.KindTask)
	unrelated.Attempt.WorkItemId = ids.New(ids.KindWorkItem)
	unrelated.Attempt.AttemptId = ids.New(ids.KindAttempt)
	stream := &nativeDispatchStream{ctx: t.Context(), received: []*agentpb.AgentMessage{
		authenticate, nativeConnectReplayComplete(),
		{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{ScriptWorkCompletionRequest: blocked}},
		{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{ScriptWorkCompletionRequest: unrelated}},
	}}
	err := server.Connect(stream)
	if err != nil || owner.blockedCalls != 1 || owner.unrelatedCalls != 1 {
		t.Fatalf("one report conflict interrupted unrelated settlement: err=%v blocked=%d unrelated=%d",
			err, owner.blockedCalls, owner.unrelatedCalls)
	}
	var rejected, accepted *agentpb.ControllerMessage
	for _, message := range stream.messages {
		if message.GetWorkReportRejection() != nil {
			rejected = message
		}
		if message.GetScriptWorkCompletionAck() != nil {
			accepted = message
		}
	}
	blockedMessage := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{
		ScriptWorkCompletionRequest: blocked}}
	if workreceipt.MatchRejection(blockedMessage, rejected) != nil ||
		rejected.GetWorkReportRejection().FailureCode != string(errs.CodeStateConflict) ||
		workreceipt.Match(blockedMessage, rejected) == nil {
		t.Fatal("refused result did not receive an exact, non-settling rejection")
	}
	if err := workreceipt.Match(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{
		ScriptWorkCompletionRequest: unrelated}}, accepted); err != nil {
		t.Fatalf("unrelated result lost its own acknowledgement: %v", err)
	}
}

// Terminal-report containment is not permission to accept a forged host/process
// identity or malformed frame. Those are rejected before the operation owner.
func TestNativeTerminalReportRefusesForeignAndMalformedIdentity(t *testing.T) {
	for _, change := range []string{"host", "agent", "generation", "input"} {
		t.Run(change, func(t *testing.T) {
			server, peer, authenticate := nativeConnectFixture(t)
			owner := &nativeConflictingReportOwner{blockedTask: peer.attempt.TaskId}
			server.scriptCompletion = owner
			attempt := proto.CloneOf(peer.attempt)
			switch change {
			case "host":
				attempt.HostId = ids.New(ids.KindHost)
			case "agent":
				attempt.AgentId = ids.New(ids.KindAgent)
			case "generation":
				attempt.ExecutorGeneration++
			case "input":
				attempt.Input = nil
			}
			stream := &nativeDispatchStream{ctx: t.Context(), received: []*agentpb.AgentMessage{
				authenticate, nativeConnectReplayComplete(),
				{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{
					ScriptWorkCompletionRequest: &agentpb.ScriptWorkCompletionRequest{Attempt: attempt}}},
			}}
			if err := server.Connect(stream); err == nil || owner.blockedCalls != 0 || owner.unrelatedCalls != 0 {
				t.Fatalf("invalid report reached settlement: err=%v owner=%+v", err, owner)
			}
			for _, message := range stream.messages {
				if message.GetWorkReportRejection() != nil || message.GetScriptWorkCompletionAck() != nil {
					t.Fatal("invalid authority received an operation result response")
				}
			}
		})
	}
}
