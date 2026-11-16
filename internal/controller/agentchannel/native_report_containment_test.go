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

type nativeContendedReportOwner struct {
	ScriptWorkCompleter
	blockedTask              string
	failures, unrelatedCalls int
	reports                  []*agentpb.ScriptWorkCompletionRequest
}

func (owner *nativeContendedReportOwner) CompleteScript(_ context.Context,
	_ agentregistration.WorkAuthorization, request *agentpb.ScriptWorkCompletionRequest,
) (*agentpb.ScriptWorkCompletionAck, error) {
	if request.Attempt.TaskId == owner.blockedTask {
		owner.reports = append(owner.reports, proto.CloneOf(request))
		if len(owner.reports) <= owner.failures {
			return nil, errs.New(errs.KindExecutionWriteContention, "publication snapshot changed")
		}
	} else {
		owner.unrelatedCalls++
	}
	return &agentpb.ScriptWorkCompletionAck{Attempt: proto.CloneOf(request.Attempt),
		CheckpointSequence: request.CheckpointSequence, CheckpointEvidence: proto.CloneOf(request.CheckpointEvidence),
		Outcome:    string(workexecution.Succeeded),
		FinishedAt: timestamppb.New(request.Attempt.Deadline.AsTime().Add(-time.Second))}, nil
}

// A concurrent publication must retry the identical terminal report, not refuse
// it or repeat execution. Live delivery and crash replay share the same finite
// budget; exhaustion must still permit an unrelated report on the channel.
// Storage contention is injected at the owner, not claimed as physical host QA.
func TestNativeTerminalPublicationContentionRetriesLiveAndReplay(t *testing.T) {
	for _, delivery := range []string{"live", "replay"} {
		for _, failures := range []int{2, 100} {
			name := "transient"
			if failures == 100 {
				name = "exhausted"
			}
			t.Run(delivery+"/"+name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				server, peer, authenticate := nativeConnectFixture(t)
				owner := &nativeContendedReportOwner{blockedTask: peer.attempt.TaskId, failures: failures}
				server.scriptCompletion = owner
				request := &agentpb.ScriptWorkCompletionRequest{Attempt: proto.CloneOf(peer.attempt),
					CheckpointSequence: 2, CheckpointEvidence: proto.CloneOf(peer.attempt.Input)}
				blocked := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{
					ScriptWorkCompletionRequest: request}}
				unrelated := proto.CloneOf(request)
				unrelated.Attempt.TaskId, unrelated.Attempt.WorkItemId, unrelated.Attempt.AttemptId =
					ids.New(ids.KindTask), ids.New(ids.KindWorkItem), ids.New(ids.KindAttempt)
				incoming := []*agentpb.AgentMessage{authenticate}
				if delivery == "replay" {
					raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(blocked)
					if err != nil {
						t.Fatal(err)
					}
					incoming = append(
						incoming,
						&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_WorkJournalReplayRequest{
							WorkJournalReplayRequest: &agentpb.WorkJournalReplayRequest{
								Report: raw,
							}}},
						nativeConnectReplayComplete(),
					)
				} else {
					incoming = append(incoming, nativeConnectReplayComplete(), blocked)
				}
				incoming = append(
					incoming,
					&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_ScriptWorkCompletionRequest{
						ScriptWorkCompletionRequest: unrelated}},
				)
				stream := &nativeDispatchStream{ctx: ctx, received: incoming}
				if err := server.Connect(stream); err != nil {
					t.Fatal(err)
				}
				wantCalls := 3
				if failures == 100 {
					wantCalls = 8
				}
				if len(owner.reports) != wantCalls || owner.unrelatedCalls != 1 {
					t.Fatalf("publication calls=%d unrelated=%d", len(owner.reports), owner.unrelatedCalls)
				}
				for _, actual := range owner.reports {
					if !proto.Equal(actual, request) {
						t.Fatal("retry changed captured terminal evidence")
					}
				}
				var acknowledgement *agentpb.ControllerMessage
				refused := false
				for _, message := range stream.messages {
					if workreceipt.Match(blocked, message) == nil {
						acknowledgement = message
					}
					if workreceipt.MatchRejection(blocked, message) == nil {
						refused = true
					}
					if replay := message.GetWorkJournalReplayAck(); replay != nil && len(replay.ReportSha256) != 0 {
						refused = replay.FailureCode != ""
						if len(replay.Acknowledgement) != 0 {
							acknowledgement = new(agentpb.ControllerMessage)
							if err := proto.Unmarshal(replay.Acknowledgement, acknowledgement); err != nil {
								t.Fatal(err)
							}
						}
					}
				}
				if failures == 100 {
					if !refused || acknowledgement != nil {
						t.Fatal("exhausted publication was acknowledged or lost its refusal")
					}
				} else if refused || workreceipt.Match(blocked, acknowledgement) != nil {
					t.Fatal("transient contention lost the exact terminal acknowledgement")
				}
			})
		}
	}
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
