package agent

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const clientTestAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

// QA: HOST-04/05; local Agent stream ordering and copied bytes, not Controller authentication or liveness expiry.
// Rationale: authentication must be the exact first frame, using a private
// token copy, before readiness or any work can flow.
func TestClientSendsAuthenticationFirstAndCopiesToken(t *testing.T) {
	t.Parallel()

	token := bytes.Repeat([]byte{0xa5}, agentprotocol.RawTokenBytes)
	wantToken := append([]byte(nil), token...)
	stream := newFakeStream(
		configMessage(2, 3),
		&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_Shutdown{Shutdown: &agentpb.Shutdown{}}},
	)
	client := newTestClient(t, token, stream)
	clear(token)
	if err := client.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	sent := stream.sentMessages()
	if len(sent) != 2 {
		t.Fatalf("sent message count = %d, want Authenticate and Ready", len(sent))
	}
	authenticate := sent[0].GetAuthenticate()
	if authenticate == nil || authenticate.AgentId != clientTestAgentID || !bytes.Equal(authenticate.Token, wantToken) {
		t.Fatal("first message is not the exact Authenticate payload")
	}
	if ready := sent[1].GetReady(); ready == nil || ready.Capacity != 3 || ready.Version != version.Value {
		t.Fatalf("second message Ready = %#v, want capacity 3 and build version", ready)
	}
}

// QA: TASK-07/10; hermetic Agent exchange only, not durable event/terminal publication or host effects.
// Rationale: terminal acknowledgement identity must survive the worker boundary
// exactly, while an unresolved procedure fails closed instead of being executed.
func TestClientSendsExactFailedTaskAcknowledgement(t *testing.T) {
	t.Parallel()
	assignment := workerAssignment(workerTestTaskID, "plan-a")
	planHash := testtaskassignment.PlanDigest(assignment.Plan)
	stream := newFakeStream(
		configMessage(60, 1),
		&agentpb.ControllerMessage{
			Payload: &agentpb.ControllerMessage_TaskAssignment{TaskAssignment: &agentpb.TaskAssignment{
				TaskId: workerTestTaskID, AssignmentId: assignment.AssignmentID,
				OperationId: assignment.OperationID,
				Plan:        assignment.Plan, ForwardDeadline: timestamppb.New(assignment.ForwardDeadline),
				RecoveryDeadline: timestamppb.New(
					assignment.RecoveryDeadline,
				), ExecutionDeadline: timestamppb.New(assignment.ForwardDeadline), ExecutionEpoch: 7,
				ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
			}},
		},
	)
	client := newTestClient(t, bytes.Repeat([]byte{0x30}, agentprotocol.RawTokenBytes), stream)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()
	select {
	case <-stream.taskAckSent:
	case <-time.After(time.Second):
		t.Fatal("client did not send TaskAck")
	}
	var acknowledgement *agentpb.TaskAck
	var events []*agentpb.TaskEvent
	for _, message := range stream.sentMessages() {
		if message.GetTaskEvent() != nil {
			events = append(events, message.GetTaskEvent())
		}
		if message.GetTaskAck() != nil {
			acknowledgement = message.GetTaskAck()
		}
	}
	if acknowledgement == nil || acknowledgement.TaskId != workerTestTaskID ||
		acknowledgement.AssignmentId != assignment.AssignmentID ||
		!bytes.Equal(acknowledgement.PlanHash, planHash[:]) ||
		acknowledgement.ExecutionEpoch != 7 ||
		acknowledgement.Terminal != agentpb.TaskTerminal_TASK_TERMINAL_FAILED ||
		acknowledgement.GetComposeResult() == nil {
		t.Fatalf("TaskAck = %#v", acknowledgement)
	}
	if len(events) != 2 || events[0].TaskId != workerTestTaskID ||
		events[0].AssignmentId != assignment.AssignmentID ||
		!bytes.Equal(events[0].PlanHash, planHash[:]) || events[0].StepId != workerTestStepID ||
		events[0].ExecutionEpoch != 7 || events[0].Ordinal != 1 ||
		events[0].State != agentpb.TaskState_TASK_STATE_RUNNING ||
		events[1].TaskId != workerTestTaskID || events[1].AssignmentId != assignment.AssignmentID ||
		!bytes.Equal(events[1].PlanHash, planHash[:]) || events[1].StepId != workerTestStepID ||
		events[1].ExecutionEpoch != 7 || events[1].Ordinal != 2 ||
		events[1].State != agentpb.TaskState_TASK_STATE_FAILED {
		t.Fatalf("TaskEvents = %#v", events)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

// QA: CMP-05, DNS-03; in-memory acknowledgement ownership only, not DNS traffic or durable compensation.
// Rationale: the client must clone and transmit the complete typed proof so
// later mutation of worker-owned memory cannot change the acknowledgement.
func TestClientSendsOwnedDNSResolverObservationEvidence(t *testing.T) {
	stream := newFakeStream()
	client := &Client{}
	evidence := &agentpb.DNSResolverObservationEvidence{
		ImageConfigDigest: bytes.Repeat([]byte{7}, 32),
		ComponentId:       "cmp_exact", RenderGeneration: 11,
		CatchAllQuery: &agentpb.DNSQueryProof{
			Name: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS, RecursionAvailable: true,
			SelectedUpstream: "1.1.1.1:53", Attempts: 1,
			Answers: []*agentpb.DNSAnswerRecord{{
				OwnerName: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
				NameServer: "a.root-servers.net.",
			}},
			Counters: []*agentpb.DNSForwardCounter{{
				Upstream: "1.1.1.1:53", Before: 1, After: 2,
			}},
		},
	}
	if err := dnsproof.Seal(evidence); err != nil {
		t.Fatal(err)
	}
	wantEvidence := proto.CloneOf(evidence)
	result := TaskResult{
		AssignmentID: workerTestAssignmentID, TaskID: workerTestTaskID,
		Terminal: TaskTerminalCompleted,
		Compose:  &agentpb.ComposeTaskResult{DnsResolverCandidateObservation: evidence},
	}
	if err := client.sendTaskAck(stream, result); err != nil {
		t.Fatalf("sendTaskAck() error = %v", err)
	}
	evidence.ComponentId = "changed"
	evidence.ImageConfigDigest[0] = 8
	evidence.CatchAllQuery.Answers[0].NameServer = "changed.invalid."
	evidence.CatchAllQuery.Counters[0].After = 99
	acks := stream.taskAcknowledgements()
	if len(acks) != 1 || !proto.Equal(
		acks[0].GetComposeResult().GetDnsResolverCandidateObservation(), wantEvidence,
	) {
		t.Fatalf("TaskAck evidence = %#v", acks)
	}
}

// QA: HOST-04; constructor validation only, not token generation, storage, revocation, or Controller acceptance.
// Rationale: malformed decoded credentials must fail at construction before dialing.
func TestNewClientRequiresExactlyThirtyTwoTokenBytes(t *testing.T) {
	t.Parallel()

	const expectedRawTokenBytes = 32
	if agentprotocol.RawTokenBytes != expectedRawTokenBytes {
		t.Fatalf("Agent protocol raw token bytes = %d, want %d", agentprotocol.RawTokenBytes, expectedRawTokenBytes)
	}
	for _, size := range []int{0, expectedRawTokenBytes - 1, expectedRawTokenBytes + 1} {
		_, err := NewClient(
			agentprotocol.SocketPath, clientTestAgentID, make([]byte, size),
			"/var/lib/groundplane/vol", testLogger(),
		)
		if err == nil {
			t.Fatalf("NewClient() with %d token bytes returned nil error", size)
		}
		var domainErr *errs.Error
		if !errors.As(err, &domainErr) || domainErr.Code != errs.CodeValidationFailed {
			t.Fatalf("NewClient() error = %v, want validation.failed", err)
		}
	}
}

// QA: HOST-04/07; local first-frame rejection only, not Controller dispatch fencing or saved config authority.
// Rationale: the Agent must fail closed when work arrives before authenticated configuration.
func TestClientRequiresConfigUpdateBeforeReadyOrWork(t *testing.T) {
	t.Parallel()

	stream := newFakeStream(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_TaskAssignment{
		TaskAssignment: &agentpb.TaskAssignment{TaskId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	}})
	client := newTestClient(t, bytes.Repeat([]byte{0x31}, agentprotocol.RawTokenBytes), stream)
	err := client.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want config-first failure")
	}
	if got := len(stream.sentMessages()); got != 1 {
		t.Fatalf("sent message count = %d, want only Authenticate", got)
	}
}

// QA: TASK-10; local Agent admission only, not durable claim validation or reconnect recovery.
// Rationale: zero is the protobuf default and must never become a durable
// event identity; only a positive Controller-authored epoch may enter a worker.
func TestClientRejectsZeroExecutionEpoch(t *testing.T) {
	t.Parallel()
	assignment := workerAssignment(workerTestTaskID, "zero-event-attempt")
	client := &Client{pool: NewWorkerPool(1, "/var/lib/groundplane/vol", nil, testLogger())}
	_, err := client.handleControllerMessage(context.Background(), &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_TaskAssignment{TaskAssignment: &agentpb.TaskAssignment{
			TaskId: assignment.TaskID, AssignmentId: assignment.AssignmentID,
			OperationId: assignment.OperationID, Plan: assignment.Plan,
			ForwardDeadline:   timestamppb.New(assignment.ForwardDeadline),
			RecoveryDeadline:  timestamppb.New(assignment.RecoveryDeadline),
			ExecutionDeadline: timestamppb.New(assignment.ForwardDeadline),
			ExecutionMode:     agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
		}},
	})
	if !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("zero event attempt error = %v, want internal", err)
	}
}

// QA: HOST-05, LOG-02; fake-stream process teardown only, not stale detection or live subscription cleanup.
// Rationale: process cancellation must close the authenticated stream without surfacing an error.
func TestClientCancellationClosesStreamGracefully(t *testing.T) {
	t.Parallel()

	stream := newFakeStream(configMessage(60, 1))
	client := newTestClient(t, bytes.Repeat([]byte{0x32}, agentprotocol.RawTokenBytes), stream)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- client.Run(ctx) }()
	<-stream.readySent
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run() error = %v, want graceful cancellation", err)
	}
	if !stream.wasClosed() || !stream.connection.wasClosed() {
		t.Fatal("Run() did not close stream and connection")
	}
	select {
	case <-client.workersDone:
	default:
		t.Fatal("Run() returned before the worker pool stopped")
	}
}

// QA: HOST-05; local goroutine cancellation only, not socket loss or authenticated reconnect.
// Rationale: a receive pump must exit rather than block on result delivery
// after the owning stream context is canceled.
func TestReceiveNextDoesNotBlockAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream := newFakeStream(&agentpb.ControllerMessage{})
	stream.ctx = ctx
	done := receiveNext(ctx, stream, make(chan receiveResult))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("receive pump remained blocked after cancellation")
	}
}

// QA: HOST-04; injected send-error sanitization only, not daemon logs or Controller-side credential handling.
// Rationale: transport implementations are untrusted error sources and must
// not be able to reflect the credential into Agent errors.
func TestClientDoesNotLeakTokenFromTransportError(t *testing.T) {
	t.Parallel()

	token := bytes.Repeat([]byte{'s'}, agentprotocol.RawTokenBytes)
	encoded := base64.RawURLEncoding.EncodeToString(token)
	stream := newFakeStream()
	stream.sendErr = errors.New("rejected token " + encoded)
	client := newTestClient(t, token, stream)
	err := client.Run(context.Background())
	if err == nil {
		t.Fatal("Run() error = nil, want send failure")
	}
	if bytes.Contains([]byte(err.Error()), []byte(encoded)) || bytes.Contains([]byte(err.Error()), token) {
		t.Fatalf("error leaked credential: %v", err)
	}
	var domainErr *errs.Error
	if !errors.As(err, &domainErr) || domainErr.Code != errs.CodeInternal {
		t.Fatalf("Run() error = %v, want internal", err)
	}
}

// QA: HOST-05, TASK-10; fake reconnect/redispatch only, not durable claim recovery or actual interrupted effects.
// Rationale: one Agent process must reauthenticate after a transient stream
// loss, fully stop the old pool first, and execute Controller redispatch only
// on the replacement pool without rereading or exposing its channel token.
func TestClientReconnectsSameInstanceAndExecutesRedispatchOnReplacementPool(t *testing.T) {
	for _, streamCode := range []codes.Code{codes.Unavailable, codes.Internal, codes.Unknown} {
		t.Run(streamCode.String(), func(t *testing.T) {
			token := bytes.Repeat([]byte{0x42}, agentprotocol.RawTokenBytes)
			first := newFakeStream(configMessage(60, 1))
			first.receiveErr = status.Error(streamCode, "test stream loss")
			assignment := workerAssignment(workerTestTaskID, "plan-reconnect")
			second := newFakeStream(
				configMessage(60, 1),
				&agentpb.ControllerMessage{
					Payload: &agentpb.ControllerMessage_TaskAssignment{TaskAssignment: &agentpb.TaskAssignment{
						TaskId: workerTestTaskID, AssignmentId: assignment.AssignmentID,
						OperationId: assignment.OperationID,
						Plan:        assignment.Plan, ForwardDeadline: timestamppb.New(assignment.ForwardDeadline),
						RecoveryDeadline: timestamppb.New(
							assignment.RecoveryDeadline,
						), ExecutionDeadline: timestamppb.New(assignment.ForwardDeadline), ExecutionEpoch: 1,
						ExecutionMode: agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_FORWARD,
					}},
				},
			)
			client := newTestClient(t, token, first)
			connections := 0
			client.connect = func(ctx context.Context, _ string) (agentStream, io.Closer, error) {
				connections++
				switch connections {
				case 1:
					first.ctx = ctx
					return first, first.connection, nil
				case 2:
					select {
					case <-client.workersDone:
					default:
						t.Fatal("replacement stream connected before the old worker pool stopped")
					}
					second.ctx = ctx
					return second, second.connection, nil
				default:
					t.Fatalf("connect call count = %d, want 2", connections)
					return nil, nil, errors.New("unexpected reconnect")
				}
			}
			var reconnectAttempts []uint
			client.reconnect = func(_ context.Context, attempt uint) error {
				reconnectAttempts = append(reconnectAttempts, attempt)
				return nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() { result <- client.Run(ctx) }()
			select {
			case <-second.taskAckSent:
			case <-time.After(time.Second):
				cancel()
				t.Fatal("redispatched task did not execute on the replacement pool")
			}
			cancel()
			if err := <-result; err != nil {
				t.Fatalf("Run() after reconnect cancellation = %v, want nil", err)
			}
			if connections != 2 || len(reconnectAttempts) != 1 || reconnectAttempts[0] != 0 {
				t.Fatalf("connections/attempts = %d/%v, want 2/[0]", connections, reconnectAttempts)
			}
			for index, stream := range []*fakeStream{first, second} {
				sent := stream.sentMessages()
				if len(sent) < 2 || sent[0].GetAuthenticate() == nil || sent[1].GetReady() == nil ||
					!bytes.Equal(sent[0].GetAuthenticate().GetToken(), token) {
					t.Fatalf("stream %d did not authenticate and become Ready with the process token", index)
				}
			}
			secondAcknowledgements := second.taskAcknowledgements()
			if len(first.taskAcknowledgements()) != 0 || len(secondAcknowledgements) != 1 ||
				secondAcknowledgements[0].GetTerminal() != agentpb.TaskTerminal_TASK_TERMINAL_FAILED {
				t.Fatalf(
					"TaskAck before/after reconnect = %d/%#v, want no original and one terminal failed replacement ack",
					len(first.taskAcknowledgements()),
					secondAcknowledgements,
				)
			}
		})
	}
}

// QA: HOST-04/05; injected gRPC status handling only, not live token revocation or replacement generation.
// Rationale: rejected authentication is permanent for the current runtime
// material and must terminate without a retry loop or reflected server detail.
func TestClientDoesNotReconnectPermanentAuthenticationFailure(t *testing.T) {
	for _, streamCode := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		t.Run(streamCode.String(), func(t *testing.T) {
			token := bytes.Repeat([]byte{'s'}, agentprotocol.RawTokenBytes)
			stream := newFakeStream()
			stream.receiveErr = status.Error(streamCode, "revoked secret detail")
			client := newTestClient(t, token, stream)
			reconnectCalled := false
			client.reconnect = func(context.Context, uint) error {
				reconnectCalled = true
				return nil
			}
			err := client.Run(context.Background())
			if err == nil || reconnectCalled {
				t.Fatalf("Run()/reconnect = %v/%t, want terminal error without reconnect", err, reconnectCalled)
			}
			if strings.Contains(err.Error(), "revoked secret detail") || bytes.Contains([]byte(err.Error()), token) {
				t.Fatalf("authentication failure leaked private detail: %v", err)
			}
		})
	}
}

// QA: HOST-05; injected transport-error classification only, not live socket failure or stale-state projection.
// Rationale: a plain receive error has no authenticated gRPC status and must
// terminate without retrying or reflecting private transport diagnostics.
func TestClientAuthenticatedStreamPlainErrorIsTerminal(t *testing.T) {
	privateCause := errors.New("private receive transport detail")
	stream := newFakeStream(configMessage(60, 1))
	stream.receiveErr = privateCause
	client := newTestClient(t, bytes.Repeat([]byte{0x43}, agentprotocol.RawTokenBytes), stream)
	reconnectCalled := false
	client.reconnect = func(context.Context, uint) error {
		reconnectCalled = true
		return errors.New("unexpected reconnect")
	}

	err := client.Run(context.Background())
	if err == nil || reconnectCalled {
		t.Fatalf("Run()/reconnect = %v/%t, want terminal error without reconnect", err, reconnectCalled)
	}
	if strings.Contains(err.Error(), privateCause.Error()) {
		t.Fatalf("authenticated stream failure leaked private detail: %v", err)
	}
}

// QA: HOST-05; pure backoff bounds only, not elapsed reconnect timing or restored authenticated Ready.
// Rationale: reconnect delay must grow exponentially without exceeding its
// ceiling, while equal jitter prevents synchronized reconnect storms.
func TestAgentChannelReconnectDelayIsBoundedAndJittered(t *testing.T) {
	for _, test := range []struct {
		name    string
		attempt uint
		minimum time.Duration
		maximum time.Duration
	}{
		{name: "initial", attempt: 0, minimum: 50 * time.Millisecond, maximum: 100 * time.Millisecond},
		{name: "second", attempt: 1, minimum: 100 * time.Millisecond, maximum: 200 * time.Millisecond},
		{name: "capped", attempt: 63, minimum: 2500 * time.Millisecond, maximum: 5 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			window := test.maximum - test.minimum
			if got := agentChannelReconnectDelay(test.attempt, 0); got != test.minimum {
				t.Fatalf("minimum delay = %s, want %s", got, test.minimum)
			}
			if got := agentChannelReconnectDelay(test.attempt, uint64(window)); got != test.maximum {
				t.Fatalf("maximum delay = %s, want %s", got, test.maximum)
			}
		})
	}
}

// QA: HOST-05; pre-cancelled wait only, not cancellation during a live randomized delay.
// Rationale: process cancellation must interrupt a pending reconnect delay so
// shutdown never waits for the backoff ceiling.
func TestAgentChannelReconnectWaitHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForAgentChannelReconnect(ctx, 63); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForAgentChannelReconnect() error = %v, want context cancellation", err)
	}
}

// QA: TASK-07; local Unix-socket gRPC receive boundary, not Controller enqueue fairness or durable event bounds.
// Rationale: the Agent must receive the complete 5 MiB Controller envelope
// while rejecting the first byte beyond that transport boundary.
func TestConnectGRPCEnforcesExactReceiveMessageLimit(t *testing.T) {
	if agentChannelAgentMaximumReceiveMessageBytes != 5*1024*1024 {
		t.Fatalf("Agent receive limit = %d, want 5 MiB", agentChannelAgentMaximumReceiveMessageBytes)
	}

	for _, test := range []struct {
		name     string
		size     int
		wantCode codes.Code
	}{
		{name: "boundary", size: 5 * 1024 * 1024, wantCode: codes.OK},
		{name: "one_byte_over", size: 5*1024*1024 + 1, wantCode: codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &agentChannelClientTransportServer{response: sizedControllerMessage(t, test.size)}
			socketPath := startAgentChannelClientTransportServer(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			stream, connection, err := connectGRPC(ctx, socketPath)
			if err != nil {
				t.Fatalf("connectGRPC() error = %v", err)
			}
			defer connection.Close()
			if err := stream.Send(&agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Ready{
				Ready: &agentpb.Ready{Capacity: 1, Version: "test"},
			}}); err != nil {
				t.Fatalf("send request: %v", err)
			}
			message, err := stream.Recv()
			if status.Code(err) != test.wantCode {
				t.Fatalf("receive %d-byte Controller message = %v, want %s", test.size, err, test.wantCode)
			}
			if err == nil && proto.Size(message) != test.size {
				t.Fatalf("Controller message size = %d, want %d", proto.Size(message), test.size)
			}
		})
	}
}

// QA: TASK-07; local Unix-socket gRPC send boundary, not Controller validation or durable storage bounds.
// Rationale: the Agent must send the complete 16 MiB envelope while rejecting
// the first byte beyond that transport boundary.
func TestConnectGRPCEnforcesExactSendMessageLimit(t *testing.T) {
	if agentChannelAgentMaximumSendMessageBytes != 16*1024*1024 {
		t.Fatalf("Agent send limit = %d, want 16 MiB", agentChannelAgentMaximumSendMessageBytes)
	}

	for _, test := range []struct {
		name     string
		size     int
		wantCode codes.Code
	}{
		{name: "boundary", size: 16 * 1024 * 1024, wantCode: codes.OK},
		{name: "one_byte_over", size: 16*1024*1024 + 1, wantCode: codes.ResourceExhausted},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := &agentChannelClientTransportServer{received: make(chan int, 1)}
			socketPath := startAgentChannelClientTransportServer(t, server)
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			stream, connection, err := connectGRPC(ctx, socketPath)
			if err != nil {
				t.Fatalf("connectGRPC() error = %v", err)
			}
			defer connection.Close()
			sendErr := stream.Send(sizedAgentMessage(t, test.size))
			if status.Code(sendErr) != test.wantCode {
				t.Fatalf("send %d-byte Agent message = %v, want %s", test.size, sendErr, test.wantCode)
			}
			if sendErr == nil {
				select {
				case size := <-server.received:
					if size != test.size {
						t.Fatalf("server received %d-byte Agent message, want %d", size, test.size)
					}
				case <-ctx.Done():
					t.Fatal("timed out waiting for exact-boundary Agent message")
				}
			}
		})
	}
}

type agentChannelClientTransportServer struct {
	agentpb.UnimplementedAgentChannelServer
	response *agentpb.ControllerMessage
	received chan int
}

func (server *agentChannelClientTransportServer) Connect(stream agentpb.AgentChannel_ConnectServer) error {
	message, err := stream.Recv()
	if err != nil {
		return err
	}
	if server.received != nil {
		server.received <- proto.Size(message)
	}
	if server.response != nil {
		return stream.Send(server.response)
	}
	return nil
}

func startAgentChannelClientTransportServer(
	t *testing.T,
	server agentpb.AgentChannelServer,
) string {
	t.Helper()
	socketPath := shortUnixSocketPath(t)
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on Agent channel socket: %v", err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(16*1024*1024+1),
		grpc.MaxSendMsgSize(5*1024*1024+1),
	)
	agentpb.RegisterAgentChannelServer(grpcServer, server)
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		if err := <-serveDone; err != nil {
			t.Errorf("serve Agent channel: %v", err)
		}
	})
	return socketPath
}

func shortUnixSocketPath(t *testing.T) string {
	t.Helper()
	// Keep the test socket inside the worktree so repository-managed temporary
	// state never spills into the system temporary directory. The short prefix
	// leaves room for the worktree path under Unix's socket path limit.
	directory, err := os.MkdirTemp("../../.tmp", "u-")
	if err != nil {
		t.Fatalf("create short Unix socket directory: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(directory); err != nil {
			t.Errorf("remove short Unix socket directory: %v", err)
		}
	})
	return filepath.Join(directory, "agent.sock")
}

func sizedControllerMessage(t *testing.T, target int) *agentpb.ControllerMessage {
	t.Helper()
	payload := strings.Repeat("x", target)
	message := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ConfigUpdate{
		ConfigUpdate: &agentpb.ConfigUpdate{AgentConfig: &agentpb.AgentConfig{
			PullIntervalSeconds: 1,
			MaxConcurrentTasks:  1,
			Labels:              map[string]string{"payload": payload},
		}},
	}}
	for proto.Size(message) > target {
		payload = payload[:len(payload)-(proto.Size(message)-target)]
		message.GetConfigUpdate().GetAgentConfig().Labels["payload"] = payload
	}
	if proto.Size(message) != target {
		t.Fatalf("cannot construct %d-byte Controller message; got %d", target, proto.Size(message))
	}
	return message
}

func sizedAgentMessage(t *testing.T, target int) *agentpb.AgentMessage {
	t.Helper()
	payload := make([]byte, target)
	message := &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_TaskEvent{
		TaskEvent: &agentpb.TaskEvent{Chunk: payload},
	}}
	for proto.Size(message) > target {
		payload = payload[:len(payload)-(proto.Size(message)-target)]
		message.GetTaskEvent().Chunk = payload
	}
	if proto.Size(message) != target {
		t.Fatalf("cannot construct %d-byte Agent message; got %d", target, proto.Size(message))
	}
	return message
}

func newTestClient(t *testing.T, token []byte, stream *fakeStream) *Client {
	t.Helper()
	client, err := NewClient(
		agentprotocol.SocketPath, clientTestAgentID, token, "/var/lib/groundplane/vol", testLogger(),
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}
	client.connect = func(ctx context.Context, _ string) (agentStream, io.Closer, error) {
		stream.ctx = ctx
		return stream, stream.connection, nil
	}
	return client
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func configMessage(pullInterval, capacity int32) *agentpb.ControllerMessage {
	return &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ConfigUpdate{ConfigUpdate: &agentpb.ConfigUpdate{
			AgentConfig: &agentpb.AgentConfig{PullIntervalSeconds: pullInterval, MaxConcurrentTasks: capacity},
		}},
	}
}

type fakeStream struct {
	mu          sync.Mutex
	ctx         context.Context
	receive     []*agentpb.ControllerMessage
	receiveErr  error
	sent        []*agentpb.AgentMessage
	sendErr     error
	closed      bool
	readySent   chan struct{}
	readyOnce   sync.Once
	taskAckSent chan struct{}
	taskAckOnce sync.Once
	connection  *fakeCloser
}

func newFakeStream(messages ...*agentpb.ControllerMessage) *fakeStream {
	return &fakeStream{
		receive: messages, readySent: make(chan struct{}), taskAckSent: make(chan struct{}), connection: &fakeCloser{},
	}
}

func (s *fakeStream) Send(message *agentpb.AgentMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sendErr != nil {
		return s.sendErr
	}
	copyMessage := &agentpb.AgentMessage{}
	if authenticate := message.GetAuthenticate(); authenticate != nil {
		copyMessage.Payload = &agentpb.AgentMessage_Authenticate{Authenticate: &agentpb.Authenticate{
			AgentId: authenticate.AgentId,
			Token:   append([]byte(nil), authenticate.Token...),
		}}
	}
	if ready := message.GetReady(); ready != nil {
		copyMessage.Payload = &agentpb.AgentMessage_Ready{Ready: &agentpb.Ready{
			Capacity: ready.Capacity,
			Version:  ready.Version,
		}}
		s.readyOnce.Do(func() { close(s.readySent) })
	}
	if acknowledgement := message.GetTaskAck(); acknowledgement != nil {
		owned := &agentpb.TaskAck{
			TaskId: acknowledgement.TaskId, AssignmentId: acknowledgement.AssignmentId,
			PlanHash: append([]byte(nil), acknowledgement.PlanHash...),
			Terminal: acknowledgement.Terminal, ExitCode: acknowledgement.ExitCode,
			ExecutionEpoch:              acknowledgement.ExecutionEpoch,
			ReleaseRecoveryRecordSha256: append([]byte(nil), acknowledgement.ReleaseRecoveryRecordSha256...),
		}
		if acknowledgement.GetComposeResult() != nil {
			owned.Result = &agentpb.TaskAck_ComposeResult{ComposeResult: acknowledgement.GetComposeResult()}
		}
		if acknowledgement.GetEnvironmentDirectoryResult() != nil {
			owned.Result = &agentpb.TaskAck_EnvironmentDirectoryResult{
				EnvironmentDirectoryResult: acknowledgement.GetEnvironmentDirectoryResult(),
			}
		}
		copyMessage.Payload = &agentpb.AgentMessage_TaskAck{TaskAck: owned}
		s.taskAckOnce.Do(func() { close(s.taskAckSent) })
	}
	if event := message.GetTaskEvent(); event != nil {
		copyMessage.Payload = &agentpb.AgentMessage_TaskEvent{TaskEvent: &agentpb.TaskEvent{
			TaskId: event.TaskId, AssignmentId: event.AssignmentId,
			PlanHash: append([]byte(nil), event.PlanHash...),
			StepId:   event.StepId, ExecutionEpoch: event.ExecutionEpoch, Ordinal: event.Ordinal,
			State: event.State, Chunk: append([]byte(nil), event.Chunk...),
		}}
	}
	s.sent = append(s.sent, copyMessage)
	return nil
}

func (s *fakeStream) Recv() (*agentpb.ControllerMessage, error) {
	s.mu.Lock()
	if len(s.receive) > 0 {
		message := s.receive[0]
		s.receive = s.receive[1:]
		s.mu.Unlock()
		return message, nil
	}
	if s.receiveErr != nil {
		err := s.receiveErr
		s.receiveErr = nil
		s.mu.Unlock()
		return nil, err
	}
	ctx := s.ctx
	s.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *fakeStream) CloseSend() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeStream) sentMessages() []*agentpb.AgentMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*agentpb.AgentMessage(nil), s.sent...)
}

func (s *fakeStream) wasClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *fakeStream) taskAcknowledgements() []*agentpb.TaskAck {
	s.mu.Lock()
	defer s.mu.Unlock()
	var acknowledgements []*agentpb.TaskAck
	for _, message := range s.sent {
		if acknowledgement := message.GetTaskAck(); acknowledgement != nil {
			acknowledgements = append(acknowledgements, acknowledgement)
		}
	}
	return acknowledgements
}

type fakeCloser struct {
	mu     sync.Mutex
	closed bool
}

func (c *fakeCloser) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	return nil
}

func (c *fakeCloser) wasClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}
