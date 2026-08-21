package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const clientTestAgentID = "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"

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
	if ready := sent[1].GetReady(); ready == nil || ready.Capacity != 3 {
		t.Fatalf("second message Ready = %#v, want capacity 3", ready)
	}
}

// Rationale: terminal acknowledgement identity must survive the worker boundary
// exactly, while an unresolved procedure fails closed instead of being executed.
func TestClientSendsExactFailedTaskAcknowledgement(t *testing.T) {
	t.Parallel()
	planHash := sha256.Sum256([]byte("immutable-plan"))
	stream := newFakeStream(
		configMessage(60, 1),
		&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_TaskAssignment{TaskAssignment: &agentpb.TaskAssignment{
			TaskId: workerTestTaskID, PlanHash: planHash[:], TimeoutSeconds: 60,
			Steps: []*agentpb.Step{{StepId: workerTestStepID, Op: "ack"}},
		}}},
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
		!bytes.Equal(acknowledgement.PlanHash, planHash[:]) ||
		acknowledgement.Terminal != agentpb.TaskTerminal_TASK_TERMINAL_FAILED {
		t.Fatalf("TaskAck = %#v", acknowledgement)
	}
	if len(events) != 2 || events[0].Attempt != 1 || events[0].Ordinal != 1 ||
		events[0].State != agentpb.TaskState_TASK_STATE_RUNNING ||
		events[1].Attempt != 1 || events[1].Ordinal != 2 ||
		events[1].State != agentpb.TaskState_TASK_STATE_FAILED {
		t.Fatalf("TaskEvents = %#v", events)
	}
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run() error = %v", err)
	}
}

// Rationale: malformed decoded credentials must fail at construction before dialing.
func TestNewClientRequiresExactlyThirtyTwoTokenBytes(t *testing.T) {
	t.Parallel()

	for _, size := range []int{0, agentprotocol.RawTokenBytes - 1, agentprotocol.RawTokenBytes + 1} {
		_, err := NewClient(agentprotocol.SocketPath, clientTestAgentID, make([]byte, size), testLogger())
		if err == nil {
			t.Fatalf("NewClient() with %d token bytes returned nil error", size)
		}
		var domainErr *errs.Error
		if !errors.As(err, &domainErr) || domainErr.Code != errs.CodeValidationFailed {
			t.Fatalf("NewClient() error = %v, want validation.failed", err)
		}
	}
}

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

// Rationale: a receive pump must exit rather than block on result delivery
// after the owning stream context is canceled.
func TestReceiveNextDoesNotBlockAfterCancellation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	stream := newFakeStream(&agentpb.ControllerMessage{})
	stream.ctx = ctx
	done := receiveNext(ctx, stream, make(chan receiveResult))
	<-done
}

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

func newTestClient(t *testing.T, token []byte, stream *fakeStream) *Client {
	t.Helper()
	client, err := NewClient(agentprotocol.SocketPath, clientTestAgentID, token, testLogger())
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
		copyMessage.Payload = &agentpb.AgentMessage_Ready{Ready: &agentpb.Ready{Capacity: ready.Capacity}}
		s.readyOnce.Do(func() { close(s.readySent) })
	}
	if acknowledgement := message.GetTaskAck(); acknowledgement != nil {
		copyMessage.Payload = &agentpb.AgentMessage_TaskAck{TaskAck: &agentpb.TaskAck{
			TaskId: acknowledgement.TaskId, PlanHash: append([]byte(nil), acknowledgement.PlanHash...),
			Terminal: acknowledgement.Terminal, ExitCode: acknowledgement.ExitCode,
			Result: append([]byte(nil), acknowledgement.Result...),
		}}
		s.taskAckOnce.Do(func() { close(s.taskAckSent) })
	}
	if event := message.GetTaskEvent(); event != nil {
		copyMessage.Payload = &agentpb.AgentMessage_TaskEvent{TaskEvent: &agentpb.TaskEvent{
			TaskId: event.TaskId, PlanHash: append([]byte(nil), event.PlanHash...),
			StepId: event.StepId, Attempt: event.Attempt, Ordinal: event.Ordinal,
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
