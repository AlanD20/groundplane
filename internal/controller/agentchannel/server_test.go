package agentchannel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type fakeAuthenticator struct {
	authorization Authorization
	err           error
	calls         int
	seenID        string
	seenToken     Token
}

func (a *fakeAuthenticator) Authenticate(_ context.Context, id string, token Token) (Authorization, error) {
	a.calls++
	a.seenID = id
	a.seenToken = token
	return a.authorization, a.err
}

type scriptedStream struct {
	ctx      context.Context
	messages []*agentpb.AgentMessage
	recvErr  error
	sent     []*agentpb.ControllerMessage
}

func (s *scriptedStream) Send(message *agentpb.ControllerMessage) error {
	s.sent = append(s.sent, message)
	return nil
}

func (s *scriptedStream) Recv() (*agentpb.AgentMessage, error) {
	if len(s.messages) != 0 {
		message := s.messages[0]
		s.messages = s.messages[1:]
		return message, nil
	}
	if s.recvErr != nil {
		err := s.recvErr
		s.recvErr = nil
		return nil, err
	}
	return nil, io.EOF
}

func (s *scriptedStream) SetHeader(metadata.MD) error  { return nil }
func (s *scriptedStream) SendHeader(metadata.MD) error { return nil }
func (s *scriptedStream) SetTrailer(metadata.MD)       {}
func (s *scriptedStream) Context() context.Context {
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}
func (s *scriptedStream) SendMsg(any) error { return errors.New("unexpected SendMsg") }
func (s *scriptedStream) RecvMsg(any) error { return errors.New("unexpected RecvMsg") }

// Rationale: no unauthenticated payload may reach Agent session handling.
func TestConnectRequiresAuthenticateFirst(t *testing.T) {
	authenticator := &fakeAuthenticator{}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{readyMessage(1)}}

	err := New(authenticator, NewRegistry()).Connect(stream)
	if status.Code(err).String() != "Unauthenticated" {
		t.Fatalf("status = %v, want Unauthenticated", status.Code(err))
	}
	if authenticator.calls != 0 || len(stream.sent) != 0 {
		t.Fatalf("auth calls = %d, sent = %d", authenticator.calls, len(stream.sent))
	}
}

// Rationale: successful authentication without a runtime configuration is a
// Controller wiring failure and must fail closed before opening a session.
func TestConnectRejectsMissingAuthorizedConfig(t *testing.T) {
	authenticator := &fakeAuthenticator{authorization: Authorization{Generation: 1}}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage("agt_01J00000000000000000000000", testToken('m')),
	}}

	err := New(authenticator, NewRegistry()).Connect(stream)
	if status.Code(err) != codes.Internal {
		t.Fatalf("status = %v, want Internal", status.Code(err))
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent = %d messages, want zero", len(stream.sent))
	}
}

// Rationale: malformed identities, malformed credentials, and authenticator
// mismatch are indistinguishable at the transport boundary.
func TestConnectRejectsMalformedAndMismatchedAuthentication(t *testing.T) {
	tests := []struct {
		name    string
		id      string
		token   []byte
		authErr error
	}{
		{name: "malformed id", id: "agent-1", token: testToken(1)},
		{name: "short token", id: testAgentID, token: []byte("short")},
		{
			name:    "mismatch",
			id:      testAgentID,
			token:   testToken(2),
			authErr: errs.New(errs.KindAgentNotFound, "credential mismatch"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			authenticator := &fakeAuthenticator{err: tt.authErr}
			stream := &scriptedStream{messages: []*agentpb.AgentMessage{authenticateMessage(tt.id, tt.token)}}

			err := New(authenticator, NewRegistry()).Connect(stream)
			if status.Code(err).String() != "Unauthenticated" {
				t.Fatalf("status = %v, want Unauthenticated", status.Code(err))
			}
			if len(stream.sent) != 0 {
				t.Fatalf("sent %d messages before authentication", len(stream.sent))
			}
		})
	}
}

// Rationale: Authenticate is a one-message handshake and cannot be replayed
// within an authorized stream.
func TestConnectRejectsDuplicateAuthenticate(t *testing.T) {
	authenticator := authorizedAuthenticator()
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(3)),
		authenticateMessage(testAgentID, testToken(3)),
	}}

	err := New(authenticator, NewRegistry()).Connect(stream)
	if status.Code(err).String() != "Unauthenticated" {
		t.Fatalf("status = %v, want Unauthenticated", status.Code(err))
	}
	if len(stream.sent) != 1 {
		t.Fatalf("sent messages = %d, want initial config only", len(stream.sent))
	}
}

// Rationale: authorized configuration is never disclosed before successful
// credential verification, and it is the first Controller message afterward.
func TestConnectGatesInitialConfigOnAuthentication(t *testing.T) {
	failed := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(4)),
	}}
	failedAuth := authorizedAuthenticator()
	failedAuth.err = errs.New(errs.KindAgentNotFound, "mismatch")
	_ = New(failedAuth, NewRegistry()).Connect(failed)
	if len(failed.sent) != 0 {
		t.Fatalf("failed authentication sent %d messages", len(failed.sent))
	}

	success := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(4)),
	}}
	if err := New(authorizedAuthenticator(), NewRegistry()).Connect(success); err != nil {
		t.Fatalf("connect: %v", err)
	}
	if len(success.sent) != 1 || success.sent[0].GetConfigUpdate() == nil {
		t.Fatalf("first message = %+v, want ConfigUpdate", success.sent)
	}
	if success.sent[0].GetConfigUpdate().GetAgentConfig().GetMaxConcurrentTasks() != 4 {
		t.Fatalf("config = %+v", success.sent[0].GetConfigUpdate().GetAgentConfig())
	}
}

// Rationale: scheduling and stale-readiness decisions need the exact capacity
// and receipt time from the latest Ready message even after disconnect.
func TestConnectTracksReadyFreshnessAndCapacity(t *testing.T) {
	registry := NewRegistry()
	server := New(authorizedAuthenticator(), registry)
	wantTime := testTime()
	server.now = func() time.Time { return wantTime }
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, testToken(5)),
		readyMessage(9),
	}}

	if err := server.Connect(stream); err != nil {
		t.Fatalf("connect: %v", err)
	}
	snapshot, ok := registry.Snapshot(testAgentID)
	if !ok || snapshot.Online || snapshot.Capacity != 9 || !snapshot.LastReady.Equal(wantTime) {
		t.Fatalf("snapshot = %+v, found = %v", snapshot, ok)
	}
}

// Rationale: transport cancellation must always release the registry's online
// state so lifecycle removal can complete its bounded wait.
func TestConnectCancellationMarksSessionOffline(t *testing.T) {
	registry := NewRegistry()
	stream := &scriptedStream{
		messages: []*agentpb.AgentMessage{authenticateMessage(testAgentID, testToken(6))},
		recvErr:  context.Canceled,
	}

	if err := New(authorizedAuthenticator(), registry).Connect(stream); err != nil {
		t.Fatalf("connect cancellation: %v", err)
	}
	snapshot, ok := registry.Snapshot(testAgentID)
	if !ok || snapshot.Online {
		t.Fatalf("snapshot = %+v, found = %v", snapshot, ok)
	}
}

// Rationale: even a dependency error containing credential material must be
// replaced with a generic boundary error and must not reach a response.
func TestAuthenticationFailureDoesNotLeakToken(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	authenticator := &fakeAuthenticator{err: errors.New("rejected token " + string(secret))}
	stream := &scriptedStream{messages: []*agentpb.AgentMessage{
		authenticateMessage(testAgentID, append([]byte(nil), secret...)),
	}}

	err := New(authenticator, NewRegistry()).Connect(stream)
	if strings.Contains(err.Error(), string(secret)) {
		t.Fatalf("credential leaked in error: %v", err)
	}
	if len(stream.sent) != 0 {
		t.Fatalf("sent %d messages after authentication failure", len(stream.sent))
	}
	var want Token
	copy(want[:], secret)
	if !bytes.Equal(authenticator.seenToken[:], want[:]) {
		t.Fatal("authenticator did not receive the decoded token")
	}
}

func authorizedAuthenticator() *fakeAuthenticator {
	return &fakeAuthenticator{authorization: Authorization{
		Generation: 1,
		Config: &agentpb.AgentConfig{
			PullIntervalSeconds: 5,
			MaxConcurrentTasks:  4,
			Labels:              map[string]string{"role": "local"},
		},
	}}
}

func authenticateMessage(id string, token []byte) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{
		Authenticate: &agentpb.Authenticate{AgentId: id, Token: token},
	}}
}

func readyMessage(capacity int32) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Ready{
		Ready: &agentpb.Ready{Capacity: capacity},
	}}
}

func testToken(seed byte) []byte {
	return bytes.Repeat([]byte{seed}, tokenSize)
}

func testTime() time.Time {
	return time.Date(2026, time.August, 20, 10, 30, 0, 0, time.UTC)
}
