package agentchannel

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TASK-110: malformed credentials/protocol and rejected authentication cannot
// disclose configuration or dispatch work. Broken authorized configuration is
// a Controller failure, not a credential error. Auth responses are supplied.
func TestNativeConnectAuthenticationFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name          string
		code          codes.Code
		calls, frames int
	}{
		{"missing-authenticate", codes.Unauthenticated, 0, 0},
		{"malformed-id", codes.Unauthenticated, 0, 0},
		{"short-token", codes.Unauthenticated, 0, 0},
		{"missing-process", codes.Unauthenticated, 0, 0},
		{"old-protocol", codes.Unauthenticated, 0, 0},
		{"rejected-credential", codes.Unauthenticated, 1, 0},
		{"missing-config", codes.Internal, 1, 0},
		{"zero-generation", codes.Internal, 1, 0},
		{"invalid-config", codes.Internal, 1, 0},
		{"duplicate-authenticate", codes.Unauthenticated, 1, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			server, peer, request := nativeConnectFixture(t)
			auth := &fakeAuthenticator{authorization: Authorization{Generation: 1,
				Config: &agentpb.AgentConfig{PullIntervalSeconds: 2, MaxConcurrentTasks: 1}}}
			server.auth = auth
			messages := []*agentpb.AgentMessage{request}
			switch test.name {
			case "missing-authenticate":
				messages[0] = nativeConnectReady(1)
			case "malformed-id":
				request.GetAuthenticate().AgentId = "not-an-agent-id"
			case "short-token":
				request.GetAuthenticate().Token = []byte("short")
			case "missing-process":
				request.GetAuthenticate().ProcessGeneration = nil
			case "old-protocol":
				request.GetAuthenticate().ChannelSchema--
			case "rejected-credential":
				auth.err = errors.New("rejected private-credential-test-value")
			case "missing-config":
				auth.authorization.Config = nil
			case "zero-generation":
				auth.authorization.Generation = 0
			case "invalid-config":
				auth.authorization.Config.MaxConcurrentTasks = 0
			case "duplicate-authenticate":
				messages = append(messages, proto.CloneOf(request))
			}
			stream := &nativeDispatchStream{ctx: t.Context(), received: messages}
			err := server.Connect(stream)
			if status.Code(err) != test.code || auth.calls != test.calls ||
				len(stream.messages) != test.frames || peer.calls != 0 {
				t.Fatalf("authentication boundary: status=%v calls=%d frames=%d dispatch=%d",
					status.Code(err), auth.calls, len(stream.messages), peer.calls)
			}
			if strings.Contains(err.Error(), "private-credential-test-value") {
				t.Fatal("authentication provider text leaked into the transport error")
			}
		})
	}
}

// TASK-110: a generation revoked during slow authentication cannot be opened
// afterward, even when the authentication provider eventually returns success.
func TestNativeConnectDoesNotResurrectRevokedAuthentication(t *testing.T) {
	server, peer, request := nativeConnectFixture(t)
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	auth := &pausedAuthenticator{authorization: Authorization{Generation: 1,
		Config: &agentpb.AgentConfig{PullIntervalSeconds: 2, MaxConcurrentTasks: 1}},
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	server.auth = auth
	stream := &nativeDispatchStream{ctx: ctx, received: []*agentpb.AgentMessage{request}}
	result := make(chan error, 1)
	go func() { result <- server.Connect(stream) }()
	defer func() {
		select {
		case <-auth.release:
		default:
			close(auth.release)
		}
	}()
	select {
	case <-auth.entered:
	case <-ctx.Done():
		t.Fatal("authentication did not start")
	}
	if err := server.sessions.Revoke(ctx, request.GetAuthenticate().AgentId, 1); err != nil {
		t.Fatal(err)
	}
	close(auth.release)
	select {
	case err := <-result:
		if status.Code(err) != codes.FailedPrecondition || len(stream.messages) != 0 || peer.calls != 0 {
			t.Fatalf("revoked authentication reopened execution: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("revoked authentication did not finish")
	}
}
