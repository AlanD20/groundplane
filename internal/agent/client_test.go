package agent

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// HOST-04: reject malformed decoded credentials before dialing. Authentication
// ordering and private-copy ownership are exercised by the native Client gate.
func TestNewClientRequiresExactlyThirtyTwoTokenBytes(t *testing.T) {
	for _, size := range []int{0, 31, 33} {
		_, err := NewClient(agentprotocol.SocketPath, "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			make([]byte, size), "/var/lib/groundplane/vol", testLogger(), clientUnusedLogs{})
		if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
			t.Fatalf("NewClient with %d decoded bytes: %v", size, err)
		}
	}
}

// HOST-04/05: only authenticated transient status permits reconnect. Private
// provider text must not escape either authentication-send or receive failure.
func TestAgentTransportFailuresDoNotRetryPermanentErrorsOrExposeCauses(t *testing.T) {
	const private = "private credential or transport diagnostic"
	for _, test := range []struct {
		name          string
		cause         error
		authenticated bool
		retry         bool
	}{
		{"send plain", errors.New(private), false, false},
		{"receive plain", errors.New(private), true, false},
		{"send internal", status.Error(codes.Internal, private), false, false},
		{"receive internal", status.Error(codes.Internal, private), true, true},
		{"unauthenticated", status.Error(codes.Unauthenticated, private), true, false},
		{"permission denied", status.Error(codes.PermissionDenied, private), true, false},
		{"unavailable", status.Error(codes.Unavailable, private), false, true},
		{"end of stream", io.EOF, true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			retry, err := agentChannelTransportResult(
				t.Context(),
				test.cause,
				"agent: transport failed",
				test.authenticated,
			)
			if retry != test.retry || (err == nil) != test.retry {
				t.Fatalf("retry=%t, err=%v", retry, err)
			}
			if err != nil &&
				(!errors.Is(err, errs.New(errs.KindInternal, "")) || strings.Contains(err.Error(), private)) {
				t.Fatalf("unsafe transport failure: %v", err)
			}
		})
	}
}

// HOST-05: cancellation must release a receive pump whose result consumer has
// gone away, rather than leaving its goroutine blocked on an unbuffered send.
func TestReceiveNextDoesNotBlockAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	done := receiveNext(ctx, clientReceiveOnly{}, make(chan receiveResult))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("receive pump remained blocked after cancellation")
	}
}

type clientReceiveOnly struct{ agentStream }

func (clientReceiveOnly) Recv() (*agentpb.ControllerMessage, error) { return nil, io.EOF }

type clientUnusedLogs struct{ agentprotocol.LogReader }

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
