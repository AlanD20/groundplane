package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/agent/containerterminal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type quiescenceOwnership bool

func (ownership quiescenceOwnership) Quiescent() bool { return bool(ownership) }

type quiescenceChecker struct{ err error }

func (checker quiescenceChecker) RequireQuiescent(context.Context) error { return checker.err }

type quiescenceStream struct{ sent []*agentpb.AgentMessage }

func (stream *quiescenceStream) Send(message *agentpb.AgentMessage) error {
	stream.sent = append(stream.sent, message)
	return nil
}
func (*quiescenceStream) Recv() (*agentpb.ControllerMessage, error) { return nil, errors.New("unused") }
func (*quiescenceStream) CloseSend() error                          { return nil }

type quiescenceFixture struct {
	workers       *executionWorkers
	images        *imageSession
	observations  *observationSession
	backingAccess *backingAccessSession
	dnsChecks     *dnsCheckSession
	edgeChecks    *edgeCheckSession
	terminals     *containerterminal.Session
}

func newQuiescenceFixture(t *testing.T) quiescenceFixture {
	t.Helper()
	terminals := containerterminal.New(t.Context(), nil)
	t.Cleanup(terminals.Close)
	return quiescenceFixture{
		workers: &executionWorkers{
			ctx: t.Context(), limit: 1, active: make(map[string]activeExecution),
		},
		images:        newImageSession(t.Context(), nil),
		observations:  newObservationSession(nil),
		backingAccess: newBackingAccessSession(nil),
		dnsChecks:     newDNSCheckSession(nil),
		edgeChecks:    newEdgeCheckSession(nil),
		terminals:     terminals,
	}
}

func (fixture quiescenceFixture) check(
	ctx context.Context,
	ownership executionOwnershipQuiescence,
	physical ExecutionQuiescenceChecker,
) bool {
	return localExecutionQuiescent(
		ctx,
		fixture.workers,
		ownership,
		fixture.images,
		fixture.observations,
		fixture.backingAccess,
		fixture.dnsChecks,
		fixture.edgeChecks,
		fixture.terminals,
		physical,
	)
}

func TestLocalExecutionQuiescenceRequiresEveryIndependentOwner(t *testing.T) {
	fixture := newQuiescenceFixture(t)
	if !fixture.check(t.Context(), quiescenceOwnership(true), quiescenceChecker{}) {
		t.Fatal("empty workers, idle ownership and physical inspector did not prove quiescence")
	}
	if fixture.check(t.Context(), quiescenceOwnership(false), quiescenceChecker{}) {
		t.Fatal("zero workers hid active process ownership")
	}
	fixture.observations.requestID = strings.Repeat("a", 32)
	if fixture.check(t.Context(), quiescenceOwnership(true), quiescenceChecker{}) {
		t.Fatal("active auxiliary observation was reported quiescent")
	}
	fixture.observations.requestID = ""
	if fixture.check(t.Context(), quiescenceOwnership(true), quiescenceChecker{err: errors.New("inspection failed")}) {
		t.Fatal("physical inspection failure was reported quiescent")
	}
}

func TestExecutionQuiescenceProbeRepliesWithBoundRequestAndFailureClosed(t *testing.T) {
	for _, test := range []struct {
		name       string
		checkerErr error
		want       bool
	}{
		{name: "quiescent", want: true},
		{name: "probe error", checkerErr: errors.New("Docker inventory unavailable")},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newQuiescenceFixture(t)
			client := &Client{executionQuiescence: quiescenceChecker{err: test.checkerErr}}
			requestID := strings.Repeat("a", 32)
			message := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CheckExecutionQuiescence{
				CheckExecutionQuiescence: &agentpb.CheckExecutionQuiescence{RequestId: requestID},
			}}
			stream := new(quiescenceStream)
			handled, err := client.handleExecutionQuiescence(
				t.Context(),
				stream,
				message,
				fixture.workers,
				quiescenceOwnership(true),
				fixture.images,
				fixture.observations,
				fixture.backingAccess,
				fixture.dnsChecks,
				fixture.edgeChecks,
				fixture.terminals,
			)
			if err != nil || !handled || len(stream.sent) != 1 {
				t.Fatalf("probe handled=%v sent=%d error=%v", handled, len(stream.sent), err)
			}
			result := stream.sent[0].GetExecutionQuiescenceResult()
			if result == nil || result.RequestId != requestID || result.Quiescent != test.want {
				t.Fatalf("quiescence result = %#v, want request=%s quiescent=%v", result, requestID, test.want)
			}
		})
	}
	t.Run("legacy operation id", func(t *testing.T) {
		fixture := newQuiescenceFixture(t)
		client := &Client{executionQuiescence: quiescenceChecker{}}
		message := &agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_CheckExecutionQuiescence{
			CheckExecutionQuiescence: &agentpb.CheckExecutionQuiescence{RequestId: "op_" + strings.Repeat("A", 26)},
		}}
		stream := new(quiescenceStream)
		handled, err := client.handleExecutionQuiescence(
			t.Context(),
			stream,
			message,
			fixture.workers,
			quiescenceOwnership(true),
			fixture.images,
			fixture.observations,
			fixture.backingAccess,
			fixture.dnsChecks,
			fixture.edgeChecks,
			fixture.terminals,
		)
		if !handled || err == nil || len(stream.sent) != 0 {
			t.Fatalf("legacy request handled=%v sent=%d error=%v", handled, len(stream.sent), err)
		}
	})
}
