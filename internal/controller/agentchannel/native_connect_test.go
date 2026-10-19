package agentchannel

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type nativeConnectStarter struct{}

func (nativeConnectStarter) Operations() []workexecution.Operation {
	return []workexecution.Operation{workexecution.Script}
}
func (nativeConnectStarter) Start(context.Context, agentregistration.WorkAuthorization,
	*agentpb.WorkStartRequest,
) (*agentpb.WorkStartAck, error) {
	return nil, errs.New(errs.KindInternal, "test did not authorize a Start")
}

func nativeConnectFixture(t *testing.T) (*Server, *nativeDispatchPeer, *agentpb.AgentMessage) {
	t.Helper()
	server, session, peer, _ := nativeDispatchFixture(t)
	agentID := session.agentID
	session.Close()
	configuration := &agentpb.AgentConfig{PullIntervalSeconds: 2, MaxConcurrentTasks: 1}
	server.auth = &fakeAuthenticator{authorization: Authorization{Generation: 1, Config: configuration}}
	server.workStarts = nativeConnectStarter{}
	return server, peer, &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Authenticate{
		Authenticate: &agentpb.Authenticate{AgentId: agentID, Token: bytes.Repeat([]byte{1}, tokenSize),
			ExecutionPlanSchema: executionplan.SchemaVersion, ChannelSchema: agentprotocol.SchemaVersion,
			ProcessGeneration: []byte{1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}},
	}}
}

func nativeConnectReady(capacity int32) *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_Ready{Ready: &agentpb.Ready{
		Capacity: capacity, Version: "qa", OperatingSystem: "linux", Architecture: "amd64",
		StagingReady: proto.Bool(false),
	}}}
}

func nativeConnectReplayComplete() *agentpb.AgentMessage {
	return &agentpb.AgentMessage{Payload: &agentpb.AgentMessage_WorkJournalReplayRequest{
		WorkJournalReplayRequest: &agentpb.WorkJournalReplayRequest{Complete: true},
	}}
}

// TASK-109: a fresh native stream cannot advertise execution capacity before
// its retained-result journal is accounted for. Completion enables ordinary
// non-staging work even while Backup staging is unavailable.
func TestNativeConnectRequiresJournalSettlementBeforeReady(t *testing.T) {
	for _, complete := range []bool{false, true} {
		name := "missing-replay"
		if complete {
			name = "replay-complete"
		}
		t.Run(name, func(t *testing.T) {
			server, peer, authenticate := nativeConnectFixture(t)
			readyAt := time.Date(2026, 10, 19, 12, 0, 0, 0, time.UTC)
			server.now = func() time.Time { return readyAt }
			stream := &nativeDispatchStream{ctx: t.Context(), received: []*agentpb.AgentMessage{authenticate}}
			if complete {
				stream.received = append(stream.received, nativeConnectReplayComplete())
			}
			stream.received = append(stream.received, nativeConnectReady(1))
			err := server.Connect(stream)
			if !complete {
				if status.Code(err) != codes.FailedPrecondition || peer.calls != 0 {
					t.Fatalf("unreconciled journal admitted dispatch: %v", err)
				}
			} else if err != nil || peer.calls != 1 {
				t.Fatalf("reconciled journal did not admit non-staging work: %v", err)
			}
			if len(stream.messages) < 2 {
				t.Fatal("Connect did not send registration")
			}
			registration := stream.messages[1].GetWorkExecutorRegistration()
			if stream.messages[0].GetConfigUpdate() == nil || registration == nil ||
				registration.HostId != peer.attempt.HostId || registration.ExecutorGeneration != peer.attempt.ExecutorGeneration {
				t.Fatal("Connect did not bind the registered host executor")
			}
			if complete {
				if len(stream.messages) != 4 || !stream.messages[2].GetWorkJournalReplayAck().GetComplete() ||
					!proto.Equal(stream.messages[3].GetScriptWorkAssignment().Attempt, peer.attempt) {
					t.Fatal("startup did not precede delivery of the exact attempt")
				}
				snapshot, found := server.sessions.Snapshot(authenticate.GetAuthenticate().AgentId)
				if !found || snapshot.Online || snapshot.Version != "qa" || snapshot.Capacity != 0 ||
					!snapshot.LastReady.Equal(readyAt) {
					t.Fatal("Ready receipt or free-capacity observation was lost on disconnect")
				}
			}
			if !bytes.Equal(authenticate.GetAuthenticate().Token, make([]byte, tokenSize)) {
				t.Fatal("Connect retained the authentication token bytes")
			}
		})
	}
}

// TASK-109: transport cancellation closes the native session even before Ready;
// it cannot leave a ghost online executor in the registry.
func TestNativeConnectCancellationMarksSessionOffline(t *testing.T) {
	server, peer, authenticate := nativeConnectFixture(t)
	stream := &nativeDispatchStream{ctx: t.Context(), received: []*agentpb.AgentMessage{authenticate},
		recvErr: context.Canceled}
	if err := server.Connect(stream); err != nil {
		t.Fatal(err)
	}
	snapshot, found := server.sessions.Snapshot(authenticate.GetAuthenticate().AgentId)
	if !found || snapshot.Online || peer.calls != 0 {
		t.Fatal("cancelled stream retained online state or dispatched before Ready")
	}
}
