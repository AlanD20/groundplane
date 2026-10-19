package agentchannel

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type nativeDispatchPeer struct {
	attempt *agentpb.WorkAttemptIdentity
	prepare func()
	calls   int
}

func (*nativeDispatchPeer) AccountJournal(context.Context, agentregistration.WorkAuthorization,
	string, *agentpb.WorkAttemptIdentity,
) error {
	return nil
}

func (*nativeDispatchPeer) Ready(context.Context, agentregistration.WorkAuthorization,
	uint16, bool,
) ([]workdispatch.Issue, error) {
	return nil, nil
}

func (peer *nativeDispatchPeer) Dispatch(_ context.Context, authorization agentregistration.WorkAuthorization,
	capacity int32, delivered map[string]string, send func(*agentpb.ControllerMessage) (bool, error),
) ([]workdispatch.Issue, error) {
	peer.calls++
	identity := authorization.Identity()
	if identity.AgentID != peer.attempt.AgentId || identity.HostID != peer.attempt.HostId ||
		identity.Generation != peer.attempt.ExecutorGeneration || capacity != 1 {
		return nil, errs.New(errs.KindStateConflict, "test delivery received another executor or capacity")
	}
	if peer.prepare != nil {
		peer.prepare()
	}
	sent, err := send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkAssignment{
		ScriptWorkAssignment: &agentpb.ScriptWorkAssignment{Attempt: proto.CloneOf(peer.attempt)},
	}})
	if sent && err == nil {
		delivered[peer.attempt.WorkItemId] = peer.attempt.AttemptId
	}
	return nil, err
}

type nativeDispatchStream struct {
	grpc.ServerStream
	ctx      context.Context
	messages []*agentpb.ControllerMessage
	received []*agentpb.AgentMessage
	recvErr  error
	sendErr  func(*agentpb.ControllerMessage) error
}

func (stream *nativeDispatchStream) Context() context.Context { return stream.ctx }
func (stream *nativeDispatchStream) Recv() (*agentpb.AgentMessage, error) {
	if len(stream.received) == 0 {
		if stream.recvErr != nil {
			err := stream.recvErr
			stream.recvErr = nil
			return nil, err
		}
		return nil, io.EOF
	}
	message := stream.received[0]
	stream.received = stream.received[1:]
	return message, nil
}
func (stream *nativeDispatchStream) Send(message *agentpb.ControllerMessage) error {
	if stream.sendErr != nil {
		if err := stream.sendErr(message); err != nil {
			return err
		}
	}
	stream.messages = append(stream.messages, proto.CloneOf(message))
	return nil
}

func nativeDispatchFixture(t *testing.T) (*Server, *Session, *nativeDispatchPeer, *channelMemoryStore) {
	t.Helper()
	at := time.Now().UTC()
	agentID := ids.New(ids.KindAgent)
	store := newChannelMemoryStore()
	identities, err := agentregistration.NewRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(bytes.Repeat([]byte{1}, tokenSize))
	_, err = identities.CreateSingleton(t.Context(), localagents.LocalAgentRecord{
		ID: agentID, EnrollmentTaskID: ids.New(ids.KindTask), Generation: 1,
		Image:          "registry.example.test/agent@sha256:" + strings.Repeat("a", 64),
		Phase:          localagents.LocalAgentPhaseProvisioning,
		Config:         localagents.LocalAgentConfig{PullIntervalSeconds: 2, MaxConcurrentTasks: 1},
		EncryptedToken: []byte("supplied-ciphertext"), TokenDigest: base64.RawURLEncoding.EncodeToString(digest[:]),
		CreatedAt: at, TokenUpdatedAt: at,
	})
	if err != nil {
		t.Fatal(err)
	}
	process := [16]byte{1}
	identity, err := identities.RegisterWorkProcess(t.Context(), agentID, 1, process, 1,
		[]workexecution.Operation{workexecution.Script})
	if err != nil {
		t.Fatal(err)
	}
	registry := NewRegistry()
	session, err := registry.Open(t.Context(), agentID, 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Close)
	if err := session.RecordProcessAuthentication(&agentpb.Authenticate{AgentId: agentID,
		ExecutionPlanSchema: executionplan.SchemaVersion, ChannelSchema: agentprotocol.SchemaVersion,
		ProcessGeneration: process[:],
	}); err != nil {
		t.Fatal(err)
	}
	attempt, err := workexecution.EncodeAgentAttempt(workexecution.Attempt{
		ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask), WorkItemID: ids.New(ids.KindWorkItem),
		Number: 1, Executor: identity, Direction: workexecution.Forward,
		Input:      workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("b", 64)},
		AssignedAt: at, Deadline: at.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	peer := &nativeDispatchPeer{attempt: attempt}
	server := New(nil, registry, nil, nil, nil)
	server.workIdentities, server.workDispatch = identities, peer
	return server, session, peer, store
}

// TASK-107: pause or fence an authenticated native dispatch while its supplied
// operation owner prepares delivery. The real send gate must refuse it without
// marking delivery. A reversible pause must retain the exact attempt/deadline.
// The Script body and durable scheduler are not exercised by this transport test.
func TestNativeDispatchFencesPreparedDelivery(t *testing.T) {
	for _, mode := range []string{"pause", "stop", "revoke", "replace-session"} {
		t.Run(mode, func(t *testing.T) {
			server, session, peer, _ := nativeDispatchFixture(t)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			stream := &nativeDispatchStream{ctx: ctx}
			payloads := newAssignmentPayloadDelivery(ctx, 1)
			defer payloads.close()
			delivered := make(map[string]string)
			entered, release := make(chan struct{}), make(chan struct{})
			defer func() {
				select {
				case <-release:
				default:
					close(release)
				}
			}()
			peer.prepare = func() { close(entered); <-release }
			result := make(chan error, 1)
			go func() { result <- server.dispatchWork(stream, session, 1, 1, 1, true, delivered, payloads) }()
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("native delivery preparation did not start")
			}
			type pauseResult struct {
				resume func()
				err    error
			}
			paused := make(chan pauseResult, 1)
			switch mode {
			case "pause":
				go func() {
					resume, err := server.sessions.PauseAssignments(ctx, session.agentID, 1)
					paused <- pauseResult{resume: resume, err: err}
				}()
				ticker := time.NewTicker(time.Millisecond)
				defer ticker.Stop()
				for session.AssignmentsAllowed() {
					select {
					case <-ticker.C:
					case <-ctx.Done():
						t.Fatal("dispatch pause did not close admission")
					}
				}
				select {
				case <-paused:
					t.Fatal("pause returned before preparation drained")
				default:
				}
			case "stop":
				if err := server.sessions.StopAssignments(ctx, session.agentID, 1); err != nil {
					t.Fatal(err)
				}
			case "revoke":
				if err := server.sessions.Revoke(ctx, session.agentID, 1); err != nil {
					t.Fatal(err)
				}
			case "replace-session":
				replacement, err := server.sessions.Open(ctx, session.agentID, 1)
				if err != nil {
					t.Fatal(err)
				}
				defer replacement.Close()
			}
			close(release)
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-ctx.Done():
				t.Fatal("fenced dispatch did not return")
			}
			if len(stream.messages) != 0 || len(delivered) != 0 {
				t.Fatal("fenced preparation sent or recorded a delivery")
			}
			peer.prepare = nil
			if mode == "pause" {
				select {
				case state := <-paused:
					if state.err != nil {
						t.Fatal(state.err)
					}
					defer state.resume()
					state.resume()
				case <-ctx.Done():
					t.Fatal("pause did not finish after preparation drained")
				}
			}
			if err := server.dispatchWork(stream, session, 1, 1, 1, true, delivered, payloads); err != nil {
				t.Fatal(err)
			}
			if mode == "pause" {
				if len(stream.messages) != 1 ||
					!proto.Equal(stream.messages[0].GetScriptWorkAssignment().Attempt, peer.attempt) ||
					delivered[peer.attempt.WorkItemId] != peer.attempt.AttemptId {
					t.Fatal("resume did not deliver the exact captured attempt")
				}
			} else if len(stream.messages) != 0 || len(delivered) != 0 || peer.calls != 1 {
				t.Fatal("old session reopened execution admission")
			}
		})
	}
}
