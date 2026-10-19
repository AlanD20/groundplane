//go:build agenthostacceptance

package agent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/AlanD20/groundplane/internal/agent/attachruntime"
	"github.com/AlanD20/groundplane/internal/agent/backingruntime"
	"github.com/AlanD20/groundplane/internal/agent/backupruntime"
	"github.com/AlanD20/groundplane/internal/agent/componentruntime"
	"github.com/AlanD20/groundplane/internal/agent/entryruntime"
	"github.com/AlanD20/groundplane/internal/agent/environmentruntime"
	"github.com/AlanD20/groundplane/internal/agent/hierarchyruntime"
	"github.com/AlanD20/groundplane/internal/agent/lifecycleruntime"
	"github.com/AlanD20/groundplane/internal/agent/networkruntime"
	"github.com/AlanD20/groundplane/internal/agent/releaseruntime"
	"github.com/AlanD20/groundplane/internal/agent/removalruntime"
	"github.com/AlanD20/groundplane/internal/agent/resolverruntime"
	"github.com/AlanD20/groundplane/internal/agent/volumeruntime"
	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentstagingjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TASK-87 / HOST-04/07: real Unix gRPC streams must authenticate and finish
// registration/replay before Ready. On authenticated stream loss the old native
// execution must join before reconnect; drained configuration replaces capacity.
// The Controller is a protocol peer, not its production authentication/storage
// service. Only Script submission runs, and no host-effect Start is granted.
func TestNativeClientReconnectsAfterJoiningItsWorker(t *testing.T) {
	for _, code := range []codes.Code{codes.Unavailable, codes.Internal, codes.Unknown} {
		t.Run(code.String(), func(t *testing.T) {
			f := newNativeWorkersHostFixture(t, 2)
			delivery := f.assignment(t)
			attempt, err := workexecution.DecodeAgentAttempt(delivery.Attempt, f.workers.identity)
			if err != nil {
				t.Fatal(err)
			}
			token := bytes.Repeat([]byte{0x46}, agentprotocol.RawTokenBytes)
			var connections atomic.Int32
			results := make(chan error, 2)
			server := &nativeClientProtocolServer{connect: func(stream agentpb.AgentChannel_ConnectServer) error {
				connection := connections.Add(1)
				if err := nativeClientHandshake(stream, f.workers.identity, token, 2); err != nil {
					return err
				}
				if connection == 1 {
					if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_ScriptWorkAssignment{
						ScriptWorkAssignment: proto.CloneOf(delivery)}}); err != nil {
						return err
					}
					request, err := stream.Recv()
					if err != nil || request.GetWorkStartRequest() == nil ||
						!proto.Equal(request.GetWorkStartRequest().Attempt, delivery.Attempt) {
						return errs.New(errs.KindStateConflict, "first stream did not submit its exact native work")
					}
					return status.Error(code, "injected authenticated stream interruption")
				}
				if connection != 2 {
					return errs.New(errs.KindStateConflict, "Client connected beyond its bounded reconnect")
				}
				if err := stream.Send(nativeClientConfig(1)); err != nil {
					return err
				}
				ready, err := stream.Recv()
				if err != nil || ready.GetReady() == nil || ready.GetReady().Capacity != 1 {
					return errs.New(
						errs.KindStateConflict,
						"live drained configuration did not advertise its new capacity",
					)
				}
				return stream.Send(
					&agentpb.ControllerMessage{
						Payload: &agentpb.ControllerMessage_Shutdown{Shutdown: &agentpb.Shutdown{}},
					},
				)
			}, results: results}
			client := newNativeProtocolClient(t, f, server, token)
			reconnects := 0
			client.reconnect = func(context.Context, uint) error {
				reconnects++
				if reconnects != 1 || client.executionWorkers != nil {
					return errs.New(errs.KindStateConflict, "replacement stream retained old native workers")
				}
				exclusive, err := f.ownership.Begin(attempt)
				if err != nil {
					return err
				}
				return exclusive.Close()
			}
			if err := client.Run(t.Context()); err != nil {
				t.Fatalf("native Client reconnect: %v", err)
			}
			if connections.Load() != 2 || reconnects != 1 || client.executionWorkers != nil {
				t.Fatalf("connections=%d reconnects=%d; native shutdown was incomplete", connections.Load(), reconnects)
			}
			if err := <-results; status.Code(err) != code {
				t.Fatalf("first protocol peer: %v", err)
			}
			if err := <-results; err != nil {
				t.Fatalf("replacement protocol peer: %v", err)
			}
			if err := client.Run(t.Context()); err == nil {
				t.Fatal("Client reused its consumed channel token")
			}
		})
	}
}

// TASK-87: permanent authentication rejection and work before configuration
// are terminal; neither can become Ready or enter a reconnect loop.
func TestNativeClientRejectsAuthenticationAndFirstFrameViolations(t *testing.T) {
	for _, scenario := range []string{"unauthenticated", "permission denied", "work before configuration",
		"wrong process registration", "zero executor generation", "unacknowledged startup replay"} {
		t.Run(scenario, func(t *testing.T) {
			f := newNativeWorkersHostFixture(t, 1)
			token := bytes.Repeat([]byte{0x61}, agentprotocol.RawTokenBytes)
			results := make(chan error, 1)
			var connections atomic.Int32
			server := &nativeClientProtocolServer{
				results: results,
				connect: func(stream agentpb.AgentChannel_ConnectServer) error {
					connections.Add(1)
					frame, err := stream.Recv()
					if err != nil || frame.GetAuthenticate() == nil ||
						!bytes.Equal(frame.GetAuthenticate().Token, token) {
						return errs.New(errs.KindStateConflict, "authentication was not the exact first frame")
					}
					if scenario == "unauthenticated" || scenario == "permission denied" {
						code := codes.Unauthenticated
						if scenario == "permission denied" {
							code = codes.PermissionDenied
						}
						return status.Error(code, "private rejected credential "+string(token))
					}
					if scenario == "work before configuration" {
						if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkCancel{
							WorkCancel: &agentpb.WorkCancel{}}}); err != nil {
							return err
						}
					} else {
						if err := nativeClientInvalidRegistration(stream, f.workers.identity, frame.GetAuthenticate(), scenario); err != nil {
							return err
						}
					}
					if frame, err := stream.Recv(); err == nil || frame != nil {
						return errs.New(errs.KindStateConflict, "Client sent readiness before valid configuration")
					}
					return nil
				},
			}
			client := newNativeProtocolClient(t, f, server, token)
			reconnects := 0
			client.reconnect = func(context.Context, uint) error {
				reconnects++
				return errs.New(errs.KindStateConflict, "permanent rejection unexpectedly reconnected")
			}
			err := client.Run(t.Context())
			if err == nil || connections.Load() != 1 || reconnects != 0 {
				t.Fatalf(
					"invalid initial stream: err=%v connections=%d reconnects=%d",
					err,
					connections.Load(),
					reconnects,
				)
			}
			if strings.Contains(err.Error(), string(token)) ||
				strings.Contains(err.Error(), "private rejected credential") {
				t.Fatal("Client exposed private authentication details")
			}
			peer := <-results
			if scenario != "unauthenticated" && scenario != "permission denied" && peer != nil {
				t.Fatal(peer)
			}
		})
	}
}

func nativeClientInvalidRegistration(stream agentpb.AgentChannel_ConnectServer, identity workexecution.Identity,
	auth *agentpb.Authenticate, scenario string,
) error {
	if err := stream.Send(nativeClientConfig(1)); err != nil {
		return err
	}
	registration := &agentpb.WorkExecutorRegistration{AgentId: identity.AgentID, HostId: identity.HostID,
		ExecutorGeneration: 1, ProcessGeneration: append([]byte(nil), auth.ProcessGeneration...)}
	if scenario == "wrong process registration" {
		registration.ProcessGeneration[0] ^= 1
	}
	if scenario == "zero executor generation" {
		registration.ExecutorGeneration = 0
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkExecutorRegistration{
		WorkExecutorRegistration: registration}}); err != nil {
		return err
	}
	if scenario == "unacknowledged startup replay" {
		request, err := stream.Recv()
		if err != nil || request.GetWorkJournalReplayRequest() == nil ||
			!request.GetWorkJournalReplayRequest().Complete {
			return errs.New(errs.KindStateConflict, "startup replay was not requested before readiness")
		}
		return stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkJournalReplayAck{
			WorkJournalReplayAck: &agentpb.WorkJournalReplayAck{}}})
	}
	return nil
}

type nativeClientProtocolServer struct {
	agentpb.UnimplementedAgentChannelServer
	connect func(agentpb.AgentChannel_ConnectServer) error
	results chan error
}

func (server *nativeClientProtocolServer) Connect(stream agentpb.AgentChannel_ConnectServer) error {
	err := server.connect(stream)
	server.results <- err
	return err
}

func nativeClientConfig(capacity int32) *agentpb.ControllerMessage {
	return &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ConfigUpdate{ConfigUpdate: &agentpb.ConfigUpdate{
			AgentConfig: &agentpb.AgentConfig{PullIntervalSeconds: 60, MaxConcurrentTasks: capacity}}},
	}
}

func nativeClientHandshake(stream agentpb.AgentChannel_ConnectServer, identity workexecution.Identity,
	token []byte, capacity int32,
) error {
	frame, err := stream.Recv()
	if err != nil {
		return err
	}
	auth := frame.GetAuthenticate()
	if auth == nil || auth.AgentId != identity.AgentID || !bytes.Equal(auth.Token, token) ||
		auth.ChannelSchema != agentprotocol.SchemaVersion || auth.ExecutionPlanSchema != executionplan.SchemaVersion ||
		len(auth.ProcessGeneration) != 16 {
		return errs.New(errs.KindStateConflict, "Client did not authenticate before other frames")
	}
	if err := stream.Send(nativeClientConfig(capacity)); err != nil {
		return err
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkExecutorRegistration{
		WorkExecutorRegistration: &agentpb.WorkExecutorRegistration{AgentId: identity.AgentID, HostId: identity.HostID,
			ExecutorGeneration: identity.Generation, ProcessGeneration: auth.ProcessGeneration}}}); err != nil {
		return err
	}
	frame, err = stream.Recv()
	if err != nil || frame.GetWorkJournalReplayRequest() == nil || !frame.GetWorkJournalReplayRequest().Complete {
		return errs.New(errs.KindStateConflict, "Client became Ready before startup replay completed")
	}
	if err := stream.Send(&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_WorkJournalReplayAck{
		WorkJournalReplayAck: &agentpb.WorkJournalReplayAck{Complete: true}}}); err != nil {
		return err
	}
	frame, err = stream.Recv()
	if err != nil || frame.GetReady() == nil || frame.GetReady().Capacity != capacity ||
		frame.GetReady().GetStagingReady() {
		return errs.New(errs.KindStateConflict, "Client readiness lost capacity or invented usable Backup staging")
	}
	return nil
}

type nativeClientUnusedLogs struct{ agentprotocol.LogReader }

func newNativeProtocolClient(t *testing.T, f *nativeWorkersHostFixture,
	peer *nativeClientProtocolServer, token []byte,
) *Client {
	t.Helper()
	socket := filepath.Join(t.TempDir(), "controller.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	agentpb.RegisterAgentChannelServer(server, peer)
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, grpc.ErrServerStopped) {
			t.Error(err)
		}
	}()
	t.Cleanup(server.Stop)
	inputToken := append([]byte(nil), token...)
	client, err := NewClient(agentprotocol.SocketPath, f.workers.identity.AgentID,
		inputToken, "/var/lib/groundplane/vol",
		slog.New(slog.NewTextHandler(io.Discard, nil)), nativeClientUnusedLogs{})
	if err != nil {
		t.Fatal(err)
	}
	clear(inputToken)
	client.socketPath = socket
	client.processOwnership, client.workJournal = f.ownership, f.exchange.journal
	client.scriptExecutor = f.workers.scripts
	// No operations except Script submission are delivered by this protocol
	// peer. These unused handlers must never execute; production composition
	// and their host effects are not qualified by this transport-only fixture.
	client.releaseExecutor, client.releaseRecoveryExecutor = &releaseruntime.Executor{}, &releaseruntime.RecoveryExecutor{}
	client.hierarchyExecutor, client.attachRemovalExecutor = &hierarchyruntime.Executor{}, &hierarchyruntime.AttachExecutor{}
	client.volumeExecutor, client.networkExecutor = &volumeruntime.Executor{}, &networkruntime.Executor{}
	client.componentExecutor, client.entryExecutor = &componentruntime.Executor{}, &entryruntime.Executor{}
	client.attachProvisionExecutor, client.lifecycleExecutor = &attachruntime.Executor{}, &lifecycleruntime.Executor{}
	client.removalRecovery, client.backupExecutor = &removalruntime.Runtime{}, &backupruntime.Executor{}
	client.environmentExecutor, client.backingExecutor = &environmentruntime.Executor{}, &backingruntime.Executor{}
	client.resolverExecutor = &resolverruntime.Executor{}
	// Exercise the supported unavailable-staging path, not a successful empty
	// inventory fabricated without physical staging reconciliation.
	client.openStaging = func(context.Context, *agentstagingjournal.Journal) (*backupStagingState, error) {
		return nil, errs.New(errs.KindStateConflict, "isolated protocol peer has no Backup staging mount")
	}
	return client
}
