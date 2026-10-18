package app

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// TASK-52: the real channel authenticator, process registration and native
// credential route must deliver the captured source on reconnect, not restore
// generic assignment-driven delivery or retain plaintext in transport objects.
func TestNativeBackupChannelDeliversCapturedCredentialsAcrossReconnect(t *testing.T) {
	f := newNativeBackupCredentialFixture(t, core.SecretScopeProject)
	server := newNativeBackupCredentialChannel(t, f)
	for range 2 {
		stream := newNativeBackupCredentialStream(t, f)
		if err := server.Connect(stream); err != nil {
			t.Fatal(err)
		}
		var transfers []*agentpb.BackupWorkSecretTransfer
		var configurations, registrations, replayAcks int
		for _, message := range stream.sent {
			if message.GetConfigUpdate() != nil {
				configurations++
			}
			if message.GetWorkJournalReplayAck().GetComplete() {
				replayAcks++
			}
			if frame := message.GetBackupWorkSecretTransfer(); frame != nil {
				transfers = append(transfers, frame)
			}
			if registration := message.GetWorkExecutorRegistration(); registration != nil {
				registrations++
				if registration.HostId != f.attempt.HostId ||
					registration.ExecutorGeneration != f.attempt.ExecutorGeneration {
					t.Fatal("same-process reconnect created another executor identity")
				}
			}
		}
		if configurations != 1 || registrations != 1 || replayAcks != 1 {
			t.Fatal("channel did not finish authentication/registration and startup replay")
		}
		if len(transfers) != 6 {
			t.Fatalf("got %d credential frames, want two complete slots", len(transfers))
		}
		for index, want := range []struct {
			purpose agentpb.BackupSecretSlotPurpose
			value   []byte
		}{
			{agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_ACCESS_KEY, []byte("app-access")},
			{agentpb.BackupSecretSlotPurpose_BACKUP_SECRET_SLOT_PURPOSE_S3_SECRET_KEY, []byte("app-secret")},
		} {
			header, chunk, end := transfers[index*3], transfers[index*3+1], transfers[index*3+2]
			for _, frame := range []*agentpb.BackupWorkSecretTransfer{header, chunk, end} {
				if !proto.Equal(frame.Attempt, f.attempt) || frame.SourceOrdinal != 0 || frame.Purpose != want.purpose {
					t.Fatal("channel delivered credentials for another attempt, source or purpose")
				}
			}
			if header.GetHeader().GetChunkCount() != 1 || chunk.GetChunk().GetSequence() != 1 ||
				!bytes.Equal(chunk.GetChunk().GetContent(), want.value) || end.GetEnd().GetChunkCount() != 1 {
				t.Fatal("channel delivered incomplete or different credentials")
			}
		}
		assertNativeBackupChannelCleared(t, f, stream)
	}
}

// TASK-52: rejected authentication/attempts and a failed stream cannot expose
// unrelated credentials or retain opened slots after Connect returns.
func TestNativeBackupChannelRejectsUnauthorizedDeliveryAndClearsFailedSend(t *testing.T) {
	for _, failure := range []string{"token", "attempt", "send"} {
		t.Run(failure, func(t *testing.T) {
			f := newNativeBackupCredentialFixture(t, core.SecretScopeProject)
			server := newNativeBackupCredentialChannel(t, f)
			stream := newNativeBackupCredentialStream(t, f)
			switch failure {
			case "token":
				stream.messages[0].GetAuthenticate().Token[0]++
			case "attempt":
				stream.messages[2].GetBackupWorkSecretRequest().Attempt.ExecutorGeneration++
			case "send":
				stream.failChunk = true
			}
			err := server.Connect(stream)
			if err == nil {
				t.Fatal("invalid credential delivery was accepted")
			}
			if failure == "token" && status.Code(err) != codes.Unauthenticated {
				t.Fatalf("wrong token was not rejected by authentication: %v", err)
			}
			if failure == "attempt" && status.Code(err) != codes.FailedPrecondition {
				t.Fatalf("foreign attempt did not reach its native ownership fence: %v", err)
			}
			if failure == "token" && len(stream.sent) != 0 {
				t.Fatal("unauthenticated channel received configuration or credentials")
			}
			if failure != "send" && len(f.crypt.opened) != 0 {
				t.Fatal("rejected identity opened credential plaintext")
			}
			if failure == "send" && len(f.crypt.opened) != 2 {
				t.Fatal("stream failure did not reach plaintext delivery")
			}
			assertNativeBackupChannelCleared(t, f, stream)
		})
	}
}

func newNativeBackupCredentialChannel(t *testing.T, f *nativeBackupCredentialFixture) *agentchannel.Server {
	t.Helper()
	auth, err := channeltransport.NewAuthenticator(f.agents)
	if err != nil {
		t.Fatal(err)
	}
	server := agentchannel.New(auth, agentchannel.NewRegistry(), nil, nil, nil)
	// These unrelated ports fail explicitly if invoked. This journey resumes an
	// already-started Backup attempt; it neither dispatches nor reports other work.
	unused := nativeBackupUncalledOperations{}
	if err := server.ConfigureWorkExecution(f.agents, unused, unused, unused, unused, unused); err != nil {
		t.Fatal(err)
	}
	if err := server.ConfigureBackupWorkSecrets(f.delivery); err != nil {
		t.Fatal(err)
	}
	return server
}

func assertNativeBackupChannelCleared(
	t *testing.T,
	f *nativeBackupCredentialFixture,
	stream *nativeBackupCredentialStream,
) {
	t.Helper()
	for _, buffer := range append(append(append([][]byte(nil), f.crypt.ciphertexts...), f.crypt.opened...), stream.aliases...) {
		for _, value := range buffer {
			if value != 0 {
				t.Fatal("channel delivery retained ciphertext or plaintext aliases")
			}
		}
	}
	for _, message := range stream.retained {
		if message.GetBackupWorkSecretTransfer().GetChunk().GetContent() != nil {
			t.Fatal("retained transport message exposes credential bytes")
		}
	}
}

type nativeBackupCredentialStream struct {
	ctx            context.Context
	messages       []*agentpb.AgentMessage
	next           int
	ends           int
	done           chan struct{}
	failChunk      bool
	sent, retained []*agentpb.ControllerMessage
	aliases        [][]byte
}

func newNativeBackupCredentialStream(t *testing.T, f *nativeBackupCredentialFixture) *nativeBackupCredentialStream {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	t.Cleanup(cancel)
	process := [16]byte{1}
	return &nativeBackupCredentialStream{ctx: ctx, done: make(chan struct{}), messages: []*agentpb.AgentMessage{
		{Payload: &agentpb.AgentMessage_Authenticate{Authenticate: &agentpb.Authenticate{
			AgentId: f.agentID, Token: bytes.Clone(f.token[:]), ProcessGeneration: bytes.Clone(process[:]),
			ExecutionPlanSchema: executionplan.SchemaVersion, ChannelSchema: agentprotocol.SchemaVersion}}},
		{
			Payload: &agentpb.AgentMessage_WorkJournalReplayRequest{
				WorkJournalReplayRequest: &agentpb.WorkJournalReplayRequest{Complete: true},
			},
		},
		{
			Payload: &agentpb.AgentMessage_BackupWorkSecretRequest{
				BackupWorkSecretRequest: &agentpb.BackupWorkSecretRequest{
					Attempt: proto.CloneOf(f.attempt), SourceOrdinal: 0},
			},
		},
	}}
}

func (stream *nativeBackupCredentialStream) Context() context.Context { return stream.ctx }
func (stream *nativeBackupCredentialStream) Recv() (*agentpb.AgentMessage, error) {
	if stream.next < len(stream.messages) {
		message := stream.messages[stream.next]
		stream.next++
		return message, nil
	}
	select {
	case <-stream.done:
		return nil, io.EOF
	case <-stream.ctx.Done():
		return nil, stream.ctx.Err()
	}
}
func (stream *nativeBackupCredentialStream) Send(message *agentpb.ControllerMessage) error {
	stream.retained = append(stream.retained, message)
	frame := message.GetBackupWorkSecretTransfer()
	if chunk := frame.GetChunk(); chunk != nil {
		stream.aliases = append(stream.aliases, chunk.Content)
		if stream.failChunk {
			return errs.New(errs.KindRequestFailed, "injected credential stream failure")
		}
	}
	stream.sent = append(stream.sent, proto.CloneOf(message))
	if frame.GetEnd() != nil {
		stream.ends++
		if stream.ends == 2 {
			close(stream.done)
		}
	}
	return nil
}
func (*nativeBackupCredentialStream) SetHeader(metadata.MD) error {
	return unexpectedBackupChannelOperation()
}
func (*nativeBackupCredentialStream) SendHeader(metadata.MD) error {
	return unexpectedBackupChannelOperation()
}
func (*nativeBackupCredentialStream) SetTrailer(metadata.MD) {}
func (*nativeBackupCredentialStream) SendMsg(any) error      { return unexpectedBackupChannelOperation() }
func (*nativeBackupCredentialStream) RecvMsg(any) error      { return unexpectedBackupChannelOperation() }

func unexpectedBackupChannelOperation() error {
	return errs.New(errs.KindInternal, "unexpected operation outside native Backup credential delivery")
}

type nativeBackupUncalledOperations struct{}

func (nativeBackupUncalledOperations) Operations() []workexecution.Operation {
	return []workexecution.Operation{workexecution.BackupPrune}
}

func (nativeBackupUncalledOperations) Start(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.WorkStartRequest,
) (*agentpb.WorkStartAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) AccountJournal(
	context.Context,
	agentregistration.WorkAuthorization,
	string,
	*agentpb.WorkAttemptIdentity,
) error {
	return unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) Ready(
	context.Context,
	agentregistration.WorkAuthorization,
	uint16,
	bool,
) ([]workdispatch.Issue, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) Dispatch(
	context.Context,
	agentregistration.WorkAuthorization,
	int32,
	map[string]string,
	func(*agentpb.ControllerMessage) (bool, error),
) ([]workdispatch.Issue, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) FailScript(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ScriptWorkFailureRequest,
) (*agentpb.ScriptWorkFailureAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) CompleteScriptRecovery(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ScriptRecoveryCompletionRequest,
) (*agentpb.ScriptRecoveryCompletionAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) CompleteScript(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ScriptWorkCompletionRequest,
) (*agentpb.ScriptWorkCompletionAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) FailEnvironmentRemoval(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.EnvironmentRemovalFailureRequest,
) (*agentpb.EnvironmentRemovalFailureAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) CheckpointEnvironmentRemoval(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.EnvironmentRemovalCheckpointRequest,
) (*agentpb.EnvironmentRemovalCheckpointAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) CompleteEnvironmentRemoval(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.EnvironmentRemovalCompletionRequest,
) (*agentpb.EnvironmentRemovalCompletionAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) CheckpointRecovery(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ReleaseRecoveryCheckpointRequest,
) (*agentpb.ReleaseRecoveryCheckpointAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) FailRelease(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ReleaseWorkFailureRequest,
) (*agentpb.ReleaseWorkFailureAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) CompleteRelease(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ReleaseWorkCompletionRequest,
) (*agentpb.ReleaseWorkCompletionAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) LoadReleaseConfiguration(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ReleaseConfigurationRequest,
) (*agentpb.ReleaseConfigurationManifest, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) LoadReleaseConfigurationFile(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ReleaseConfigurationFileRequest,
) (*agentpb.ReleaseConfigurationFileDelivery, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) PublishReleaseEffect(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ReleaseEffectEvidenceRequest,
) (*agentpb.ReleaseEffectEvidenceAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) CheckpointRelease(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ReleaseWorkCheckpointRequest,
) (*agentpb.ReleaseWorkCheckpointAck, error) {
	return nil, unexpectedBackupChannelOperation()
}

func (nativeBackupUncalledOperations) LoadReleaseHook(
	context.Context,
	agentregistration.WorkAuthorization,
	*agentpb.ReleaseHookRequest,
) (*agentpb.ReleaseHookDelivery, error) {
	return nil, unexpectedBackupChannelOperation()
}
