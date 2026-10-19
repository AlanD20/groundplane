package agentchannel

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type nativeFileDeliveryOwner struct {
	ReleaseWorkCheckpointer
	request  *agentpb.ReleaseConfigurationFileRequest
	delivery *agentpb.ReleaseConfigurationFileDelivery
	err      error
	calls    int
}

func (owner *nativeFileDeliveryOwner) LoadReleaseConfigurationFile(_ context.Context,
	authority agentregistration.WorkAuthorization, request *agentpb.ReleaseConfigurationFileRequest,
) (*agentpb.ReleaseConfigurationFileDelivery, error) {
	owner.calls++
	identity := authority.Identity()
	if !proto.Equal(request, owner.request) || identity.AgentID != request.Attempt.AgentId ||
		identity.HostID != request.Attempt.HostId || identity.Generation != request.Attempt.ExecutorGeneration {
		return nil, errs.New(errs.KindStateConflict, "test file request reached another source or executor")
	}
	return owner.delivery, owner.err
}

// TASK-114: native file delivery preserves the selected metadata/bytes, then
// clears the provider-owned plaintext on success, source refusal or send failure.
// The source port is supplied; its digest/Close checks and actual Agent writes
// are separate requirements, not qualified by this transport ownership check.
func TestNativeConfigurationDeliveryClearsPrivateBytes(t *testing.T) {
	for _, mode := range []string{"success", "source-error", "send-error"} {
		t.Run(mode, func(t *testing.T) {
			server, peer, authenticate := nativeConnectFixture(t)
			content := append(bytes.Repeat([]byte("private-test-value"), 2048), []byte("last-bytes")...)
			expected := bytes.Clone(content)
			stage, environmentID := ids.New(ids.KindStep), ids.New(ids.KindEnvironment)
			header, err := entrymaterialization.NewHeader(entrymaterialization.HeaderSpec{
				TaskID: peer.attempt.TaskId, StepID: stage, EnvironmentID: environmentID,
				Generation: 1, Destination: "secrets/qa-value", OutputKind: entrymaterialization.OutputSecretFile,
				Mode: entrymaterialization.ModePrivate, Length: uint64(len(content)), Digest: entrymaterialization.DigestBytes(content),
			})
			if err != nil {
				t.Fatal(err)
			}
			request := &agentpb.ReleaseConfigurationFileRequest{Attempt: proto.CloneOf(peer.attempt), StageId: stage,
				Destination: header.Destination()}
			owner := &nativeFileDeliveryOwner{request: proto.CloneOf(request),
				delivery: &agentpb.ReleaseConfigurationFileDelivery{
					Attempt: proto.CloneOf(peer.attempt),
					StageId: stage,
					Configuration: &agentpb.WorkConfigurationReference{ConfigurationId: ids.New(ids.KindConfig),
						EnvironmentId: environmentID, Generation: 1, Sha256: bytes.Repeat([]byte{1}, 32)},
					File:    releaseexecution.EncodeConfigurationFile(header),
					Content: content,
				}}
			server.releaseCheckpoints = owner
			stream := &nativeDispatchStream{ctx: t.Context(), received: []*agentpb.AgentMessage{
				authenticate, nativeConnectReplayComplete(), {Payload: &agentpb.AgentMessage_ReleaseConfigurationFileRequest{
					ReleaseConfigurationFileRequest: request}},
			}}
			if mode == "source-error" {
				owner.err = errs.New(errs.KindStateConflict, "source refused its private input")
			}
			if mode == "send-error" {
				stream.sendErr = func(message *agentpb.ControllerMessage) error {
					if message.GetReleaseConfigurationFileDelivery() != nil {
						return context.Canceled
					}
					return nil
				}
			}
			err = server.Connect(stream)
			if (err == nil) != (mode == "success") || owner.calls != 1 {
				t.Fatalf("native file boundary: success=%t calls=%d", err == nil, owner.calls)
			}
			if owner.delivery.Content != nil || !bytes.Equal(content, make([]byte, len(content))) {
				t.Fatal("native file delivery retained provider-owned private bytes")
			}
			if mode == "success" {
				if len(stream.messages) != 4 {
					t.Fatal("configuration file was not delivered")
				}
				delivered := stream.messages[3].GetReleaseConfigurationFileDelivery()
				if delivered == nil || !bytes.Equal(delivered.Content, expected) ||
					!proto.Equal(delivered.Attempt, peer.attempt) || delivered.StageId != stage ||
					!proto.Equal(delivered.File, releaseexecution.EncodeConfigurationFile(header)) {
					t.Fatal("delivered private bytes or captured metadata changed before Send")
				}
			} else if len(stream.messages) != 3 {
				t.Fatal("failed source or send emitted a completed file delivery")
			}
		})
	}
}
