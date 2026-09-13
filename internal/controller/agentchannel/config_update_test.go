package agentchannel

import (
	"context"
	"io"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// QA: HOST-07; scripted Ready sequencing, not actual worker drain or new-limit dispatch.
// Rationale: replacement config must wait for full capacity under the old limit;
// sending the lower limit on the busy Ready would reject the following idle Ready.
func TestConfigUpdateDrainsBeforeDelivery(t *testing.T) {
	t.Parallel()

	agentID := "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	authenticator := &fakeAuthenticator{
		authorization: Authorization{Generation: 7, Config: &agentpb.AgentConfig{
			PullIntervalSeconds: 2, MaxConcurrentTasks: 2,
		}},
		configuration: &agentpb.AgentConfig{PullIntervalSeconds: 5, MaxConcurrentTasks: 1},
	}
	stream := &scriptedStream{ctx: context.Background(), recvErr: io.EOF, messages: []*agentpb.AgentMessage{
		{Payload: &agentpb.AgentMessage_Authenticate{Authenticate: &agentpb.Authenticate{
			AgentId: agentID, Token: make([]byte, tokenSize),
		}}},
		{Payload: &agentpb.AgentMessage_Ready{Ready: &agentpb.Ready{Capacity: 1, Version: "v0.4.2"}}},
		{Payload: &agentpb.AgentMessage_Ready{Ready: &agentpb.Ready{Capacity: 2, Version: "v0.4.2"}}},
	}}
	server := New(authenticator, NewRegistry(), &fakeTaskStore{}, nil)
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if len(stream.sent) != 2 {
		t.Fatalf("sent messages = %d, want initial config plus drained update", len(stream.sent))
	}
	initial := stream.sent[0].GetConfigUpdate().GetAgentConfig()
	updated := stream.sent[1].GetConfigUpdate().GetAgentConfig()
	if initial.GetMaxConcurrentTasks() != 2 || initial.GetPullIntervalSeconds() != 2 ||
		updated.GetMaxConcurrentTasks() != 1 ||
		updated.GetPullIntervalSeconds() != 5 {
		t.Fatalf("configs = initial %#v, updated %#v", initial, updated)
	}
}
