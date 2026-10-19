package agentchannel

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

// HOST-07, TASK-109: deliver a reduced worker limit only after Ready reports
// full capacity under the OLD limit. Native registration/journal replay runs;
// operation dispatch and configuration responses are supplied.
func TestConfigUpdateDrainsBeforeDelivery(t *testing.T) {
	server, peer, authenticate := nativeConnectFixture(t)
	server.auth = &fakeAuthenticator{
		authorization: Authorization{Generation: 1,
			Config: &agentpb.AgentConfig{PullIntervalSeconds: 2, MaxConcurrentTasks: 2}},
		configuration: &agentpb.AgentConfig{PullIntervalSeconds: 5, MaxConcurrentTasks: 1},
	}
	stream := &nativeDispatchStream{ctx: t.Context(), received: []*agentpb.AgentMessage{
		authenticate, nativeConnectReplayComplete(), nativeConnectReady(1), nativeConnectReady(2),
	}}
	if err := server.Connect(stream); err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	if len(stream.messages) != 4 || peer.calls != 0 {
		t.Fatalf(
			"busy/config-transition Ready dispatched work: frames=%d dispatch=%d",
			len(stream.messages),
			peer.calls,
		)
	}
	initial := stream.messages[0].GetConfigUpdate().GetAgentConfig()
	updated := stream.messages[3].GetConfigUpdate().GetAgentConfig()
	if initial.GetMaxConcurrentTasks() != 2 || initial.GetPullIntervalSeconds() != 2 ||
		updated.GetMaxConcurrentTasks() != 1 ||
		updated.GetPullIntervalSeconds() != 5 {
		t.Fatalf("configs = initial %#v, updated %#v", initial, updated)
	}
}
