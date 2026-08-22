package app

import (
	"bytes"
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type fakeAgentChannelResolver struct {
	authorization etcd.LocalAgentChannelAuthorization
	agentID       string
	token         [agentprotocol.RawTokenBytes]byte
}

func (resolver *fakeAgentChannelResolver) ResolveAgentChannel(
	_ context.Context,
	agentID string,
	token [agentprotocol.RawTokenBytes]byte,
) (etcd.LocalAgentChannelAuthorization, error) {
	resolver.agentID = agentID
	resolver.token = token
	return resolver.authorization, nil
}

func (resolver *fakeAgentChannelResolver) GetSingleton(
	context.Context,
) (etcd.Versioned[etcd.LocalAgentRecord], error) {
	return etcd.Versioned[etcd.LocalAgentRecord]{Record: etcd.LocalAgentRecord{
		ID:         resolver.authorization.AgentID,
		Generation: resolver.authorization.Generation,
		Phase:      etcd.LocalAgentPhaseReady,
		Config:     resolver.authorization.Config,
	}, Revision: 1}, nil
}

func TestAgentChannelAuthenticatorTranslatesDurableAuthorization(t *testing.T) {
	resolver := &fakeAgentChannelResolver{authorization: etcd.LocalAgentChannelAuthorization{
		AgentID: "agt_01ARZ3NDEKTSV4RRFFQ69G5FAV", Generation: 7,
		Config: etcd.LocalAgentConfig{
			PullIntervalSeconds: 3, MaxConcurrentTasks: 2,
			Labels: map[string]string{"role": "local"},
		},
	}}
	authenticator, err := newAgentChannelAuthenticator(resolver)
	if err != nil {
		t.Fatalf("newAgentChannelAuthenticator() error = %v", err)
	}
	tokenBytes := bytes.Repeat([]byte{9}, agentprotocol.RawTokenBytes)
	var token agentchannel.Token
	copy(token[:], tokenBytes)
	authorization, err := authenticator.Authenticate(
		context.Background(),
		resolver.authorization.AgentID,
		token,
	)
	if err != nil {
		t.Fatalf("Authenticate() error = %v", err)
	}
	if authorization.Generation != 7 || authorization.Config.PullIntervalSeconds != 3 ||
		authorization.Config.MaxConcurrentTasks != 2 ||
		authorization.Config.Labels["role"] != "local" {
		t.Fatalf("Authenticate() = %#v", authorization)
	}
	if resolver.agentID != resolver.authorization.AgentID ||
		!bytes.Equal(resolver.token[:], tokenBytes) {
		t.Fatalf("resolver input = %q, %x", resolver.agentID, resolver.token)
	}
}
