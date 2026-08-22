package agent

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/agentprotocol"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestClientAppliesLiveConfigAndReadvertisesCapacity(t *testing.T) {
	t.Parallel()

	stream := newFakeStream(
		configMessage(60, 2),
		configMessage(5, 1),
		&agentpb.ControllerMessage{Payload: &agentpb.ControllerMessage_Shutdown{Shutdown: &agentpb.Shutdown{}}},
	)
	client := newTestClient(t, make([]byte, agentprotocol.RawTokenBytes), stream)
	if err := client.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	var capacities []int32
	for _, message := range stream.sentMessages() {
		if ready := message.GetReady(); ready != nil {
			capacities = append(capacities, ready.Capacity)
		}
	}
	if len(capacities) != 2 || capacities[0] != 2 || capacities[1] != 1 {
		t.Fatalf("Ready capacities = %v, want [2 1]", capacities)
	}
}
