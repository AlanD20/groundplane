package composeobserver

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/network"
	"github.com/moby/moby/client"
)

// Built-in bridge is ambient, while even an empty Compose-created default
// network remains collision evidence when it is absent from sealed authority.
func TestObserveDefaultNetworkPreservesOwnershipBoundary(t *testing.T) {
	for _, projectOwned := range []bool{false, true} {
		name := "bridge"
		labels := map[string]string{}
		if projectOwned {
			name = "groundplane-infra_default"
			labels[composeProjectLabel] = "groundplane-infra"
			labels[composeNetworkLabel] = "default"
		}
		t.Run(name, func(t *testing.T) {
			engine := &defaultNetworkEngine{item: network.Network{ID: "network-id", Name: name, Labels: labels}}
			observer, err := NewWithEngine(engine)
			if err != nil {
				t.Fatal(err)
			}
			result, err := observer.Observe(context.Background(), observerPlan(t), observerArtifactID)
			if err != nil {
				t.Fatal(err)
			}
			if len(result.Networks) != 0 {
				t.Fatal("unsealed network accepted as owned")
			}
			if projectOwned {
				if engine.inspected != 1 || len(result.Collisions) != 1 ||
					result.Collisions[0].Kind != agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK ||
					result.Collisions[0].Name != name {
					t.Fatalf("project default not rejected: %v", result)
				}
			} else if engine.inspected != 0 || len(result.Collisions) != 0 {
				t.Fatalf("ambient bridge claimed: %v", result)
			}
		})
	}
}

type defaultNetworkEngine struct {
	fakeEngine
	item      network.Network
	inspected int
}

func (engine *defaultNetworkEngine) NetworkList(
	context.Context,
	client.NetworkListOptions,
) (client.NetworkListResult, error) {
	return client.NetworkListResult{Items: []network.Summary{{Network: engine.item}}}, nil
}

func (engine *defaultNetworkEngine) NetworkInspect(
	context.Context,
	string,
	client.NetworkInspectOptions,
) (client.NetworkInspectResult, error) {
	engine.inspected++
	return client.NetworkInspectResult{Network: network.Inspect{Network: engine.item}}, nil
}
