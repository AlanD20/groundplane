package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestManagedNetworkEnsureBindsOwnedArtifactSelection(t *testing.T) {
	const networkID = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, name := range []string{"valid", "missing-artifact", "missing-network", "external-only", "remove-operation"} {
		t.Run(name, func(t *testing.T) {
			ensure := &agentpb.ManagedNetworkEnsure{ArtifactId: "artifact", NetworkId: networkID}
			artifact := &agentpb.ComposeArtifact{
				OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
				Networks:  []*agentpb.ComposeNetwork{{NetworkId: networkID}},
			}
			artifacts := map[string]*agentpb.ComposeArtifact{"artifact": artifact}
			operation := agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
			switch name {
			case "missing-artifact":
				ensure.ArtifactId = "missing"
			case "missing-network":
				ensure.NetworkId = "net_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			case "external-only":
				artifact.Networks = nil
			case "remove-operation":
				operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
			}
			err := validateManagedNetworkEnsure(operation, ensure, artifacts)
			if (name == "valid") != (err == nil) {
				t.Fatalf("validation=%v", err)
			}
		})
	}
}
