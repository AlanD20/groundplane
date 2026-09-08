package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestBlueprintManagedNoDependenciesDoesNotAuthorizeNativeOrAmbiguousSelection(t *testing.T) {
	for _, name := range []string{"managed", "native", "mixed", "unknown", "duplicate-service", "duplicate-owner", "foreign-operation", "native-force"} {
		t.Run(name, func(t *testing.T) {
			artifactID, serviceID, nativeID, componentID := ids.New(
				ids.KindConfig,
			), ids.New(
				ids.KindService,
			), ids.New(
				ids.KindService,
			), ids.New(
				ids.KindComponent,
			)
			managed := &agentpb.ComposeService{ServiceId: serviceID, OwnerComponentId: componentID}
			artifact := &agentpb.ComposeArtifact{
				ArtifactId: artifactID,
				OwnerKind:  agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
				Services:   []*agentpb.ComposeService{managed, {ServiceId: nativeID}},
			}
			apply := &agentpb.ComposeApply{
				ArtifactId:     artifactID,
				ServiceIds:     []string{serviceID},
				NoDependencies: true,
			}
			operation := agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
			switch name {
			case "native":
				apply.ServiceIds = []string{nativeID}
			case "mixed":
				apply.ServiceIds = []string{serviceID, nativeID}
			case "unknown":
				apply.ServiceIds = []string{ids.New(ids.KindService)}
			case "duplicate-service":
				artifact.Services = append(artifact.Services, managed)
			case "duplicate-owner":
				artifact.Services[1].OwnerComponentId = componentID
			case "foreign-operation":
				operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
			case "native-force":
				apply.ServiceIds, apply.ForceRecreate = []string{nativeID}, true
			}
			step := &agentpb.ExecutionStep{
				StepId:         ids.New(ids.KindStep),
				TimeoutSeconds: 120,
				Payload:        &agentpb.ExecutionStep_ComposeApply{ComposeApply: apply},
			}
			err := validateStep(operation, 1, step, map[string]*agentpb.ComposeArtifact{artifactID: artifact}, nil)
			wantAllowed := name == "managed" || name == "native-force"
			if (err == nil) != wantAllowed {
				t.Fatalf("selection validation = %v, allowed=%t", err, wantAllowed)
			}
		})
	}
}
