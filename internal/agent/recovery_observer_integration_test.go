package agent

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/docker/composeobserver"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
)

// Rationale: SVC-09 exercises the actual observer and Agent verifier together:
// recovery succeeds before and after candidate creation, but not after ownership
// drift. A mocked collision-free observation would miss the original defect.
func TestRecoveryObserverCandidateCreationSequence(t *testing.T) {
	assignment, probe := sealedRecreateProbeAssignment(t, 1, "blue")
	prior := assignment.Plan.Artifacts[1]
	candidate := assignment.Plan.Artifacts[0]
	engine := &recoveryInventoryEngine{}
	engine.add(prior, true)
	observer, err := composeobserver.NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}
	runtime := &ComposeRuntime{observer: observer}
	proof := composeStepResult{RecreateEvidence: &agentpb.ServiceRecreateEvidence{
		ServiceId: prior.Services[0].ServiceId, ArtifactId: prior.ArtifactId,
		ReleaseId: probe.GetServiceRecreateProbe().PriorReleaseId, Target: "blue", Compensated: true,
	}}
	for _, stage := range []string{"before candidate", "candidate created", "candidate ownership changed"} {
		t.Run(stage, func(t *testing.T) {
			if stage == "candidate created" {
				engine.add(candidate, false)
			}
			if stage == "candidate ownership changed" {
				engine.items[1].Config.Labels["com.groundplane.plan-id"] = "foreign"
			}
			result, err := runtime.verifyReleaseRestorationPostcondition(
				context.Background(),
				assignment,
				probe,
				proof,
				nil,
			)
			if stage == "candidate ownership changed" {
				if err == nil || !result.ReconciliationRequired {
					t.Fatal("foreign candidate closed recovery")
				}
			} else {
				if err != nil || result.ReconciliationRequired {
					t.Fatalf("recovery blocked: %v", err)
				}
				if len(result.Observed.Containers) != len(engine.items) {
					t.Fatal("candidate evidence was dropped")
				}
				// Bound polling if the native recovery path loses the predecessor.
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				recovered, err := runtime.observeRecreateRecovery(ctx, assignment, probe)
				if err != nil || recovered.RecreateEvidence.GetArtifactId() != prior.ArtifactId {
					t.Fatalf("native recovery lost healthy predecessor: %v", err)
				}
			}
		})
	}
}

type recoveryInventoryEngine struct{ items []container.InspectResponse }

func (engine *recoveryInventoryEngine) add(artifact *agentpb.ComposeArtifact, healthy bool) {
	service := artifact.Services[0]
	labels := map[string]string{
		"com.docker.compose.project": artifact.ProjectName,
		"com.docker.compose.service": service.ComposeName,
	}
	for _, label := range service.ExpectedLabels {
		labels[label.Key] = label.Value
	}
	state := &container.State{Status: container.StateCreated, Health: &container.Health{Status: "unhealthy"}}
	if healthy {
		state.Status, state.Health.Status = container.StateRunning, "healthy"
	}
	engine.items = append(engine.items, container.InspectResponse{
		ID: fmt.Sprintf(
			"%064x",
			len(engine.items)+1,
		), Name: "/" + service.ComposeName + "-1", Image: service.ImageReference,
		Config: &container.Config{Image: service.ImageReference, Labels: labels}, State: state,
	})
}

func (engine *recoveryInventoryEngine) ContainerList(
	context.Context,
	client.ContainerListOptions,
) (client.ContainerListResult, error) {
	var result client.ContainerListResult
	for _, item := range engine.items {
		result.Items = append(result.Items, container.Summary{ID: item.ID, Labels: item.Config.Labels})
	}
	return result, nil
}

func (engine *recoveryInventoryEngine) ContainerInspect(
	_ context.Context,
	id string,
	_ client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	for _, item := range engine.items {
		if item.ID == id {
			return client.ContainerInspectResult{Container: item}, nil
		}
	}
	return client.ContainerInspectResult{}, nil
}

func (*recoveryInventoryEngine) NetworkList(
	context.Context,
	client.NetworkListOptions,
) (client.NetworkListResult, error) {
	return client.NetworkListResult{}, nil
}

func (*recoveryInventoryEngine) NetworkInspect(
	context.Context,
	string,
	client.NetworkInspectOptions,
) (client.NetworkInspectResult, error) {
	return client.NetworkInspectResult{}, nil
}
func (*recoveryInventoryEngine) VolumeList(context.Context, client.VolumeListOptions) (client.VolumeListResult, error) {
	return client.VolumeListResult{}, nil
}

func (*recoveryInventoryEngine) VolumeInspect(
	context.Context,
	string,
	client.VolumeInspectOptions,
) (client.VolumeInspectResult, error) {
	return client.VolumeInspectResult{}, nil
}
func (*recoveryInventoryEngine) Close() error { return nil }
