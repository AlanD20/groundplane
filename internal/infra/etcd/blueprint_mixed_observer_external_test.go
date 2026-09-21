package etcd_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"
)

// Only Engine reads are simulated; the production observer validates labels and
// returns the independently inspected image/replica evidence to the real Worker.
type mixedRecoveryEngine struct {
	containers []container.Summary
	inspects   map[string]client.ContainerInspectResult
}

func newMixedRecoveryEngine(runtime *mixedRecoveryRuntime) *mixedRecoveryEngine {
	engine := &mixedRecoveryEngine{inspects: make(map[string]client.ContainerInspectResult)}
	for _, service := range runtime.witness.Services {
		if service.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON {
			continue
		}
		if !runtime.restored[service.ServiceId] {
			runtime.t.Fatal("positive observation preceded compensation")
		}
		labels := map[string]string{
			"com.docker.compose.project": runtime.witness.ProjectName,
			"com.docker.compose.service": service.ComposeName,
		}
		for _, label := range service.ExpectedLabels {
			labels[label.Key] = label.Value
		}
		for index := uint32(0); index < service.ExpectedReplicas; index++ {
			id := fmt.Sprintf("%064x", len(engine.containers)+1)
			engine.containers = append(engine.containers, container.Summary{ID: id, Labels: labels})
			engine.inspects[id] = client.ContainerInspectResult{Container: container.InspectResponse{
				ID: id, Name: fmt.Sprintf("/%s-%s-%d", runtime.witness.ProjectName, service.ComposeName, index+1), Image: service.ImageReference,
				Config: &container.Config{Image: service.ImageReference, Labels: labels},
				State:  &container.State{Status: container.StateRunning, Health: &container.Health{Status: "healthy"}},
			}}
		}
	}
	return engine
}

func (engine *mixedRecoveryEngine) ContainerList(
	context.Context,
	client.ContainerListOptions,
) (client.ContainerListResult, error) {
	return client.ContainerListResult{Items: engine.containers}, nil
}

func (engine *mixedRecoveryEngine) ContainerInspect(
	_ context.Context,
	id string,
	_ client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	return engine.inspects[id], nil
}
func (*mixedRecoveryEngine) NetworkList(context.Context, client.NetworkListOptions) (client.NetworkListResult, error) {
	return client.NetworkListResult{}, nil
}

func (*mixedRecoveryEngine) NetworkInspect(
	context.Context,
	string,
	client.NetworkInspectOptions,
) (client.NetworkInspectResult, error) {
	return client.NetworkInspectResult{}, nil
}
func (*mixedRecoveryEngine) VolumeList(context.Context, client.VolumeListOptions) (client.VolumeListResult, error) {
	return client.VolumeListResult{}, nil
}

func (*mixedRecoveryEngine) VolumeInspect(
	context.Context,
	string,
	client.VolumeInspectOptions,
) (client.VolumeInspectResult, error) {
	return client.VolumeInspectResult{}, nil
}
func (*mixedRecoveryEngine) Close() error { return nil }

func proveMixedObservationBoundary(t *testing.T, assignment testtaskassignment.Assignment) *agentpb.ComposeArtifact {
	t.Helper()
	var stepID, selectedServiceID string
	for _, member := range assignment.Plan.CandidateReleaseProcedure.Members {
		if executionplan.RestorationTargetForService(
			assignment.RestorationAuthority,
			member.ServiceId,
		) == agentpb.ReleaseRestorationTarget_RELEASE_RESTORATION_TARGET_SERVING_PREDECESSOR {
			stepID = member.ServingPredecessor.CompensateStepId
			selectedServiceID = member.ServiceId
		}
	}
	var nativeIndex = -1
	var nativeBytes []byte
	for index, predecessor := range assignment.RestorationAuthority.NativePredecessors {
		if predecessor.ServiceId == selectedServiceID {
			if nativeIndex >= 0 {
				t.Fatal("mixed authority repeated the selected native predecessor")
			}
			nativeIndex, nativeBytes = index, predecessor.CurrentArtifact
		}
	}
	if stepID == "" || nativeIndex < 0 || len(nativeBytes) == 0 {
		t.Fatal("mixed authority omitted the selected serving native predecessor")
	}
	beforePlan, beforeAuthority := proto.CloneOf(assignment.Plan), proto.CloneOf(assignment.RestorationAuthority)
	observation, err := executionplan.NewRestorationObservation(
		assignment.Plan,
		assignment.RestorationAuthority,
		stepID,
	)
	if err != nil {
		t.Fatal(err)
	}
	artifact := observation.Artifact()
	descriptorBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil || !bytes.Equal(descriptorBytes, nativeBytes) {
		t.Fatal("observation descriptor did not preserve exact selected native bytes")
	}
	if applied := assignment.RestorationAuthority.AppliedPredecessor; applied != nil &&
		bytes.Equal(descriptorBytes, applied.ComposeArtifact) {
		t.Fatal("mixed fixture observation selected the applied artifact instead of the distinct native predecessor")
	}
	artifact.OwnerId = "mutated-copy"
	if observation.Artifact().OwnerId == artifact.OwnerId || !proto.Equal(beforePlan, assignment.Plan) ||
		!proto.Equal(beforeAuthority, assignment.RestorationAuthority) {
		t.Fatal("historical observation changed its input or exposed mutable authority")
	}
	for _, test := range []struct {
		name   string
		mutate func(*agentpb.ReleaseRestorationAuthority)
	}{
		{"changed plan hash", func(a *agentpb.ReleaseRestorationAuthority) { a.PlanHash[0] ^= 1 }},
		{"changed applied hash", func(a *agentpb.ReleaseRestorationAuthority) {
			a.AppliedPredecessor.ComposeArtifactSha256[0] ^= 1
		}},
		{"foreign owner", func(a *agentpb.ReleaseRestorationAuthority) {
			a.EnvironmentId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		}},
		{"foreign member", func(a *agentpb.ReleaseRestorationAuthority) {
			a.Candidates[0].ReleaseId = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		}},
		{"missing native authority", func(a *agentpb.ReleaseRestorationAuthority) {
			a.NativePredecessors = nil
		}},
		{"foreign native authority", func(a *agentpb.ReleaseRestorationAuthority) {
			a.NativePredecessors[nativeIndex].ServiceId = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{"changed native authority", func(a *agentpb.ReleaseRestorationAuthority) {
			a.NativePredecessors[nativeIndex].CurrentArtifact[0] ^= 0xff
		}},
	} {
		changed := proto.CloneOf(assignment.RestorationAuthority)
		test.mutate(changed)
		if _, err := executionplan.NewRestorationObservation(assignment.Plan, changed, stepID); err == nil {
			t.Fatalf("observation accepted %s", test.name)
		}
	}
	withoutApplied := proto.CloneOf(assignment.RestorationAuthority)
	withoutApplied.AppliedPredecessor = nil
	nullableObservation, err := executionplan.NewRestorationObservation(assignment.Plan, withoutApplied, stepID)
	if err != nil {
		t.Fatalf("observation rejected nullable applied witness: %v", err)
	}
	nullableBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(nullableObservation.Artifact())
	if err != nil || !bytes.Equal(nullableBytes, nativeBytes) {
		t.Fatal("nullable applied witness changed the selected native observation")
	}
	if _, err := executionplan.NewRestorationObservation(assignment.Plan, assignment.RestorationAuthority, "undeclared"); err == nil {
		t.Fatal("observation accepted undeclared step")
	}
	changedPlan := proto.CloneOf(assignment.Plan)
	changedPlan.RenderGeneration++
	if _, err := executionplan.NewRestorationObservation(changedPlan, assignment.RestorationAuthority, stepID); err == nil {
		t.Fatal("observation bypassed current plan hash")
	}
	return observation.Artifact()
}
