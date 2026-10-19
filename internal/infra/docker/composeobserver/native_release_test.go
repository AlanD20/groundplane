package composeobserver

import (
	"crypto/sha256"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"
)

// SVC-09/18, TASK-70: the actual native observer must retain exact predecessor
// evidence beside a newly created candidate. A healthy predecessor cannot make
// that candidate ready; candidate ownership drift must be a blocking collision.
// Docker is simulated. This does not execute restoration or Controller settlement.
func TestNativeReleaseObservationKeepsCapturedCompanionsWithoutBorrowingReadiness(t *testing.T) {
	input, prior := nativeObservedReleaseInput(t)
	engine := &fakeEngine{inspects: make(map[string]client.ContainerInspectResult)}
	add := func(artifact *agentpb.ComposeArtifact, id string, healthy bool) {
		service := artifact.Services[0]
		labels := map[string]string{composeProjectLabel: artifact.ProjectName, composeServiceLabel: service.ComposeName}
		for _, label := range service.ExpectedLabels {
			labels[label.Key] = label.Value
		}
		state := &container.State{Status: container.StateCreated, Health: &container.Health{Status: "unhealthy"}}
		if healthy {
			state.Status, state.Health.Status = container.StateRunning, "healthy"
		}
		engine.containers = append(engine.containers, container.Summary{ID: id, Labels: labels})
		engine.inspects[id] = client.ContainerInspectResult{Container: container.InspectResponse{ID: id,
			Name: "/" + service.ComposeName + "-1", Image: service.ImageReference,
			Config: &container.Config{Image: service.ImageReference, Labels: labels}, State: state}}
	}
	add(prior, strings.Repeat("a", 64), true)
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}
	progress := releaseexecution.Progress{AttemptID: input.Attempt.ID, Sequence: 1, Phase: releaseexecution.Rollout,
		Action: releaseexecution.WaitCandidate, InFlight: true, BeganAt: input.Attempt.StartedAt, UpdatedAt: *input.Attempt.StartedAt}
	for _, stage := range []string{"before candidate", "candidate created", "candidate healthy", "candidate foreign"} {
		t.Run(stage, func(t *testing.T) {
			id := strings.Repeat("b", 64)
			switch stage {
			case "candidate created":
				add(input.Release.Candidate, id, false)
			case "candidate healthy":
				engine.inspects[id].Container.State.Status, engine.inspects[id].Container.State.Health.Status = container.StateRunning, "healthy"
			case "candidate foreign":
				engine.inspects[id].Container.Config.Labels["com.groundplane.plan-id"] = ids.New(ids.KindPlan)
			}
			observed, err := observer.ObserveRelease(t.Context(), input)
			if err != nil {
				t.Fatal(err)
			}
			ready, readinessErr := releaseexecution.Readiness(input, progress, observed)
			if stage == "candidate foreign" {
				if readinessErr == nil || ready || len(observed.Collisions) != 1 || len(observed.Containers) != 1 {
					t.Fatalf("foreign candidate became serving proof: %v %v", observed, readinessErr)
				}
				return
			}
			wantContainers := 2
			if stage == "before candidate" {
				wantContainers = 1
			}
			if readinessErr != nil || ready != (stage == "candidate healthy") || len(observed.Collisions) != 0 ||
				len(observed.Containers) != wantContainers {
				t.Fatalf(
					"candidate borrowed readiness or dropped predecessor evidence: %v ready=%v error=%v",
					observed,
					ready,
					readinessErr,
				)
			}
		})
	}
}

func nativeObservedReleaseInput(t *testing.T) (releaseexecution.Input, *agentpb.ComposeArtifact) {
	t.Helper()
	at := time.Now().UTC().Add(-time.Second)
	environment, service, plan := ids.New(ids.KindEnvironment), ids.New(ids.KindService), ids.New(ids.KindPlan)
	directory := "/var/lib/groundplane/volumes/" + ids.New(
		ids.KindTenant,
	) + "/" + ids.New(
		ids.KindProject,
	) + "/" + environment
	release, priorRelease := ids.New(ids.KindDeployment), ids.New(ids.KindDeployment)
	artifact := func(name, owner, releaseID string, generation uint64) *agentpb.ComposeArtifact {
		image := "sha256:" + strings.Repeat("c", 64)
		labels := map[string]string{"com.groundplane.environment-id": environment, "com.groundplane.kind": "service",
			"com.groundplane.managed": "true", "com.groundplane.plan-id": owner,
			"com.groundplane.render-generation": strconv.FormatUint(
				generation,
				10,
			), "com.groundplane.service-id": service,
			"com.groundplane.runtime-role": "singleton", "com.groundplane.release-id": releaseID}
		workload := &agentpb.ComposeService{
			ServiceId:        service,
			ComposeName:      name,
			ExpectedReplicas: 1,
			HasHealthcheck:   true,
			Role:             agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
			ImageReference:   image,
		}
		keys := make([]string, 0, len(labels))
		for key := range labels {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			workload.ExpectedLabels = append(workload.ExpectedLabels, &agentpb.LabelPair{Key: key, Value: labels[key]})
		}
		yaml := []byte("services:\n  " + name + ":\n    image: " + image + "\n")
		digest := sha256.Sum256(yaml)
		return &agentpb.ComposeArtifact{
			ArtifactId:          ids.New(ids.KindConfig),
			OwnerKind:           agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
			OwnerId:             environment,
			ProjectName:         "gp-" + strings.ToLower(environment),
			AuthorizedVolumeDir: directory,
			CanonicalYaml:       yaml,
			YamlSha256:          digest[:],
			Services:            []*agentpb.ComposeService{workload},
		}
	}
	candidate, prior := artifact("app", plan, release, 7), artifact("app-prior", ids.New(ids.KindPlan), priorRelease, 6)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(prior)
	if err != nil {
		t.Fatal(err)
	}
	input := releaseexecution.Input{
		Attempt: workexecution.Attempt{ID: ids.New(ids.KindAttempt), WorkItemID: ids.New(ids.KindWorkItem),
			TaskID: ids.New(
				ids.KindTask,
			), Number: 1, Direction: workexecution.Forward, AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Minute),
			Input: workexecution.Input{ID: plan, SHA256: strings.Repeat("d", 64)}, Executor: workexecution.Identity{
				Executor: workexecution.Agent, HostID: ids.New(ids.KindHost), AgentID: ids.New(ids.KindAgent), Generation: 1}},
		Release: &agentpb.ReleaseWorkInput{
			EnvironmentId:    environment,
			ServiceId:        service,
			ReleaseId:        release,
			RenderGeneration: 7,
			Strategy:         agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE,
			Candidate:        candidate,
			Predecessor: &agentpb.ReleaseWorkPredecessor{
				ReleaseId:       priorRelease,
				Target:          "singleton",
				CurrentArtifact: encoded,
			},
		},
	}
	input.Artifacts, err = executionplan.OpenReleaseWorkArtifacts(input.Attempt, input.Release)
	if err != nil {
		t.Fatal(err)
	}
	return input, prior
}
