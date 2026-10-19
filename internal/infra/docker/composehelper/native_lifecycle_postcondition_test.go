package composehelper

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SVC-04/05, TASK-73: native physical observations require the exact captured
// replica set, labels and actual image. A successful stop/rm command alone is
// not evidence. Permission/capture decoding is supplied here, not qualified.
func TestNativeLifecycleRequiresExactPhysicalPostcondition(t *testing.T) {
	for _, scenario := range []string{"exact stopped", "partial", "foreign labels", "wrong actual image", "still running", "removed", "rm returned without removing"} {
		t.Run(scenario, func(t *testing.T) {
			input, host := nativeLifecycleObservationFixture(t)
			switch scenario {
			case "partial":
				host.containers = host.containers[:1]
			case "foreign labels":
				host.foreign = true
			case "wrong actual image":
				host.wrongImage = true
			case "still running":
				host.running = true
			case "removed", "rm returned without removing":
				input.Stage.Expected = lifecycleexecution.Absent
				host.keepRemoved = scenario == "rm returned without removing"
			}
			before, err := inspectLifecycleMember(t.Context(), host.runner, input)
			if scenario == "partial" || scenario == "foreign labels" || scenario == "wrong actual image" {
				if err == nil || before != nil {
					t.Fatalf("foreign/incomplete runtime became proof: %+v %v", before, err)
				}
				if err := mutateLifecycleMember(t.Context(), host.runner, input); err == nil || host.stops != 0 ||
					host.removals != 0 {
					t.Fatal("invalid runtime received a mutation")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "exact stopped" {
				if !before.Satisfied || before.Evidence == nil || before.State != string(lifecycleexecution.Stopped) {
					t.Fatal("exact stopped footprint did not produce proof")
				}
				return
			}
			if before.Satisfied || before.Evidence != nil {
				t.Fatal("unfinished effect already had terminal evidence")
			}
			if err := mutateLifecycleMember(t.Context(), host.runner, input); err != nil {
				t.Fatal(err)
			}
			after, err := inspectLifecycleMember(t.Context(), host.runner, input)
			if err != nil {
				t.Fatal(err)
			}
			wantSatisfied := scenario != "rm returned without removing"
			if after.Satisfied != wantSatisfied || (after.Evidence != nil) != wantSatisfied || host.stops != 2 {
				t.Fatalf("command exit replaced physical proof: %+v stops=%d", after, host.stops)
			}
			if input.Stage.Expected == lifecycleexecution.Absent && host.removals != 2 {
				t.Fatal("removal lost a captured replica")
			}
		})
	}
}

// SVC-04: a lost response is reconciled by observation of absence, never another
// destructive command. This does not qualify Agent journal/Controller replay.
func TestNativeLifecycleAbsentInspectionHasNoMutation(t *testing.T) {
	input, host := nativeLifecycleObservationFixture(t)
	input.Stage.Expected, input.InspectOnly = lifecycleexecution.Absent, true
	host.containers = nil
	for range 2 {
		response, err := inspectLifecycleMember(t.Context(), host.runner, input)
		if err != nil || !response.Satisfied || response.Evidence == nil || !response.Inspection || host.stops != 0 ||
			host.removals != 0 {
			t.Fatalf("absence replay acquired effects or lost proof: %+v %v", response, err)
		}
	}
}

type nativeLifecycleObservationHost struct {
	runner                                    *runner.FakeRunner
	containers                                []string
	labels                                    map[string]string
	image                                     string
	running, foreign, wrongImage, keepRemoved bool
	stops, removals                           int
}

func nativeLifecycleObservationFixture(
	t *testing.T,
) (executionplan.LifecycleHelperInput, *nativeLifecycleObservationHost) {
	t.Helper()
	environment, service := ids.New(ids.KindEnvironment), ids.New(ids.KindService)
	stage := lifecycleexecution.Stage{ID: ids.New(ids.KindStep), Member: "api", Expected: lifecycleexecution.Stopped}
	image := "sha256:" + strings.Repeat("c", 64)
	workload := &agentpb.ComposeService{ServiceId: service, ComposeName: "api", ExpectedReplicas: 2,
		Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, ImageReference: image,
		ExpectedLabels: []*agentpb.LabelPair{{Key: "com.groundplane.release-id", Value: ids.New(ids.KindDeployment)}}}
	artifact := &agentpb.ComposeArtifact{
		OwnerId:     environment,
		ProjectName: "gp-" + strings.ToLower(environment),
		Services:    []*agentpb.ComposeService{workload},
	}
	input := executionplan.LifecycleHelperInput{Attempt: workexecution.Attempt{ID: ids.New(ids.KindAttempt),
		Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)}},
		Stage: stage, Procedure: &agentpb.ServiceLifecycleWorkInput{EnvironmentId: environment, Members: []*agentpb.ServiceLifecycleMember{{
			Artifact: artifact, Ownership: &agentpb.ServiceLifecycleSource{StepId: stage.ID, ComposeNames: []string{"api"}}}}}}
	host := &nativeLifecycleObservationHost{runner: runner.NewFake(), image: image,
		containers: []string{strings.Repeat("a", 64), strings.Repeat("b", 64)}, labels: map[string]string{
			"com.docker.compose.project": artifact.ProjectName, "com.docker.compose.service": "api",
			"com.groundplane.managed": "true", "com.groundplane.environment-id": environment, "com.groundplane.service-id": service,
			"com.groundplane.release-id": workload.ExpectedLabels[0].Value}}
	host.runner.RunFunc = func(_ context.Context, command runner.RunCmdOpts) (runner.Result, error) {
		args := command.Args
		if command.Name != DockerExecutable || len(args) < 2 || args[0] != "container" {
			t.Fatalf("unexpected host command: %+v", command)
		}
		var output []byte
		switch args[1] {
		case "ls":
			output = []byte(strings.Join(host.containers, "\n"))
		case "inspect":
			if len(args) != 5 {
				t.Fatalf("unexpected inspect: %v", args)
			}
			if !slices.Contains(host.containers, args[4]) {
				t.Fatal("observed an unselected container")
			}
			switch args[3] {
			case "{{json .Config.Labels}}":
				labels := make(map[string]string, len(host.labels))
				for key, value := range host.labels {
					labels[key] = value
				}
				if host.foreign {
					labels["com.groundplane.release-id"] = ids.New(ids.KindDeployment)
				}
				var err error
				output, err = json.Marshal(labels)
				if err != nil {
					t.Fatal(err)
				}
			case "{{.Config.Image}}\n{{.Image}}":
				actual := host.image
				if host.wrongImage {
					actual = "sha256:" + strings.Repeat("d", 64)
				}
				output = []byte(host.image + "\n" + actual)
			case "{{json .State}}":
				var err error
				output, err = json.Marshal(map[string]bool{
					"Running": host.running, "Paused": false, "Restarting": false, "Dead": false,
				})
				if err != nil {
					t.Fatal(err)
				}
			default:
				t.Fatalf("unexpected inspection: %v", args)
			}
		case "stop":
			if !slices.Contains(host.containers, args[len(args)-1]) {
				t.Fatal("stopped an unselected container")
			}
			host.stops++
			host.running = false
		case "rm":
			id := args[len(args)-1]
			if !slices.Contains(host.containers, id) {
				t.Fatal("removed an unselected container")
			}
			host.removals++
			if !host.keepRemoved {
				host.containers = slices.DeleteFunc(host.containers, func(value string) bool { return value == id })
			}
		default:
			t.Fatalf("unexpected mutation: %v", args)
		}
		return runner.Result{Stdout: output}, nil
	}
	return input, host
}
