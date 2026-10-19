package composehelper

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SVC-09/15, TASK-95: drive the native recovery operation, not the removed
// generic compensation dispatcher. The independent Docker model must actually
// lose the exact failed candidate before recovery can prove absence. Earlier
// configuration/quiescence receipts are supplied, not qualified by this case.
func TestNativeRecoveryCandidateRemovalRequiresExactAbsence(t *testing.T) {
	for _, scenario := range []string{"remove", "already absent", "inspection only", "foreign name owner", "foreign actual image", "remove failed", "remove without effect"} {
		t.Run(scenario, func(t *testing.T) {
			input := nativeAbsentRecoveryInput(t)
			report := nativeRecoveryReport(t, input, releaseexecution.RemoveFailedCandidate)
			host := nativeRecoveryRemovalHost(t, input)
			host.present = scenario != "already absent"
			host.foreign = scenario == "foreign name owner"
			host.wrongImage = scenario == "foreign actual image"
			host.failRemove = scenario == "remove failed"
			host.keepRemoved = scenario == "remove without effect"
			inspection := scenario == "inspection only"
			err := RemoveRecoveryCandidate(t.Context(), host.runner, input, report, inspection)
			wantSuccess := scenario == "remove" || scenario == "already absent"
			if wantSuccess != (err == nil) || wantSuccess && host.present {
				t.Fatalf(
					"unproved removal: present=%v stops=%d removals=%d error=%v",
					host.present,
					host.stops,
					host.removals,
					err,
				)
			}
			if scenario != "remove" && scenario != "remove failed" && scenario != "remove without effect" &&
				(host.stops != 0 || host.removals != 0) {
				t.Fatal("inspection, absence or foreign runtime acquired mutation authority")
			}
			if scenario == "remove" {
				if host.stops != 1 || host.removals != 1 {
					t.Fatal("exact candidate was not stopped and removed once")
				}
				// Lost response: the same captured removal is now inspection only.
				if err := RemoveRecoveryCandidate(t.Context(), host.runner, input, report, true); err != nil ||
					host.stops != 1 || host.removals != 1 {
					t.Fatalf("lost-response inspection repeated effects: %v", err)
				}
				verified := nativeRecoveryReport(t, input, releaseexecution.VerifyRestoration)
				if err := VerifyRecoveryRuntime(t.Context(), host.runner, input, verified); err != nil {
					t.Fatalf("independent final absence verification: %v", err)
				}
			}
		})
	}
}

func nativeAbsentRecoveryInput(t *testing.T) releaseexecution.RecoveryInput {
	t.Helper()
	at := time.Now().UTC().Add(-time.Minute)
	forward := workexecution.Attempt{ID: ids.New(ids.KindAttempt), TaskID: ids.New(ids.KindTask),
		WorkItemID: ids.New(ids.KindWorkItem), Number: 1, Direction: workexecution.Forward,
		AssignedAt: at, StartedAt: &at, Deadline: at.Add(time.Hour),
		Input: workexecution.Input{ID: ids.New(ids.KindPlan), SHA256: strings.Repeat("a", 64)},
		Executor: workexecution.Identity{Executor: workexecution.Agent, HostID: ids.New(ids.KindHost),
			AgentID: ids.New(ids.KindAgent), Generation: 1}}
	environmentID, serviceID, releaseID := ids.New(
		ids.KindEnvironment,
	), ids.New(
		ids.KindService,
	), ids.New(
		ids.KindDeployment,
	)
	image := "sha256:" + strings.Repeat("b", 64)
	labels := map[string]string{"com.groundplane.environment-id": environmentID,
		"com.groundplane.service-id": serviceID, "com.groundplane.release-id": releaseID,
		"com.groundplane.plan-id": forward.Input.ID, "com.groundplane.render-generation": "1",
		"com.groundplane.runtime-role": "singleton", "com.groundplane.managed": "true", "com.groundplane.kind": "service"}
	service := &agentpb.ComposeService{ServiceId: serviceID, ComposeName: "app", ImageReference: image,
		ExpectedReplicas: 1, Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON}
	keys := make([]string, 0, len(labels))
	for key := range labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		service.ExpectedLabels = append(service.ExpectedLabels, &agentpb.LabelPair{Key: key, Value: labels[key]})
	}
	yaml := []byte("services:\n  app:\n    image: " + image + "\n    network_mode: none\n")
	digest := sha256.Sum256(yaml)
	directory, err := environmentpath.Derive(environmentpath.DefaultVolumeRoot, environmentpath.Scope{
		ProjectKind: environmentpath.ProjectKindTenant, TenantID: ids.New(ids.KindTenant),
		ProjectID: ids.New(ids.KindProject), EnvironmentID: environmentID})
	if err != nil {
		t.Fatal(err)
	}
	artifact := &agentpb.ComposeArtifact{ArtifactId: ids.New(ids.KindConfig), OwnerId: environmentID,
		OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		ProjectName: "gp-" + strings.ToLower(environmentID), CanonicalYaml: yaml, YamlSha256: digest[:],
		AuthorizedVolumeDir: directory, Services: []*agentpb.ComposeService{service}}
	failedAt := at.Add(time.Second)
	failure := releaseexecution.Failure{AttemptID: forward.ID, Input: forward.Input,
		Reason: releaseexecution.ExecutionFailed, RecordedAt: failedAt}
	_, result, err := releaseexecution.EncodeFailure(forward, failure)
	if err != nil {
		t.Fatal(err)
	}
	forward.FinishedAt, forward.Result, forward.Outcome = &failedAt, &result, workexecution.Failed
	recovery := forward
	started := failedAt.Add(time.Second)
	recovery.ID, recovery.Number, recovery.Direction = ids.New(ids.KindAttempt), 2, workexecution.Recovery
	recovery.AssignedAt, recovery.StartedAt = started, &started
	recovery.FinishedAt, recovery.Result, recovery.Outcome = nil, nil, ""
	input := releaseexecution.RecoveryInput{Forward: forward, Failure: failure,
		Disposition: agentpb.WorkReleaseRecoveryDisposition_WORK_RELEASE_RECOVERY_DISPOSITION_RESTORE_CAPTURED,
		Input: releaseexecution.Input{Attempt: recovery, Release: &agentpb.ReleaseWorkInput{
			EnvironmentId: environmentID, ServiceId: serviceID, ReleaseId: releaseID, RenderGeneration: 1,
			Candidate: artifact, Strategy: agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_RECREATE,
			Predecessor: &agentpb.ReleaseWorkPredecessor{Absent: true}}}}
	input.Input.Artifacts, err = executionplan.OpenReleaseWorkArtifacts(recovery, input.Input.Release)
	if err != nil || input.Validate() != nil {
		t.Fatalf("native captured recovery fixture: %v", err)
	}
	return input
}

func nativeRecoveryReport(
	t *testing.T,
	input releaseexecution.RecoveryInput,
	phase releaseexecution.RecoveryPhase,
) releaseexecution.RecoveryReport {
	t.Helper()
	at := *input.Input.Attempt.StartedAt
	progress, err := releaseexecution.NewRecoveryProgress(input, at)
	if err != nil {
		t.Fatal(err)
	}
	for {
		progress, err = progress.Begin(input, at)
		if err != nil {
			t.Fatal(err)
		}
		if progress.Phase == phase {
			break
		}
		// Earlier effects are supplied receipts. This does not exercise their
		// physical implementations or Controller admission.
		progress, err = progress.Confirm(input, input.Input.Attempt.Input, at)
		if err != nil {
			t.Fatal(err)
		}
	}
	stageID, err := progress.StageID(input)
	if err != nil {
		t.Fatal(err)
	}
	report := releaseexecution.RecoveryReport{Sequence: progress.Sequence, StageID: stageID, Progress: progress}
	ack, err := report.Checkpoint(input)
	if err != nil {
		t.Fatal(err)
	}
	report.Ack = &ack
	return report
}

type nativeRecoveryDocker struct {
	runner                                                *runner.FakeRunner
	present, foreign, wrongImage, failRemove, keepRemoved bool
	stops, removals                                       int
}

func nativeRecoveryRemovalHost(t *testing.T, input releaseexecution.RecoveryInput) *nativeRecoveryDocker {
	t.Helper()
	host := &nativeRecoveryDocker{runner: runner.NewFake()}
	artifact, service := input.Input.Release.Candidate, input.Input.Release.Candidate.Services[0]
	const candidateID, unrelatedID = "bbbbbbbbbbbbbbbb", "cccccccccccccccc"
	host.runner.RunFunc = func(_ context.Context, command runner.RunCmdOpts) (runner.Result, error) {
		args := command.Args
		if command.Name != DockerExecutable || len(args) < 2 || args[0] != "container" {
			t.Fatalf("recovery exceeded container scope: %+v", command)
		}
		switch args[1] {
		case "ls":
			containers := []string{unrelatedID}
			if host.present {
				containers = append(containers, candidateID)
			}
			return runner.Result{Stdout: []byte(strings.Join(containers, "\n"))}, nil
		case "inspect":
			id := args[len(args)-1]
			if args[3] == "{{json .Config.Labels}}" {
				labels := map[string]string{"com.docker.compose.project": artifact.ProjectName}
				if id == unrelatedID {
					labels["com.docker.compose.service"], labels["com.groundplane.service-id"] = "worker", "another-service"
				} else {
					for _, label := range service.ExpectedLabels {
						labels[label.Key] = label.Value
					}
					labels["com.docker.compose.service"] = service.ComposeName
					if host.foreign {
						labels["com.groundplane.service-id"] = "another-service"
					}
				}
				raw, err := json.Marshal(labels)
				return runner.Result{Stdout: raw}, err
			}
			if id != candidateID || args[3] != "{{.Config.Image}}\n{{.Image}}" {
				t.Fatalf("inspected beyond captured candidate: %v", args)
			}
			actual := service.ImageReference
			if host.wrongImage {
				actual = "sha256:" + strings.Repeat("f", 64)
			}
			return runner.Result{Stdout: []byte(service.ImageReference + "\n" + actual)}, nil
		case "stop":
			if !slices.Equal(args, []string{"container", "stop", "--time", "30", "--", candidateID}) {
				t.Fatalf("stopped another runtime: %v", args)
			}
			host.stops++
		case "rm":
			if !slices.Equal(args, []string{"container", "rm", "--", candidateID}) {
				t.Fatalf("removed another runtime: %v", args)
			}
			host.removals++
			if host.failRemove {
				return runner.Result{ExitCode: 1}, nil
			}
			if !host.keepRemoved {
				host.present = false
			}
		default:
			t.Fatalf("unexpected recovery command: %v", args)
		}
		return runner.Result{}, nil
	}
	return host
}
