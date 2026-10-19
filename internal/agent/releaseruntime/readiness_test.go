package releaseruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// SVC-18: fail on the first selected probe-execution error, not the Task timeout.
// Normal startup may converge; a different Service's broken probe is irrelevant.
// The checkpoint is supplied. This tests the real native wait loop, not Docker.
func TestNativeReadinessFailsMissingToolWithoutWaitingForDeadline(t *testing.T) {
	for _, diagnostic := range []agentpb.ComposeHelperDiagnostic{
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_NC_MISSING,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_MISSING,
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_COMMAND_NOT_EXECUTABLE,
	} {
		t.Run(diagnostic.String(), func(t *testing.T) {
			input, report, ready := nativeReadinessBoundary(t, true)
			warming := proto.CloneOf(ready)
			warming.Containers[0].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_STARTING
			warming.Containers[0].HealthcheckDiagnostic = diagnostic
			unrelated := proto.CloneOf(ready.Containers[0])
			unrelated.ContainerId, unrelated.ServiceId = "unrelated", ids.New(ids.KindService)
			unrelated.HealthcheckDiagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING
			warming.Containers = append(warming.Containers, unrelated)
			ready.Containers = append(ready.Containers, proto.CloneOf(unrelated))
			observer := &nativeReadinessObserver{projects: []*agentpb.ObservedProject{warming, ready}}
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			progressCalls := 0
			observed, err := WaitCandidate(ctx, observer, input, report, func(project *agentpb.ObservedProject) {
				progressCalls++
				// UI progress cannot mutate the observation used for readiness.
				project.Containers[0].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE
			})
			if diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED {
				if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) || observer.calls != 1 ||
					ctx.Err() != nil || observed == nil || observed.Containers[0].HealthcheckDiagnostic != diagnostic {
					t.Fatalf(
						"missing-tool failure lost or waited: calls=%d observed=%v error=%v",
						observer.calls,
						observed,
						err,
					)
				}
			} else if err != nil || observer.calls != 2 || observed.Containers[0].Health !=
				agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY {
				t.Fatalf("startup did not converge: calls=%d observed=%v error=%v", observer.calls, observed, err)
			}
			if progressCalls != observer.calls {
				t.Fatal("wait failed to publish its observed progress")
			}
		})
	}
}

// SVC-05/18: only exact captured replicas and images establish readiness.
// Unrelated collisions and old-slot containers must not poison selected work;
// target collisions, stale evidence and duplicate identities must fail closed.
func TestNativeReadinessRequiresExactSelectedReplicas(t *testing.T) {
	input, report, observed := nativeReadinessBoundary(t, true)
	input.Artifacts.Workloads[0].ExpectedReplicas = 2
	second := proto.CloneOf(observed.Containers[0])
	second.ContainerId = "second"
	observed.Containers = append(observed.Containers, second)
	for _, test := range []struct {
		name     string
		mutate   func(*agentpb.ObservedProject)
		ready    bool
		conflict bool
	}{
		{"exact", func(*agentpb.ObservedProject) {}, true, false},
		{"missing replica", func(v *agentpb.ObservedProject) { v.Containers = v.Containers[:1] }, false, false},
		{"unhealthy", func(v *agentpb.ObservedProject) {
			v.Containers[0].Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY
		}, false, false},
		{"stopped", func(v *agentpb.ObservedProject) {
			v.Containers[0].State = agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED
		}, false, false},
		{"wrong image", func(v *agentpb.ObservedProject) { v.Containers[0].ImageReference = "foreign" }, false, true},
		{"wrong labels", func(v *agentpb.ObservedProject) { v.Containers[0].Labels[0].Value = "foreign" }, false, false},
		{"duplicate ID", func(v *agentpb.ObservedProject) { v.Containers[1].ContainerId = v.Containers[0].ContainerId }, false, true},
		{"stale observation", func(v *agentpb.ObservedProject) {
			v.ObservedAt = timestamppb.New(report.Progress.UpdatedAt.Add(-time.Second))
		}, false, true},
		{"wrong project", func(v *agentpb.ObservedProject) { v.ProjectName = "foreign" }, false, true},
		{"selected collision", func(v *agentpb.ObservedProject) {
			v.Collisions = []*agentpb.ObservedCollision{{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
				ComposeServiceName: input.Artifacts.Workloads[0].ComposeName}}
		}, false, true},
		{"unrelated collision", func(v *agentpb.ObservedProject) {
			v.Collisions = []*agentpb.ObservedCollision{{Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK,
				Name: "unrelated-network"}}
		}, true, false},
		{"old slot", func(v *agentpb.ObservedProject) {
			old := proto.CloneOf(v.Containers[0])
			old.ContainerId, old.Labels[0].Value = "old-slot", "older-generation"
			old.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_UNHEALTHY
			v.Containers = append(v.Containers, old)
		}, true, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			project := proto.CloneOf(observed)
			test.mutate(project)
			ready, err := releaseexecution.Readiness(input, report.Progress, project)
			if ready != test.ready || test.conflict != errors.Is(err, errs.New(errs.KindStateConflict, "")) ||
				!test.conflict && err != nil {
				t.Fatalf("ready=%v error=%v", ready, err)
			}
		})
	}
}

// SVC-05: unchecked running singletons are valid for recreate, not blue-green.
func TestNativeReadinessWithoutHealthcheckIsRecreateOnly(t *testing.T) {
	input, report, observed := nativeReadinessBoundary(t, false)
	if ready, err := releaseexecution.Readiness(input, report.Progress, observed); err != nil || !ready {
		t.Fatalf("running unchecked recreate rejected: %v", err)
	}
	input.Release.Strategy = agentpb.WorkReleaseStrategy_WORK_RELEASE_STRATEGY_BLUE_GREEN
	input.Artifacts.Workloads[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
	if ready, err := releaseexecution.Readiness(input, report.Progress, observed); ready ||
		!errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("unchecked slot acquired readiness: ready=%v error=%v", ready, err)
	}
}

func nativeReadinessBoundary(
	t *testing.T,
	healthcheck bool,
) (releaseexecution.Input, releaseexecution.CheckpointReport, *agentpb.ObservedProject) {
	t.Helper()
	supplied := failureHookInput(t, false)
	input := supplied.Input
	input.Attempt = supplied.Forward
	input.Attempt.FinishedAt, input.Attempt.Result, input.Attempt.Outcome = nil, nil, ""
	input.Release.Hooks = nil
	service := input.Artifacts.Workloads[0]
	service.HasHealthcheck = healthcheck
	at := time.Now().UTC()
	report := releaseexecution.CheckpointReport{Sequence: 1, StageID: ids.New(ids.KindStep),
		Progress: releaseexecution.Progress{AttemptID: input.Attempt.ID, Sequence: 1, Phase: releaseexecution.Rollout,
			Action: releaseexecution.WaitCandidate, InFlight: true, BeganAt: &at, UpdatedAt: at}}
	ack, err := report.Checkpoint(input.Attempt)
	if err != nil {
		t.Fatal(err)
	}
	report.Ack = &ack
	if err := report.ValidateEnvelope(input.Attempt); err != nil {
		t.Fatal(err)
	}
	if err := report.Progress.Validate(input); err != nil {
		t.Fatal(err)
	}
	health := agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE
	if healthcheck {
		health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY
	}
	observed := &agentpb.ObservedProject{
		ProjectName: input.Artifacts.Candidate.ProjectName,
		ObservedAt:  timestamppb.New(at),
		Containers: []*agentpb.ObservedContainer{
			{ContainerId: "candidate", Name: service.ComposeName, ServiceId: service.ServiceId,
				ImageReference: service.ImageReference, ImageId: service.ImageReference, Labels: proto.CloneOf(service).ExpectedLabels,
				State: agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING, Health: health},
		},
	}
	return input, report, observed
}

type nativeReadinessObserver struct {
	projects []*agentpb.ObservedProject
	calls    int
}

func (observer *nativeReadinessObserver) ObserveRelease(
	_ context.Context,
	_ releaseexecution.Input,
) (*agentpb.ObservedProject, error) {
	index := min(observer.calls, len(observer.projects)-1)
	observer.calls++
	project := proto.CloneOf(observer.projects[index])
	project.ObservedAt = timestamppb.Now()
	return project, nil
}
