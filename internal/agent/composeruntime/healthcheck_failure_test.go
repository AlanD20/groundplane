package composeruntime

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type healthcheckObserver struct {
	projects []*agentpb.ObservedProject
	calls    int
}

func (observer *healthcheckObserver) Observe(
	context.Context,
	*agentpb.ExecutionPlan,
	string,
) (*agentpb.ObservedProject, error) {
	index := observer.calls
	observer.calls++
	if index >= len(observer.projects) {
		index = len(observer.projects) - 1
	}
	return observer.projects[index], nil
}

func (observer *healthcheckObserver) ObserveRestoration(
	ctx context.Context,
	_ *executionplan.RestorationObservation,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, nil, "")
}

func (observer *healthcheckObserver) ObserveReleaseRestoration(
	ctx context.Context,
	_ *agentpb.ExecutionPlan,
	_, _ string,
) (*agentpb.ObservedProject, error) {
	return observer.Observe(ctx, nil, "")
}

// SVC-18: both health-wait paths must fail on the first observed missing tool,
// retain its diagnostic for Task recovery, and still allow startup to converge.
func TestHealthWaitFailsMissingExecutableWithoutWaitingForDeadline(t *testing.T) {
	for _, workload := range []bool{false, true} {
		for _, missing := range []bool{false, true} {
			t.Run(testHealthWaitName(workload, missing), func(t *testing.T) {
				artifact := convergenceArtifact()
				artifact.Services[0].ExpectedReplicas = 1
				artifact.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT
				artifact.Services[0].Slot = "blue"
				warming := healthyContainer("api-1", "svc_api")
				warming.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_STARTING
				if missing {
					warming.HealthcheckDiagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_PGREP_MISSING
				}
				unrelated := healthyContainer("worker-1", "svc_worker")
				unrelated.HealthcheckDiagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_HEALTHCHECK_CURL_MISSING
				observer := &healthcheckObserver{projects: []*agentpb.ObservedProject{
					{ProjectName: artifact.ProjectName, Containers: []*agentpb.ObservedContainer{warming, unrelated}},
					{
						ProjectName: artifact.ProjectName,
						Containers:  []*agentpb.ObservedContainer{healthyContainer("api-1", "svc_api"), unrelated},
					},
				}}
				runtime, err := New(cancellationHelper{}, observer)
				if err != nil {
					t.Fatal(err)
				}
				plan := &agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{artifact}}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				var result StepResult
				if workload {
					result, err = runtime.waitWorkloadHealthy(
						ctx,
						plan,
						&agentpb.WaitWorkloadHealthy{
							ArtifactId: artifact.ArtifactId,
							ServiceId:  "svc_api",
							Target:     "blue",
						},
						nil,
					)
				} else {
					result, err = runtime.waitHealthy(ctx, plan, &agentpb.WaitHealthy{ArtifactId: artifact.ArtifactId, ServiceIds: []string{"svc_api"}}, nil)
				}
				if missing {
					if err == nil || ctx.Err() != nil || observer.calls != 1 || !result.ReconciliationRequired ||
						result.Diagnostic != warming.HealthcheckDiagnostic {
						t.Fatalf(
							"missing executable: calls=%d result=%v error=%v deadline=%v",
							observer.calls,
							result,
							err,
							ctx.Err(),
						)
					}
				} else if err != nil || observer.calls != 2 || result.ReconciliationRequired {
					t.Fatalf("startup convergence: calls=%d result=%v error=%v", observer.calls, result, err)
				}
			})
		}
	}
}

func testHealthWaitName(workload, missing bool) string {
	name := "project"
	if workload {
		name = "workload"
	}
	if missing {
		return name + "/missing"
	}
	return name + "/startup"
}
