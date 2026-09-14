package executionplan

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

const (
	candidateRuntimeEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimeServiceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimeOtherService  = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	candidateRuntimeReleaseID     = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimePriorRelease  = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	candidateRuntimeArtifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimePriorArtifact = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	candidateRuntimePlanID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	candidateRuntimePriorPlanID   = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

// Rationale: SVC-15/H41 requires an ordinary Deploy or Rollback receipt to
// keep the unchanged stable proxy's acknowledged ownership and resources while
// recording the candidate workload and exact post-switch configuration.
func TestPrepareCandidateRuntimesBlueGreenPostActivation(t *testing.T) {
	for _, operation := range []agentpb.PlanOperation{
		agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK,
	} {
		t.Run(operation.String(), func(t *testing.T) {
			plan := candidateRuntimeBlueGreenPlan(t, true)
			plan.PlanHash = nil
			plan.Operation = operation
			prior := plan.GetArtifacts()[1]
			prior.CanonicalYaml = bytes.ReplaceAll(
				prior.GetCanonicalYaml(), []byte("gp-proxy-runtime"), []byte("gp-proxy-prior"),
			)
			prior.CanonicalYaml = bytes.Replace(
				prior.GetCanonicalYaml(), []byte("    image: proxy:sealed\n"),
				[]byte("    image: proxy:sealed\n    x-runtime-source: acknowledged\n"), 1,
			)
			priorDigest := sha256.Sum256(prior.GetCanonicalYaml())
			prior.YamlSha256 = priorDigest[:]
			plan, err := Seal(plan)
			if err != nil {
				t.Fatalf("Seal(%s runtime fixture) error = %v", operation, err)
			}
			original := proto.CloneOf(plan)
			switchStep := plan.GetSteps()[2].GetServiceProxySwitch()
			preSwitch := slices.Clone(plan.GetArtifacts()[0].GetServices()[0].GetProxyConfigJson())

			got, err := PrepareCandidateRuntimes(plan)
			if err != nil {
				t.Fatalf("PrepareCandidateRuntimes() error = %v", err)
			}
			if !proto.Equal(plan, original) {
				t.Fatal("PrepareCandidateRuntimes() mutated its sealed input")
			}
			if len(got) != 1 || got[0].ServiceID != candidateRuntimeServiceID ||
				got[0].ReleaseID != candidateRuntimeReleaseID || got[0].Target != "blue" ||
				got[0].ProxyGeneration != switchStep.GetProxyGeneration() ||
				!bytes.Equal(got[0].ProxyConfigSHA256, switchStep.GetConfigSha256()) {
				t.Fatalf("prepared runtime = %#v", got)
			}
			current := candidateRuntimeOpenArtifact(t, got[0].CurrentArtifact)
			if current.GetArtifactId() != candidateRuntimeArtifactID || len(current.GetServices()) != 2 {
				t.Fatalf("current artifact selection = %#v", current.GetServices())
			}
			wantWorkload := candidateRuntimeService(
				plan.GetArtifacts()[0],
				candidateRuntimeServiceID,
				"blue",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			)
			gotWorkload := candidateRuntimeService(
				current,
				candidateRuntimeServiceID,
				"blue",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
			)
			gotProxy := candidateRuntimeService(
				current,
				candidateRuntimeServiceID,
				"",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			)
			wantProxy := proto.CloneOf(candidateRuntimeService(
				plan.GetArtifacts()[1],
				candidateRuntimeServiceID,
				"",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			))
			wantProxy.ProxyConfigJson = slices.Clone(switchStep.GetConfigJson())
			wantProxy.ProxyConfigSha256 = slices.Clone(switchStep.GetConfigSha256())
			if !proto.Equal(gotWorkload, wantWorkload) || !proto.Equal(gotProxy, wantProxy) {
				t.Fatal("current artifact changed candidate workload or existing proxy ownership")
			}
			if bytes.Contains(current.GetCanonicalYaml(), preSwitch) ||
				!candidateRuntimeYAMLConfigEquals(
					t,
					current.GetCanonicalYaml(),
					"gp-proxy-prior",
					switchStep.GetConfigJson(),
				) {
				t.Fatalf("current YAML did not replace pre-switch proxy config:\n%s", current.GetCanonicalYaml())
			}
			if !bytes.Contains(current.GetCanonicalYaml(), []byte("x-runtime-source: acknowledged")) ||
				bytes.Contains(current.GetCanonicalYaml(), []byte("gp-proxy-runtime")) {
				t.Fatalf("current YAML did not retain acknowledged proxy resources:\n%s", current.GetCanonicalYaml())
			}
			for _, unwanted := range []string{"api--green:", "worker:", "unrelated-data:", "unrelated-config:"} {
				if bytes.Contains(current.GetCanonicalYaml(), []byte(unwanted)) {
					t.Fatalf("current runtime retained unrelated %q:\n%s", unwanted, current.GetCanonicalYaml())
				}
			}
			for _, wanted := range []string{"shared:", "api-data:", "app-config:", "api-secret:"} {
				if !bytes.Contains(current.GetCanonicalYaml(), []byte(wanted)) {
					t.Fatalf("current runtime lost referenced %q:\n%s", wanted, current.GetCanonicalYaml())
				}
			}

			retained := candidateRuntimeOpenArtifact(t, got[0].RetainedPriorArtifact)
			if retained.GetArtifactId() != candidateRuntimePriorArtifact || len(retained.GetServices()) != 1 ||
				candidateRuntimeService(
					retained,
					candidateRuntimeServiceID,
					"green",
					agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
				) == nil || candidateRuntimeService(
				retained,
				candidateRuntimeServiceID,
				"",
				agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			) != nil {
				t.Fatalf("retained artifact = %#v", retained)
			}
			if err := ValidateNativePredecessorWitness(
				candidateRuntimeEnvironmentID,
				candidateRuntimeServiceID,
				got[0].CurrentArtifact,
				got[0].RetainedPriorArtifact,
			); err != nil {
				t.Fatalf("prepared runtime is not a native witness: %v", err)
			}
			repeated, err := PrepareCandidateRuntimes(plan)
			if err != nil || !candidateRuntimeEqual(got[0], repeated[0]) {
				t.Fatalf("repeated derivation diverged: %v", err)
			}
		})
	}
}

// Rationale: SVC-15/H41 must not invent a prior proxy for the first explicit
// Deploy after configured-only Apply; EnsureProxy proves that Compose creates
// the candidate proxy, whose new ownership and switched config are acknowledged.
func TestPrepareCandidateRuntimesFirstBlueGreenAppliesCandidateProxy(t *testing.T) {
	plan := candidateRuntimeBlueGreenPlan(t, false)
	original := proto.CloneOf(plan)
	switchStep := plan.GetSteps()[2].GetServiceProxySwitch()
	apply := plan.GetSteps()[0].GetComposeWorkloadApply()
	if !apply.GetEnsureProxy() {
		t.Fatal("first Deploy fixture does not apply its candidate proxy")
	}

	got, err := PrepareCandidateRuntimes(plan)
	if err != nil {
		t.Fatalf("PrepareCandidateRuntimes() error = %v", err)
	}
	if !proto.Equal(plan, original) {
		t.Fatal("PrepareCandidateRuntimes() mutated its sealed first-Deploy input")
	}
	current := candidateRuntimeOpenArtifact(t, got[0].CurrentArtifact)
	gotProxy := candidateRuntimeService(
		current,
		candidateRuntimeServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	)
	wantProxy := proto.CloneOf(candidateRuntimeService(
		plan.GetArtifacts()[0],
		candidateRuntimeServiceID,
		"",
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	))
	wantProxy.ProxyConfigJson = slices.Clone(switchStep.GetConfigJson())
	wantProxy.ProxyConfigSha256 = slices.Clone(switchStep.GetConfigSha256())
	if !proto.Equal(gotProxy, wantProxy) || !candidateRuntimeYAMLConfigEquals(
		t,
		current.GetCanonicalYaml(),
		"gp-proxy-runtime",
		switchStep.GetConfigJson(),
	) {
		t.Fatal("first Deploy receipt did not retain the applied candidate proxy and switched config")
	}
}

// Rationale: SVC-15/H41 cannot describe one real post-switch runtime when the
// retained proxy and candidate workload give a shared Compose resource two
// identities; publication must fail closed instead of fabricating a receipt.
func TestPrepareCandidateRuntimesRejectsRetainedProxyResourceConflict(t *testing.T) {
	plan := proto.CloneOf(candidateRuntimeBlueGreenPlan(t, true))
	plan.PlanHash = nil
	prior := plan.GetArtifacts()[1]
	prior.CanonicalYaml = bytes.Replace(
		prior.GetCanonicalYaml(),
		[]byte("  shared:\n    external: true\n"),
		[]byte("  shared:\n    driver: bridge\n"),
		1,
	)
	digest := sha256.Sum256(prior.GetCanonicalYaml())
	prior.YamlSha256 = digest[:]
	plan, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(conflicting retained proxy fixture) error = %v", err)
	}
	if _, err := PrepareCandidateRuntimes(plan); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("PrepareCandidateRuntimes() error = %v, want validation.failed", err)
	}
}

// Rationale: a portless recreate member has no proxy activation evidence; its
// exact ComposeApply, health wait, and recreate acknowledgement still select
// one singleton runtime with zero proxy metadata.
func TestPrepareCandidateRuntimesPortlessRecreate(t *testing.T) {
	plan := candidateRuntimeRecreatePlan(t)
	got, err := PrepareCandidateRuntimes(plan)
	if err != nil {
		t.Fatalf("PrepareCandidateRuntimes() error = %v", err)
	}
	if len(got) != 1 || got[0].Target != "singleton" || got[0].ProxyGeneration != 0 ||
		len(got[0].ProxyConfigSHA256) != 0 || len(got[0].RetainedPriorArtifact) != 0 {
		t.Fatalf("portless recreate runtime = %#v", got)
	}
	current := candidateRuntimeOpenArtifact(t, got[0].CurrentArtifact)
	if len(current.GetServices()) != 1 || current.GetServices()[0].GetRole() !=
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON ||
		current.GetServices()[0].GetComposeName() != "api" ||
		bytes.Contains(current.GetCanonicalYaml(), []byte("worker:")) {
		t.Fatalf("portless recreate selection = %#v\n%s", current.GetServices(), current.GetCanonicalYaml())
	}
	if err := ValidateNativePredecessorWitness(
		candidateRuntimeEnvironmentID,
		candidateRuntimeServiceID,
		got[0].CurrentArtifact,
		nil,
	); err != nil {
		t.Fatalf("portless recreate runtime is not a native witness: %v", err)
	}
}

// Rationale: publication may consume only a fully sealed ordinary Release
// whose forward anchors and proxy YAML agree with its exact activation; it
// must not invent a runtime from malformed, unrelated, or mismatched input.
func TestPrepareCandidateRuntimesRejectsInvalidAuthority(t *testing.T) {
	valid := candidateRuntimeBlueGreenPlan(t, false)
	tests := []struct {
		name string
		plan func(*testing.T) *agentpb.ExecutionPlan
	}{
		{name: "nil", plan: func(*testing.T) *agentpb.ExecutionPlan { return nil }},
		{name: "unsealed", plan: func(*testing.T) *agentpb.ExecutionPlan {
			value := proto.CloneOf(valid)
			value.PlanHash = nil
			return value
		}},
		{name: "Blueprint", plan: func(t *testing.T) *agentpb.ExecutionPlan {
			value, err := Seal(validBlueprintScriptReconcilePlan(t))
			if err != nil {
				t.Fatal(err)
			}
			return value
		}},
		{name: "mismatched wait target", plan: func(t *testing.T) *agentpb.ExecutionPlan {
			value := proto.CloneOf(valid)
			value.PlanHash = nil
			value.Steps[1].GetWaitWorkloadHealthy().Target = "green"
			sealed, err := Seal(value)
			if err != nil {
				t.Fatalf("seal independently valid mismatched wait: %v", err)
			}
			return sealed
		}},
		{name: "proxy YAML differs from metadata", plan: func(t *testing.T) *agentpb.ExecutionPlan {
			value := proto.CloneOf(valid)
			value.PlanHash = nil
			artifact := value.Artifacts[0]
			oldContent := []byte("content: " + strconv.Quote(string(artifact.Services[0].ProxyConfigJson)))
			artifact.CanonicalYaml = bytes.Replace(
				artifact.CanonicalYaml,
				oldContent,
				[]byte("content: \"{\\\"different\\\":true}\""),
				1,
			)
			digest := sha256.Sum256(artifact.CanonicalYaml)
			artifact.YamlSha256 = digest[:]
			sealed, err := Seal(value)
			if err != nil {
				t.Fatalf("seal independently valid mismatched YAML: %v", err)
			}
			return sealed
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := PrepareCandidateRuntimes(test.plan(t)); !errors.Is(
				err,
				errs.New(errs.KindValidationFailed, ""),
			) {
				t.Fatalf("PrepareCandidateRuntimes() error = %v, want validation.failed", err)
			}
		})
	}
}

func candidateRuntimeBlueGreenPlan(t *testing.T, withPrior bool) *agentpb.ExecutionPlan {
	t.Helper()
	preTarget, preRelease, preGeneration := "singleton", candidateRuntimeReleaseID, uint64(6)
	if withPrior {
		preTarget, preRelease = "green", candidateRuntimePriorRelease
	}
	preConfig := candidateRuntimeProxyConfig(preRelease, preTarget, preGeneration)
	postConfig := candidateRuntimeProxyConfig(candidateRuntimeReleaseID, "blue", 7)
	candidate := candidateRuntimeFixtureArtifact(
		t,
		candidateRuntimeArtifactID,
		candidateRuntimePlanID,
		7,
		candidateRuntimeReleaseID,
		"blue",
		preConfig,
		true,
	)
	plan := &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: candidateRuntimePlanID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: candidateRuntimeServiceID,
		Artifacts: []*agentpb.ComposeArtifact{candidate},
	}
	priorArtifactID := ""
	if withPrior {
		prior := candidateRuntimeFixtureArtifact(
			t,
			candidateRuntimePriorArtifact,
			candidateRuntimePriorPlanID,
			6,
			candidateRuntimePriorRelease,
			"green",
			preConfig,
			false,
		)
		plan.Artifacts = append(plan.Artifacts, prior)
		priorArtifactID = candidateRuntimePriorArtifact
	}
	postDigest := sha256.Sum256(postConfig)
	plan.Steps = []*agentpb.ExecutionStep{
		{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeWorkloadApply{ComposeWorkloadApply: &agentpb.ComposeWorkloadApply{
				ArtifactId: candidateRuntimeArtifactID, ServiceId: candidateRuntimeServiceID, Target: "blue",
				EnsureProxy: !withPrior,
			}},
		},
		{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_WaitWorkloadHealthy{WaitWorkloadHealthy: &agentpb.WaitWorkloadHealthy{
				ArtifactId: candidateRuntimeArtifactID, ServiceId: candidateRuntimeServiceID, Target: "blue",
			}},
		},
		{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAX", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ServiceProxySwitch{ServiceProxySwitch: &agentpb.ServiceProxySwitch{
				CandidateArtifactId: candidateRuntimeArtifactID, PriorArtifactId: priorArtifactID,
				ServiceId: candidateRuntimeServiceID, FromTarget: preTarget, ToTarget: "blue",
				ProxyGeneration: 7, ConfigJson: postConfig, ConfigSha256: postDigest[:],
				ReleaseId: candidateRuntimeReleaseID,
			}},
		},
	}
	for _, step := range plan.Steps {
		step.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
	}
	probeID, compensateID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAY", "step_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
	member := &agentpb.CandidateReleaseMember{
		ServiceId: candidateRuntimeServiceID, CandidateReleaseId: candidateRuntimeReleaseID,
		CandidateArtifactId: candidateRuntimeArtifactID,
		ForwardStepIds: []string{
			plan.Steps[0].GetStepId(),
			plan.Steps[1].GetStepId(),
			plan.Steps[2].GetStepId(),
		},
	}
	if !withPrior {
		member.CandidateAbsence = &agentpb.CandidateAbsenceRestoration{
			ComposeProjectName: candidate.GetProjectName(), ProbeStepId: probeID, CompensateStepId: compensateID,
			Services: []*agentpb.CandidateReleaseService{{
				ServiceId: candidateRuntimeServiceID, ReleaseId: candidateRuntimeReleaseID,
			}},
		}
		plan.Steps = append(plan.Steps, candidateRuntimeAbsenceSteps(probeID, compensateID)...)
	} else {
		preDigest := sha256.Sum256(preConfig)
		member.ServingPredecessor = &agentpb.ServingPredecessorRestoration{
			ProbeStepId: probeID, CompensateStepId: compensateID,
			PriorArtifactId: candidateRuntimePriorArtifact,
			PriorReleaseId:  candidateRuntimePriorRelease,
			PriorTarget:     "green",
		}
		plan.Steps = append(plan.Steps,
			&agentpb.ExecutionStep{
				StepId: probeID, TimeoutSeconds: 30,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_ServiceProxyProbe{ServiceProxyProbe: &agentpb.ServiceProxyProbe{
					CandidateArtifactId:      candidateRuntimeArtifactID,
					PriorArtifactId:          candidateRuntimePriorArtifact,
					ServiceId:                candidateRuntimeServiceID,
					ExpectedTarget:           "green",
					ProxyGeneration:          preGeneration,
					ConfigJson:               preConfig,
					ConfigSha256:             preDigest[:],
					ReleaseId:                candidateRuntimePriorRelease,
					AlternateTarget:          "blue",
					AlternateProxyGeneration: 7,
					AlternateConfigJson:      postConfig,
					AlternateConfigSha256:    postDigest[:],
					AlternateReleaseId:       candidateRuntimeReleaseID,
				}},
			},
			&agentpb.ExecutionStep{
				StepId: compensateID, TimeoutSeconds: 30,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				Payload: &agentpb.ExecutionStep_ServiceProxyCompensate{ServiceProxyCompensate: &agentpb.ServiceProxyCompensate{
					CandidateArtifactId: candidateRuntimeArtifactID,
					PriorArtifactId:     candidateRuntimePriorArtifact,
					ServiceId:           candidateRuntimeServiceID,
					CandidateTarget:     "blue",
					PriorTarget:         "green",
					ProxyGeneration:     preGeneration,
					ConfigJson:          preConfig,
					ConfigSha256:        preDigest[:],
					PriorReleaseId:      candidateRuntimePriorRelease,
					Enabled:             true,
				}},
			},
		)
	}
	plan.CandidateReleaseProcedure = &agentpb.CandidateReleaseProcedure{
		Members: []*agentpb.CandidateReleaseMember{member},
	}
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(blue-green runtime fixture) error = %v", err)
	}
	return sealed
}

func candidateRuntimeRecreatePlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	artifact := candidateRuntimeFixtureArtifact(
		t,
		candidateRuntimeArtifactID,
		candidateRuntimePlanID,
		7,
		candidateRuntimeReleaseID,
		"singleton",
		nil,
		true,
	)
	applyID, waitID, acknowledgeID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", "step_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	probeID, compensateID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAY", "step_01ARZ3NDEKTSV4RRFFQ69G5FAZ"
	steps := []*agentpb.ExecutionStep{
		{
			StepId: applyID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: candidateRuntimeArtifactID, ServiceIds: []string{candidateRuntimeServiceID},
				ForceRecreate: true, NoDependencies: true,
			}},
		},
		{
			StepId: waitID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_WaitHealthy{WaitHealthy: &agentpb.WaitHealthy{
				ArtifactId: candidateRuntimeArtifactID, ServiceIds: []string{candidateRuntimeServiceID},
			}},
		},
		{
			StepId: acknowledgeID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ServiceRecreateAcknowledge{
				ServiceRecreateAcknowledge: &agentpb.ServiceRecreateAcknowledge{
					ArtifactId: candidateRuntimeArtifactID, ServiceId: candidateRuntimeServiceID,
					ReleaseId: candidateRuntimeReleaseID,
				},
			},
		},
	}
	for _, step := range steps {
		step.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
	}
	steps = append(steps, candidateRuntimeAbsenceSteps(probeID, compensateID)...)
	plan := &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: candidateRuntimePlanID, RenderGeneration: 7,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK, TargetId: candidateRuntimeServiceID,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{
			Members: []*agentpb.CandidateReleaseMember{{
				ServiceId: candidateRuntimeServiceID, CandidateReleaseId: candidateRuntimeReleaseID,
				CandidateArtifactId: candidateRuntimeArtifactID,
				ForwardStepIds:      []string{applyID, waitID, acknowledgeID},
				CandidateAbsence: &agentpb.CandidateAbsenceRestoration{
					ComposeProjectName: artifact.GetProjectName(), ProbeStepId: probeID, CompensateStepId: compensateID,
					Services: []*agentpb.CandidateReleaseService{{
						ServiceId: candidateRuntimeServiceID, ReleaseId: candidateRuntimeReleaseID,
					}},
				},
			}},
		},
	}
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(recreate runtime fixture) error = %v", err)
	}
	return sealed
}

func candidateRuntimeFixtureArtifact(
	t *testing.T,
	artifactID, ownerPlanID string,
	generation uint64,
	releaseID, target string,
	proxyConfig []byte,
	includeUnrelated bool,
) *agentpb.ComposeArtifact {
	t.Helper()
	image := "sha256:" + strings.Repeat("a", 64)
	services := []*agentpb.ComposeService{}
	var yamlServices string
	if len(proxyConfig) != 0 {
		proxyDigest := sha256.Sum256(proxyConfig)
		services = append(services, &agentpb.ComposeService{
			ServiceId: candidateRuntimeServiceID, ComposeName: "api", ExpectedReplicas: 1,
			Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
			ExpectedLabels: candidateRuntimeLabels(
				ownerPlanID,
				generation,
				candidateRuntimeServiceID,
				"proxy",
				"",
				"",
			),
			ProxyConfigJson: proxyConfig, ProxyConfigSha256: proxyDigest[:],
		})
		yamlServices += "  api:\n    image: proxy:sealed\n    configs:\n      - source: gp-proxy-runtime\n" +
			"        target: /etc/caddy/groundplane-proxy.json\n    networks:\n      shared: {}\n"
	}
	if target == "singleton" {
		services = append(services, candidateRuntimeWorkload(ownerPlanID, generation, releaseID, "", "api", image,
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON))
		yamlServices += "  api:\n    image: " + image + "\n    networks:\n      shared: {}\n"
	} else {
		services = append(services, candidateRuntimeWorkload(ownerPlanID, generation, releaseID, target, "api--"+target, image,
			agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT))
		yamlServices += "  api--" + target + ":\n    image: " + image + "\n" + candidateRuntimeResourceBindingsYAML()
		otherTarget := "green"
		if target == "green" {
			otherTarget = "blue"
		}
		if includeUnrelated {
			services = append(services, candidateRuntimeWorkload(ownerPlanID, generation, "", otherTarget,
				"api--"+otherTarget, image, agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT))
			yamlServices += "  api--" + otherTarget + ":\n    image: " + image + "\n"
		}
	}
	if includeUnrelated {
		services = append(services, &agentpb.ComposeService{
			ServiceId: candidateRuntimeOtherService, ComposeName: "worker", ExpectedReplicas: 1,
			ExpectedLabels: candidateRuntimeLabels(
				ownerPlanID,
				generation,
				candidateRuntimeOtherService,
				"",
				"",
				"",
			),
		})
		yamlServices += "  worker:\n    image: worker:unrelated\n    volumes:\n      - type: volume\n        source: unrelated-data\n        target: /data\n"
	}
	sort.Slice(services, func(left, right int) bool {
		if services[left].GetServiceId() == services[right].GetServiceId() {
			return services[left].GetComposeName() < services[right].GetComposeName()
		}
		return services[left].GetServiceId() < services[right].GetServiceId()
	})
	configs := ""
	if len(proxyConfig) != 0 {
		configs += "  gp-proxy-runtime:\n    content: " + strconv.Quote(string(proxyConfig)) + "\n"
	}
	configs += "  app-config:\n    content: selected\n  unrelated-config:\n    content: unrelated\n"
	canonical := []byte("services:\n" + yamlServices +
		"networks:\n  shared:\n    external: true\n  unrelated-network:\n    external: true\n" +
		"volumes:\n  api-data: {}\n  unrelated-data: {}\nconfigs:\n" + configs +
		"secrets:\n  api-secret:\n    file: /sealed/api-secret\n  unrelated-secret:\n    file: /sealed/unrelated\n")
	digest := sha256.Sum256(canonical)
	return &agentpb.ComposeArtifact{
		ArtifactId: artifactID, OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT,
		OwnerId: candidateRuntimeEnvironmentID, ProjectName: "gp-" + strings.ToLower(candidateRuntimeEnvironmentID),
		AuthorizedVolumeDir: "/var/lib/groundplane/volumes/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/" +
			"prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + candidateRuntimeEnvironmentID,
		CanonicalYaml: canonical, YamlSha256: digest[:], Services: services,
	}
}

func candidateRuntimeResourceBindingsYAML() string {
	return "    networks:\n      shared: {}\n" +
		"    volumes:\n      - type: volume\n        source: api-data\n        target: /data\n" +
		"    configs:\n      - source: app-config\n        target: /etc/app/config\n" +
		"    secrets:\n      - source: api-secret\n        target: api-secret\n"
}

func candidateRuntimeWorkload(
	planID string,
	generation uint64,
	releaseID, slot, composeName, image string,
	role agentpb.ComposeServiceRole,
) *agentpb.ComposeService {
	runtimeRole := "singleton"
	if role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
		runtimeRole = "slot"
	}
	return &agentpb.ComposeService{
		ServiceId: candidateRuntimeServiceID, ComposeName: composeName, ExpectedReplicas: 1,
		HasHealthcheck: true, Role: role, Slot: slot, ImageReference: image,
		ExpectedLabels: candidateRuntimeLabels(
			planID,
			generation,
			candidateRuntimeServiceID,
			runtimeRole,
			releaseID,
			slot,
		),
	}
}

func candidateRuntimeLabels(
	planID string,
	generation uint64,
	serviceID, runtimeRole, releaseID, slot string,
) []*agentpb.LabelPair {
	values := map[string]string{
		labelEnvironmentID: candidateRuntimeEnvironmentID,
		labelKind:          "service",
		labelManaged:       "true",
		labelPlanID:        planID,
		labelRenderGen:     strconv.FormatUint(generation, 10),
		labelServiceID:     serviceID,
	}
	if runtimeRole != "" {
		values[labelRuntimeRole] = runtimeRole
	}
	if releaseID != "" {
		values[labelReleaseID] = releaseID
	}
	if slot != "" {
		values[labelSlot] = slot
	}
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	labels := make([]*agentpb.LabelPair, len(keys))
	for index, key := range keys {
		labels[index] = &agentpb.LabelPair{Key: key, Value: values[key]}
	}
	return labels
}

func candidateRuntimeAbsenceSteps(probeID, compensateID string) []*agentpb.ExecutionStep {
	return []*agentpb.ExecutionStep{
		{
			StepId: probeID, TimeoutSeconds: 30,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
				CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
					CandidateArtifactId: candidateRuntimeArtifactID,
					ServiceId:           candidateRuntimeServiceID,
					CandidateReleaseId:  candidateRuntimeReleaseID,
				},
			},
		},
		{
			StepId: compensateID, TimeoutSeconds: 30,
			Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
				CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
					CandidateArtifactId: candidateRuntimeArtifactID,
					ServiceId:           candidateRuntimeServiceID,
					CandidateReleaseId:  candidateRuntimeReleaseID,
				},
			},
		},
	}
}

func candidateRuntimeProxyConfig(releaseID, target string, generation uint64) []byte {
	return []byte(fmt.Sprintf(
		`{"apps":{"http":{"servers":{"gp_g%d_%s_p8080":{"routes":[{"handle":[{"handler":"reverse_proxy","upstreams":[{"dial":"api--%s:8080"}]}]}]}}}}}`,
		generation,
		strings.ToLower(releaseID),
		target,
	))
}

func candidateRuntimeService(
	artifact *agentpb.ComposeArtifact,
	serviceID, slot string,
	role agentpb.ComposeServiceRole,
) *agentpb.ComposeService {
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID && service.GetSlot() == slot && service.GetRole() == role {
			return service
		}
	}
	return nil
}

func candidateRuntimeOpenArtifact(t *testing.T, encoded []byte) *agentpb.ComposeArtifact {
	t.Helper()
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(encoded, artifact); err != nil {
		t.Fatalf("unmarshal prepared runtime artifact: %v", err)
	}
	return artifact
}

func candidateRuntimeYAMLConfigEquals(t *testing.T, encoded []byte, name string, want []byte) bool {
	t.Helper()
	var document struct {
		Configs map[string]struct {
			Content string `yaml:"content"`
		} `yaml:"configs"`
	}
	if err := yaml.Unmarshal(encoded, &document); err != nil {
		t.Fatalf("unmarshal prepared runtime YAML: %v", err)
	}
	return document.Configs[name].Content == string(want)
}

func candidateRuntimeEqual(left, right CandidateRuntime) bool {
	return left.ServiceID == right.ServiceID && left.ReleaseID == right.ReleaseID && left.Target == right.Target &&
		left.ProxyGeneration == right.ProxyGeneration && bytes.Equal(left.ProxyConfigSHA256, right.ProxyConfigSHA256) &&
		bytes.Equal(left.CurrentArtifact, right.CurrentArtifact) &&
		bytes.Equal(left.RetainedPriorArtifact, right.RetainedPriorArtifact)
}
