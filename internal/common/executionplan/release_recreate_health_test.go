package executionplan

import (
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	recreateHealthCandidateArtifact = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	recreateHealthPriorArtifact     = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	recreateHealthCandidateRelease  = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	recreateHealthPriorRelease      = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAW"
)

// Rationale: recreate steps can reach the host only when every candidate and prior
// workload they reference has a positive exact count, sealed image, and healthcheck.
func TestValidateServiceRecreateStepsRequireHealthySealedSets(t *testing.T) {
	artifacts := recreateHealthArtifacts()
	acknowledge := &agentpb.ServiceRecreateAcknowledge{
		ArtifactId: recreateHealthCandidateArtifact, ServiceId: testServiceID,
		ReleaseId: recreateHealthCandidateRelease,
	}
	probe := &agentpb.ServiceRecreateProbe{
		CandidateArtifactId: recreateHealthCandidateArtifact, PriorArtifactId: recreateHealthPriorArtifact,
		ServiceId: testServiceID, CandidateReleaseId: recreateHealthCandidateRelease,
		PriorReleaseId: recreateHealthPriorRelease,
	}
	compensate := &agentpb.ServiceRecreateCompensate{
		CandidateArtifactId: recreateHealthCandidateArtifact, ArtifactId: recreateHealthPriorArtifact,
		ServiceId: testServiceID, CandidateReleaseId: recreateHealthCandidateRelease,
		PriorReleaseId: recreateHealthPriorRelease, PriorTarget: "blue", Enabled: true,
	}
	if err := validateServiceRecreateAcknowledge(agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, acknowledge, artifacts); err != nil {
		t.Fatalf("valid N=3 acknowledgement: %v", err)
	}
	if err := validateServiceRecreateProbe(agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, probe, artifacts); err != nil {
		t.Fatalf("valid blue predecessor probe: %v", err)
	}
	if err := validateServiceRecreateCompensate(agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, compensate, artifacts); err != nil {
		t.Fatalf("valid blue predecessor compensation: %v", err)
	}

	for _, test := range []struct {
		name     string
		validate func(map[string]*agentpb.ComposeArtifact) error
		mutate   func(map[string]*agentpb.ComposeArtifact)
	}{
		{name: "acknowledgement candidate", mutate: clearCandidateHealth,
			validate: func(values map[string]*agentpb.ComposeArtifact) error {
				return validateServiceRecreateAcknowledge(agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, acknowledge, values)
			}},
		{name: "probe candidate", mutate: clearCandidateHealth,
			validate: func(values map[string]*agentpb.ComposeArtifact) error {
				return validateServiceRecreateProbe(agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, probe, values)
			}},
		{name: "probe predecessor", mutate: clearPriorHealth,
			validate: func(values map[string]*agentpb.ComposeArtifact) error {
				return validateServiceRecreateProbe(agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, probe, values)
			}},
		{name: "compensation candidate", mutate: clearCandidateHealth,
			validate: func(values map[string]*agentpb.ComposeArtifact) error {
				return validateServiceRecreateCompensate(agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, compensate, values)
			}},
		{name: "compensation predecessor", mutate: clearPriorHealth,
			validate: func(values map[string]*agentpb.ComposeArtifact) error {
				return validateServiceRecreateCompensate(agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, compensate, values)
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			owned := map[string]*agentpb.ComposeArtifact{}
			for id, artifact := range artifacts {
				owned[id] = proto.Clone(artifact).(*agentpb.ComposeArtifact)
			}
			test.mutate(owned)
			if err := test.validate(owned); err == nil {
				t.Fatal("recreate validator accepted a referenced workload without a healthcheck")
			}
		})
	}
}

func recreateHealthArtifacts() map[string]*agentpb.ComposeArtifact {
	service := func(role agentpb.ComposeServiceRole, slot, release string, replicas uint32) *agentpb.ComposeService {
		roleLabel := "singleton"
		if role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
			roleLabel = "slot"
		}
		labels := []*agentpb.LabelPair{{Key: labelReleaseID, Value: release}, {Key: labelRuntimeRole, Value: roleLabel}}
		if slot != "" {
			labels = append(labels, &agentpb.LabelPair{Key: labelSlot, Value: slot})
		}
		return &agentpb.ComposeService{
			ServiceId: testServiceID, ComposeName: "api-" + slot, Role: role, Slot: slot,
			ExpectedReplicas: replicas, HasHealthcheck: true, ImageReference: "registry.example/api:sealed",
			ExpectedLabels: labels,
		}
	}
	return map[string]*agentpb.ComposeArtifact{
		recreateHealthCandidateArtifact: {ArtifactId: recreateHealthCandidateArtifact,
			Services: []*agentpb.ComposeService{service(agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
				"", recreateHealthCandidateRelease, 3)}},
		recreateHealthPriorArtifact: {ArtifactId: recreateHealthPriorArtifact,
			Services: []*agentpb.ComposeService{
				service(agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, "blue", recreateHealthPriorRelease, 1),
				service(agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, "green", "", 1),
			}},
	}
}

func clearCandidateHealth(artifacts map[string]*agentpb.ComposeArtifact) {
	artifacts[recreateHealthCandidateArtifact].Services[0].HasHealthcheck = false
}

func clearPriorHealth(artifacts map[string]*agentpb.ComposeArtifact) {
	artifacts[recreateHealthPriorArtifact].Services[0].HasHealthcheck = false
}
