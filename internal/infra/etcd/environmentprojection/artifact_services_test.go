package environmentprojection

import (
	domain "github.com/AlanD20/groundplane/internal/core/release"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
	strconv "strconv"
	testing "testing"
)

// Rationale: executed artifacts cover logical Services through closed runtime
// roles; extra copies, missing slots and changed owners never become valid coverage.
func TestEnvironmentArtifactLogicalRuntimeCoverage(t *testing.T) {
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	expected := map[string]environmentArtifactServiceIdentity{serviceID: {name: "api"}}
	singletonName, err := domain.WorkloadComposeName("api", domain.WorkloadSingleton)
	if err != nil {
		t.Fatal(err)
	}
	blueName, err := domain.WorkloadComposeName("api", domain.WorkloadBlue)
	if err != nil {
		t.Fatal(err)
	}
	greenName, err := domain.WorkloadComposeName("api", domain.WorkloadGreen)
	if err != nil {
		t.Fatal(err)
	}
	service := func(name string, role agentpb.ComposeServiceRole, slot string) *agentpb.ComposeService {
		return &agentpb.ComposeService{ServiceId: serviceID, ComposeName: name, Role: role, Slot: slot}
	}
	proxy := service("api", agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY, "")
	singleton := service(singletonName, agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, "")
	blue := service(blueName, agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, "blue")
	green := service(greenName, agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, "green")
	for _, test := range []struct {
		name     string
		services []*agentpb.ComposeService
		valid    bool
	}{
		{"authored", []*agentpb.ComposeService{service("api", agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED, "")}, true},
		{"portless", []*agentpb.ComposeService{service("api", agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON, "")}, true},
		{"addressable recreate", []*agentpb.ComposeService{proxy, singleton}, true},
		{"blue green", []*agentpb.ComposeService{proxy, blue, green}, true},
		{"missing proxy", []*agentpb.ComposeService{singleton}, false},
		{"missing green", []*agentpb.ComposeService{proxy, blue}, false},
		{"duplicate slot", []*agentpb.ComposeService{proxy, blue, blue}, false},
		{"mixed strategies", []*agentpb.ComposeService{proxy, singleton, blue, green}, false},
		{"missing logical service", nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := validateEnvironmentArtifactServices(&agentpb.ComposeArtifact{Services: test.services}, expected)
			if (err == nil) != test.valid {
				t.Fatalf("coverage error=%v valid=%t", err, test.valid)
			}
		})
	}
	for _, field := range []string{"name", "owner", "service", "slot"} {
		t.Run(field, func(t *testing.T) {
			changed := proto.CloneOf(singleton)
			switch field {
			case "name":
				changed.ComposeName = "other"
			case "owner":
				changed.OwnerComponentId = "cmp_other"
			case "service":
				changed.ServiceId = "svc_other"
			case "slot":
				changed.Slot = "blue"
			}
			if validateEnvironmentArtifactServices(
				&agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{proxy, changed}},
				expected,
			) == nil {
				t.Fatal("changed runtime identity accepted")
			}
		})
	}
}

// Rationale: a Component-only Blueprint retains the exact serving runtime;
// an inactive slot without retained Release authority must not be invented.
func TestEnvironmentArtifactRetainedSingleSlotCoverage(t *testing.T) {
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	planID := "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	for _, slot := range []string{"blue", "green"} {
		t.Run(slot, func(t *testing.T) {
			name, err := domain.WorkloadComposeName("api", domain.WorkloadTarget(slot))
			if err != nil {
				t.Fatal(err)
			}
			labels := func(generation uint64) []*agentpb.LabelPair {
				return []*agentpb.LabelPair{
					{Key: "com.groundplane.plan-id", Value: planID},
					{Key: "com.groundplane.render-generation", Value: strconv.FormatUint(generation, 10)},
				}
			}
			proxy := &agentpb.ComposeService{ServiceId: serviceID, ComposeName: "api",
				Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY, ExpectedLabels: labels(4)}
			workload := &agentpb.ComposeService{ServiceId: serviceID, ComposeName: name, Slot: slot,
				Role: agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT, ExpectedLabels: labels(4)}
			expected := map[string]environmentArtifactServiceIdentity{serviceID: {name: "api", renderGeneration: 5}}
			artifact := &agentpb.ComposeArtifact{Services: []*agentpb.ComposeService{proxy, workload}}
			if err := validateEnvironmentArtifactServices(artifact, expected); err != nil {
				t.Fatalf("retained serving slot: %v", err)
			}
			for _, change := range []string{"fresh", "future", "different generation", "different plan", "missing label", "duplicate label"} {
				t.Run(change, func(t *testing.T) {
					changed := proto.CloneOf(artifact)
					switch change {
					case "fresh":
						changed.Services[0].ExpectedLabels, changed.Services[1].ExpectedLabels = labels(5), labels(5)
					case "future":
						changed.Services[0].ExpectedLabels, changed.Services[1].ExpectedLabels = labels(6), labels(6)
					case "different generation":
						changed.Services[1].ExpectedLabels = labels(3)
					case "different plan":
						changed.Services[1].ExpectedLabels[0].Value = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"
					case "missing label":
						changed.Services[1].ExpectedLabels = nil
					case "duplicate label":
						changed.Services[1].ExpectedLabels = append(changed.Services[1].ExpectedLabels, labels(4)[0])
					}
					if validateEnvironmentArtifactServices(changed, expected) == nil {
						t.Fatal("unbound or fresh partial runtime accepted")
					}
				})
			}
		})
	}
}
