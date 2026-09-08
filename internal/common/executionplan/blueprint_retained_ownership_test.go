package executionplan

import (
	"crypto/sha256"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Historical native ownership is observation authority only. A managed-only
// Blueprint must never turn it into permission to execute a native workload.
func TestBlueprintRetainedNativeLabelsAreReadOnly(t *testing.T) {
	otherCandidate := func(p *agentpb.ExecutionPlan) {
		p.CandidateReleaseProcedure = &agentpb.CandidateReleaseProcedure{Members: []*agentpb.CandidateReleaseMember{{
			ServiceId: p.Artifacts[0].Services[1].ServiceId, CandidateArtifactId: testArtifact,
		}}}
		p.Artifacts[0].Services[1].OwnerComponentId = ""
	}
	cases := map[string]func(*agentpb.ExecutionPlan){
		"other candidate": otherCandidate,
		"candidate selects retained": func(p *agentpb.ExecutionPlan) {
			otherCandidate(p)
			p.Steps[0].GetComposeApply().ServiceIds = []string{testServiceID}
		},
		"candidate follows dependencies": func(p *agentpb.ExecutionPlan) {
			otherCandidate(p)
			p.Steps[0].GetComposeApply().NoDependencies = false
		},
		"candidate reconciles project": func(p *agentpb.ExecutionPlan) {
			otherCandidate(p)
			p.Steps[0].GetComposeApply().FullReconcile = true
		},
		"candidate recovers retained": func(p *agentpb.ExecutionPlan) {
			otherCandidate(p)
			p.Steps = append(p.Steps, &agentpb.ExecutionStep{
				Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
					CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
						CandidateArtifactId: testArtifact, ServiceId: testServiceID,
					},
				},
			})
		},
		"candidate scripts retained": func(p *agentpb.ExecutionPlan) {
			otherCandidate(p)
			p.Steps = append(p.Steps, &agentpb.ExecutionStep{
				Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{ServiceId: testServiceID}},
			})
		},
		"retained candidate member": func(p *agentpb.ExecutionPlan) {
			p.CandidateReleaseProcedure = &agentpb.CandidateReleaseProcedure{
				Members: []*agentpb.CandidateReleaseMember{{
					ServiceId: testServiceID, CandidateArtifactId: testArtifact,
				}},
			}
		},
		"managed only":    func(*agentpb.ExecutionPlan) {},
		"native selected": func(p *agentpb.ExecutionPlan) { p.Steps[0].GetComposeApply().ServiceIds = []string{testServiceID} },
		"dependencies":    func(p *agentpb.ExecutionPlan) { p.Steps[0].GetComposeApply().NoDependencies = false },
		"full project":    func(p *agentpb.ExecutionPlan) { p.Steps[0].GetComposeApply().FullReconcile = true },
		"candidate":       func(p *agentpb.ExecutionPlan) { p.CandidateReleaseProcedure = &agentpb.CandidateReleaseProcedure{} },
		"script": func(p *agentpb.ExecutionPlan) {
			p.Steps[0].Payload = &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{}}
		},
		"native stop": func(p *agentpb.ExecutionPlan) {
			p.Steps[0].Payload = &agentpb.ExecutionStep_ComposeStop{
				ComposeStop: &agentpb.ComposeStop{ArtifactId: testArtifact, ServiceIds: []string{testServiceID}},
			}
		},
		"other operation": func(p *agentpb.ExecutionPlan) { p.Operation = agentpb.PlanOperation_PLAN_OPERATION_DEPLOY },
		"unsealed image":  func(p *agentpb.ExecutionPlan) { p.Artifacts[0].Services[0].ImageReference = "example/app:latest" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := validPlan()
			p.Operation, p.TargetId = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, testEnvironmentID
			a := p.Artifacts[0]
			a.OwnerKind, a.OwnerId = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, testEnvironmentID
			native := a.Services[0]
			native.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
			native.ImageReference = "sha256:" + strings.Repeat("a", 64)
			for _, pair := range native.ExpectedLabels {
				if pair.Key == labelRenderGen {
					pair.Value = "6"
				}
			}
			native.ExpectedLabels = append(native.ExpectedLabels,
				&agentpb.LabelPair{Key: labelEnvironmentID, Value: testEnvironmentID},
				&agentpb.LabelPair{Key: labelReleaseID, Value: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
				&agentpb.LabelPair{Key: labelRuntimeRole, Value: "singleton"})
			sort.Slice(
				native.ExpectedLabels,
				func(i, j int) bool { return native.ExpectedLabels[i].Key < native.ExpectedLabels[j].Key },
			)
			managedID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
			a.Services = append(
				a.Services,
				&agentpb.ComposeService{ServiceId: managedID, OwnerComponentId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			)
			p.Steps[0].GetComposeApply().ServiceIds = []string{managedID}
			p.Steps[0].GetComposeApply().NoDependencies = true
			mutate(p)
			err := validateLabels(p, a, "service", testServiceID, native.ExpectedLabels)
			if (err == nil) != (name == "managed only" || name == "other candidate") {
				t.Fatalf("ownership result = %v", err)
			}
		})
	}
}

// A managed teardown may carry the current candidate artifact plus a distinct
// historical source artifact. The retained native service remains observation
// authority in the candidate artifact, while the managed source is removed by
// one exact Service selection. This is the narrow regression for the old
// single-artifact retained-ownership gate.
func TestBlueprintRetainedNativeLabelsAllowExactManagedTeardownArtifact(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	plan.Operation, plan.TargetId = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, testEnvironmentID
	candidate := plan.Artifacts[0]
	candidate.OwnerKind, candidate.OwnerId = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, testEnvironmentID
	candidate.ProjectName = "gp-" + strings.ToLower(testEnvironmentID)
	candidate.AuthorizedVolumeDir = "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + testEnvironmentID
	native := candidate.Services[0]
	native.Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
	native.ImageReference = "sha256:" + strings.Repeat("a", 64)
	for _, pair := range native.ExpectedLabels {
		if pair.Key == labelRenderGen {
			pair.Value = "6"
		}
	}
	native.ExpectedLabels = append(native.ExpectedLabels,
		&agentpb.LabelPair{Key: labelEnvironmentID, Value: testEnvironmentID},
		&agentpb.LabelPair{Key: labelReleaseID, Value: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		&agentpb.LabelPair{Key: labelRuntimeRole, Value: "singleton"})
	sort.Slice(
		native.ExpectedLabels,
		func(i, j int) bool { return native.ExpectedLabels[i].Key < native.ExpectedLabels[j].Key },
	)
	componentID := "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	managedID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	historicalID := "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	historical := &agentpb.ComposeArtifact{ArtifactId: historicalID,
		OwnerKind: agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT, OwnerId: testEnvironmentID,
		Services: []*agentpb.ComposeService{
			{ServiceId: managedID, ComposeName: "router", OwnerComponentId: componentID},
		},
	}
	historicalBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(historical)
	if err != nil {
		t.Fatal(err)
	}
	historicalDigest := sha256.Sum256(historicalBytes)
	plan.Artifacts = append(plan.Artifacts, historical)
	plan.Steps[0].Payload = &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
		ArtifactId: historicalID, ServiceIds: []string{managedID},
	}}
	plan.ManagedComponentProcedure = &agentpb.ManagedComponentProcedure{Services: []*agentpb.ManagedComponentService{{
		ComponentKind: "caddy", ComponentId: componentID, ServiceId: managedID, ComposeServiceName: "router",
		SourceRevisionId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAW", SourceArtifactId: historicalID,
		SourceArtifactSha256: historicalDigest[:], RemoveStepId: plan.Steps[0].StepId,
	}}}

	if err := validateLabels(plan, candidate, "service", testServiceID, native.ExpectedLabels); err != nil {
		t.Fatalf("retained native labels with exact managed teardown = %v", err)
	}
	for name, mutate := range map[string]func(*agentpb.ExecutionPlan){
		"native selected": func(value *agentpb.ExecutionPlan) {
			value.Steps[0].Payload = &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: testArtifact, ServiceIds: []string{testServiceID},
			}}
		},
		"whole project removal": func(value *agentpb.ExecutionPlan) {
			value.Steps[0].GetComposeRemove().WholeProject = true
			value.Steps[0].GetComposeRemove().ServiceIds = nil
		},
		"unrelated source": func(value *agentpb.ExecutionPlan) {
			value.ManagedComponentProcedure.Services[0].SourceArtifactId = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		},
		"unsealed source": func(value *agentpb.ExecutionPlan) {
			value.ManagedComponentProcedure.Services[0].SourceArtifactSha256[0]++
		},
	} {
		t.Run(name, func(t *testing.T) {
			changed := proto.Clone(plan).(*agentpb.ExecutionPlan)
			mutate(changed)
			if err := validateLabels(changed, changed.Artifacts[0], "service", testServiceID, changed.Artifacts[0].Services[0].ExpectedLabels); err == nil {
				t.Fatal("retained native labels accepted an unauthorized managed teardown")
			}
		})
	}

	// Run the same mixed artifact through the full sealing path. Seal clones
	// the plan before validation, so this also guards against pointer-based
	// artifact identity checks.
	full := proto.Clone(plan).(*agentpb.ExecutionPlan)
	historical = full.Artifacts[1]
	managed := sharedManagedSourceService(testEnvironmentID, componentID, managedID, "router")
	historicalNative := proto.Clone(full.Artifacts[0].Services[0]).(*agentpb.ComposeService)
	historical.Services = []*agentpb.ComposeService{historicalNative, managed}
	sort.Slice(historical.Services, func(i, j int) bool {
		if historical.Services[i].ServiceId != historical.Services[j].ServiceId {
			return historical.Services[i].ServiceId < historical.Services[j].ServiceId
		}
		return historical.Services[i].ComposeName < historical.Services[j].ComposeName
	})
	historical.ProjectName = "gp-" + strings.ToLower(testEnvironmentID)
	historical.AuthorizedVolumeDir = candidate.AuthorizedVolumeDir
	historical.CanonicalYaml = []byte("services:\n  api:\n    image: old\n  router:\n    image: managed\n")
	historicalYAMLHash := sha256.Sum256(historical.CanonicalYaml)
	historical.YamlSha256 = historicalYAMLHash[:]
	historicalBytes, err = (proto.MarshalOptions{Deterministic: true}).Marshal(historical)
	if err != nil {
		t.Fatal(err)
	}
	historicalDigest = sha256.Sum256(historicalBytes)
	full.ManagedComponentProcedure.Services[0].SourceArtifactSha256 = historicalDigest[:]
	if _, err := Seal(full); err != nil {
		t.Fatalf("Seal(mixed historical managed teardown) error = %v", err)
	}
}
