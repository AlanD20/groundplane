package executionplan

import (
	"bytes"
	"crypto/sha256"
	"sort"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: first enabling Caddy and Tunnel renders both managed services in
// one immutable Environment artifact. Each source must authenticate its own
// service while the shared artifact remains one authority.
func TestSealAcceptsDistinctManagedSourcesSharingArtifact(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	environmentID := "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	plan.TargetId = environmentID
	artifact := plan.Artifacts[0]
	artifact.OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	artifact.OwnerId = environmentID
	artifact.ProjectName = "gp-" + strings.ToLower(environmentID)
	artifact.AuthorizedVolumeDir = "/var/lib/groundplane/volumes/" +
		"tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID
	componentIDs := []string{
		"cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW",
	}
	serviceIDs := []string{
		"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"svc_01ARZ3NDEKTSV4RRFFQ69G5FAW",
	}
	services := []*agentpb.ComposeService{
		sharedManagedSourceService(environmentID, componentIDs[0], serviceIDs[0], "caddy"),
		sharedManagedSourceService(environmentID, componentIDs[1], serviceIDs[1], "cloudflare-tunnel"),
	}
	artifact.Services = services
	steps := make([]*agentpb.ExecutionStep, len(services))
	for index, service := range services {
		steps[index] = &agentpb.ExecutionStep{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FA" + string(rune('V'+index)), TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifact.ArtifactId, ServiceIds: []string{service.ServiceId},
			}},
		}
		if index > 0 {
			steps[index].PrerequisiteStepId = steps[index-1].StepId
		}
	}
	plan.Steps = steps
	digest, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	artifactDigest := sha256.Sum256(digest)
	plan.ManagedComponentProcedure = &agentpb.ManagedComponentProcedure{Services: []*agentpb.ManagedComponentService{
		{ComponentKind: "caddy", ComponentId: componentIDs[0], ServiceId: serviceIDs[0], ComposeServiceName: "caddy",
			SourceRevisionId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", SourceArtifactId: artifact.ArtifactId,
			SourceArtifactSha256: artifactDigest[:], RemoveStepId: steps[0].StepId},
		{
			ComponentKind:        "cloudflare-tunnel",
			ComponentId:          componentIDs[1],
			ServiceId:            serviceIDs[1],
			ComposeServiceName:   "cloudflare-tunnel",
			SourceRevisionId:     "task_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			SourceArtifactId:     artifact.ArtifactId,
			SourceArtifactSha256: artifactDigest[:],
			RemoveStepId:         steps[1].StepId,
		},
	}}
	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(shared managed artifact) error = %v", err)
	}

	duplicate := proto.Clone(plan).(*agentpb.ExecutionPlan)
	duplicate.ManagedComponentProcedure.Services[1].ServiceId = serviceIDs[0]
	duplicate.Steps[1].GetComposeRemove().ServiceIds[0] = serviceIDs[0]
	if _, err := Seal(duplicate); err == nil {
		t.Fatal("Seal(duplicate managed service source) accepted conflicting authority")
	}
}

// The diagnostic must identify the closed predicate branch without exposing
// any plan-owned identity. The invalid source digest exercises an existing
// rejection while the subsequent valid Seal proves that exact removals from
// an all-historical artifact remain accepted.
func TestSealRetainedOwnershipDiagnosticUsesClosedReason(t *testing.T) {
	t.Parallel()
	plan := validPlan()
	const environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
	plan.TargetId = environmentID
	artifact := plan.Artifacts[0]
	artifact.OwnerKind = agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT
	artifact.OwnerId = environmentID
	artifact.ProjectName = "gp-" + strings.ToLower(environmentID)
	artifact.AuthorizedVolumeDir = "/var/lib/groundplane/volumes/" + environmentID
	componentIDs := []string{
		"cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"cmp_01ARZ3NDEKTSV4RRFFQ69G5FAW",
	}
	serviceIDs := []string{
		"svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		"svc_01ARZ3NDEKTSV4RRFFQ69G5FAW",
	}
	managed := []*agentpb.ComposeService{
		sharedManagedSourceService(environmentID, componentIDs[0], serviceIDs[0], "caddy"),
		sharedManagedSourceService(environmentID, componentIDs[1], serviceIDs[1], "cloudflare-tunnel"),
	}
	nativeID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	native := &agentpb.ComposeService{
		ServiceId: nativeID, ComposeName: "api", ExpectedReplicas: 1,
		Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
		ImageReference: "sha256:" + strings.Repeat("a", 64),
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: labelEnvironmentID, Value: environmentID},
			{Key: labelKind, Value: "service"}, {Key: labelManaged, Value: "true"},
			{Key: labelPlanID, Value: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
			{Key: labelReleaseID, Value: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Key: labelRenderGen, Value: "6"}, {Key: labelRuntimeRole, Value: "singleton"},
			{Key: labelServiceID, Value: nativeID},
		},
	}
	sort.Slice(
		native.ExpectedLabels,
		func(i, j int) bool { return native.ExpectedLabels[i].Key < native.ExpectedLabels[j].Key },
	)
	artifact.Services = append(managed, native)
	sort.Slice(artifact.Services, func(i, j int) bool {
		return artifact.Services[i].ServiceId+"\x00"+artifact.Services[i].ComposeName <
			artifact.Services[j].ServiceId+"\x00"+artifact.Services[j].ComposeName
	})
	steps := make([]*agentpb.ExecutionStep, len(managed))
	for index, service := range managed {
		steps[index] = &agentpb.ExecutionStep{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FA" + string(rune('V'+index)), TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifact.ArtifactId, ServiceIds: []string{service.ServiceId},
			}},
		}
		if index > 0 {
			steps[index].PrerequisiteStepId = steps[index-1].StepId
		}
	}
	plan.Steps = steps
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encoded)
	plan.ManagedComponentProcedure = &agentpb.ManagedComponentProcedure{Services: []*agentpb.ManagedComponentService{
		{ComponentKind: "caddy", ComponentId: componentIDs[0], ServiceId: serviceIDs[0], ComposeServiceName: "caddy",
			SourceRevisionId: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", SourceArtifactId: artifact.ArtifactId,
			SourceArtifactSha256: digest[:], RemoveStepId: steps[0].StepId},
		{
			ComponentKind:        "cloudflare-tunnel",
			ComponentId:          componentIDs[1],
			ServiceId:            serviceIDs[1],
			ComposeServiceName:   "cloudflare-tunnel",
			SourceRevisionId:     "task_01ARZ3NDEKTSV4RRFFQ69G5FAW",
			SourceArtifactId:     artifact.ArtifactId,
			SourceArtifactSha256: digest[:],
			RemoveStepId:         steps[1].StepId,
		},
	}}
	plan.ManagedComponentProcedure.Services[0].SourceArtifactSha256 = []byte("wrong-digest")
	if _, err := Seal(plan); err == nil || !strings.Contains(err.Error(), "retained ownership: procedure") {
		t.Fatalf("Seal(retained ownership diagnostic) error = %v, want closed procedure reason", err)
	}
	plan.ManagedComponentProcedure.Services[0].SourceArtifactSha256 = digest[:]
	if _, err := Seal(plan); err != nil {
		t.Fatalf("Seal(all historical exact removals) error = %v", err)
	}
}

func sharedManagedSourceService(environmentID, componentID, serviceID, name string) *agentpb.ComposeService {
	service := &agentpb.ComposeService{
		ServiceId: serviceID, ComposeName: name, ExpectedReplicas: 1, OwnerComponentId: componentID,
		ExpectedLabels: []*agentpb.LabelPair{
			{Key: labelComponentID, Value: componentID}, {Key: labelEnvironmentID, Value: environmentID},
			{Key: labelKind, Value: "service"}, {Key: labelManaged, Value: "true"},
			{Key: labelPlanID, Value: testPlanID}, {Key: labelRenderGen, Value: "7"},
			{Key: labelServiceID, Value: serviceID},
		},
	}
	service.ImageRepository = "example/component"
	service.ImageReference = service.ImageRepository + "@sha256:" + strings.Repeat("02", sha256.Size)
	service.ImageOs, service.ImageArchitecture = "linux", "amd64"
	service.ImageIndexDigest = bytes.Repeat([]byte{1}, sha256.Size)
	service.ImageChildDigest = bytes.Repeat([]byte{2}, sha256.Size)
	service.ImageConfigDigest = bytes.Repeat([]byte{3}, sha256.Size)
	service.ExpectedLabels = append(service.ExpectedLabels,
		&agentpb.LabelPair{Key: labelImageConfigDigest, Value: "sha256:" + strings.Repeat("03", sha256.Size)},
		&agentpb.LabelPair{Key: labelImageChildDigest, Value: "sha256:" + strings.Repeat("02", sha256.Size)},
		&agentpb.LabelPair{Key: labelImageIndexDigest, Value: "sha256:" + strings.Repeat("01", sha256.Size)},
		&agentpb.LabelPair{Key: labelImagePlatform, Value: "linux/amd64"})
	sort.Slice(service.ExpectedLabels, func(left, right int) bool {
		return service.ExpectedLabels[left].Key < service.ExpectedLabels[right].Key
	})
	return service
}
