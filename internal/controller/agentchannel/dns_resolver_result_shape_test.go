package agentchannel

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Rationale: successful enable/update tasks must carry the exact candidate
// serving proof, while disable has no resolver observation action to prove.
func TestDNSResolverResultShapeMatchesCompletedComponentLifecycle(t *testing.T) {
	t.Parallel()
	plan, candidate, _ := dnsResolverShapePlan(agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE)
	acknowledgement := dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED, "", candidate, nil)
	if err := validateDNSResolverResultShape(acknowledgement, plan); err != nil {
		t.Fatalf("validateDNSResolverResultShape() error = %v", err)
	}
	acknowledgement.GetComposeResult().DnsResolverCandidateObservation = nil
	if err := validateDNSResolverResultShape(acknowledgement, plan); err == nil {
		t.Fatal("completed update accepted missing candidate proof")
	}
	disable, _ := dnsResolverDisableShapePlan()
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED, "", nil, nil),
		disable,
	); err != nil {
		t.Fatalf("completed disable error = %v", err)
	}
}

func TestDNSResolverResultShapeRequiresDisableRollbackServingProof(t *testing.T) {
	t.Parallel()
	plan, rollback := dnsResolverDisableShapePlan()
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "restore", nil, nil), plan,
	); err == nil {
		t.Fatal("failed disable accepted missing rollback serving proof")
	}
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "remove", nil, rollback), plan,
	); err != nil {
		t.Fatalf("compensated disable error = %v", err)
	}
	wrong := proto.Clone(rollback).(*agentpb.DNSResolverObservationEvidence)
	wrong.ArtifactSha256 = bytes.Repeat([]byte{9}, sha256.Size)
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "remove", nil, wrong), plan,
	); err == nil {
		t.Fatal("failed disable accepted rollback proof for the wrong artifact")
	}
}

// Rationale: rollback serving proof is required only when plan order proves
// the candidate mutation completed before a later failure; a failure on the
// mutation itself must not manufacture that requirement.
func TestDNSResolverResultShapeDerivesRollbackRequirementFromExecutedPlan(t *testing.T) {
	t.Parallel()
	plan, _, rollback := dnsResolverShapePlan(agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE)
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "publish", nil, nil),
		plan,
	); err != nil {
		t.Fatalf("pre-mutation failure error = %v", err)
	}
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_TIMED_OUT, "observe", nil, nil),
		plan,
	); err == nil {
		t.Fatal("post-mutation timeout accepted missing rollback proof")
	}
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_ABORTED, "observe", nil, rollback),
		plan,
	); err != nil {
		t.Fatalf("compensated abort error = %v", err)
	}
	wrong := proto.Clone(rollback).(*agentpb.DNSResolverObservationEvidence)
	wrong.ArtifactSha256 = bytes.Repeat([]byte{9}, sha256.Size)
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "observe", nil, wrong),
		plan,
	); err == nil {
		t.Fatal("compensated failure accepted rollback proof for the wrong digest")
	}
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "publish", nil, rollback),
		plan,
	); err != nil {
		t.Fatalf("ambiguous publish failure rejected exact rollback proof: %v", err)
	}
	unproved := dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "publish", nil, nil)
	unproved.GetComposeResult().ReconciliationRequired = true
	if err := validateDNSResolverResultShape(unproved, plan); err == nil {
		t.Fatal("ambiguous publish failure accepted without compensation proof")
	}
	before := &agentpb.ExecutionStep{
		StepId:  "before-publish",
		Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{}},
	}
	plan.Steps = append([]*agentpb.ExecutionStep{before}, plan.Steps...)
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, before.GetStepId(), nil, rollback),
		plan,
	); err == nil {
		t.Fatal("pre-mutation failure accepted manufactured rollback proof")
	}
	enable, _, disabledPredecessor := dnsResolverShapePlan(
		agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE,
	)
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "observe", nil, nil),
		enable,
	); err != nil {
		t.Fatalf("compensated enable restoring disabled predecessor error = %v", err)
	}
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "observe", nil, disabledPredecessor),
		enable,
	); err == nil {
		t.Fatal("failed enable accepted impossible serving proof for its disabled predecessor")
	}
}

func TestDNSResolverResultShapeUsesDistinctRollbackServiceImage(t *testing.T) {
	t.Parallel()
	plan, _, rollback := dnsResolverShapePlan(agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE)
	candidateArtifact := plan.Artifacts[0]
	candidateArtifact.ArtifactId = "cfg_exact"
	rollbackArtifact := proto.Clone(candidateArtifact).(*agentpb.ComposeArtifact)
	rollbackArtifact.ArtifactId = "cfg_previous"
	previousService := rollbackArtifact.Services[0]
	previousService.ImageReference = "image@sha256:previous"
	previousService.ImageRepository = "image-previous"
	previousService.ImageIndexDigest = bytes.Repeat([]byte{5}, sha256.Size)
	previousService.ImageChildDigest = bytes.Repeat([]byte{6}, sha256.Size)
	plan.Artifacts = append(plan.Artifacts, rollbackArtifact)
	plan.Steps = []*agentpb.ExecutionStep{
		plan.Steps[0],
		{StepId: "apply", Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: candidateArtifact.GetArtifactId(), ServiceIds: []string{"svc_exact"},
		}}},
		plan.Steps[1],
	}
	rollback.ImageReference = previousService.GetImageReference()
	rollback.ImageRepository = previousService.GetImageRepository()
	rollback.ImageIndexDigest = previousService.GetImageIndexDigest()
	rollback.VerifiedImageDigest = previousService.GetImageChildDigest()
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "observe", nil, rollback), plan,
	); err != nil {
		t.Fatalf("predecessor-image rollback proof rejected: %v", err)
	}
	wrong := proto.Clone(rollback).(*agentpb.DNSResolverObservationEvidence)
	candidateService := candidateArtifact.Services[0]
	wrong.ImageReference = candidateService.GetImageReference()
	wrong.ImageRepository = candidateService.GetImageRepository()
	wrong.ImageIndexDigest = candidateService.GetImageIndexDigest()
	wrong.VerifiedImageDigest = candidateService.GetImageChildDigest()
	if err := validateDNSResolverResultShape(
		dnsResolverShapeAck(agentpb.TaskTerminal_TASK_TERMINAL_FAILED, "observe", nil, wrong), plan,
	); err == nil {
		t.Fatal("candidate-image rollback proof accepted for a predecessor artifact")
	}
}

func TestComposeResultRejectsSelfHashedSemanticallyEmptyDNSProof(t *testing.T) {
	t.Parallel()
	evidence := validComposeDNSProof(t)
	acknowledgement := dnsResolverShapeAck(
		agentpb.TaskTerminal_TASK_TERMINAL_COMPLETED,
		"",
		evidence,
		nil,
	)
	acknowledgement.GetComposeResult().Diagnostic =
		agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE
	if err := validateComposeTaskResult(acknowledgement); err != nil {
		t.Fatalf("valid proof rejected: %v", err)
	}
	invalid := proto.Clone(evidence).(*agentpb.DNSResolverObservationEvidence)
	invalid.CatchAllQuery = &agentpb.DNSQueryProof{}
	digest, err := dnsproof.Digest(invalid)
	if err != nil {
		t.Fatal(err)
	}
	invalid.ProofSha256 = digest[:]
	acknowledgement.GetComposeResult().DnsResolverCandidateObservation = invalid
	if err := validateComposeTaskResult(acknowledgement); err == nil {
		t.Fatal("Controller accepted a self-hashed semantically empty DNS proof")
	}
}

func validComposeDNSProof(t *testing.T) *agentpb.DNSResolverObservationEvidence {
	t.Helper()
	childDigest := bytes.Repeat([]byte{4}, sha256.Size)
	evidence := &agentpb.DNSResolverObservationEvidence{
		ComponentId:    "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ServiceId:      "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ArtifactId:     "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ArtifactSha256: bytes.Repeat([]byte{1}, sha256.Size), RenderGeneration: 7,
		ImageReference:  "registry.example/resolver@sha256:" + hex.EncodeToString(childDigest),
		ImageRepository: "registry.example/resolver", ImageIndexDigest: bytes.Repeat([]byte{3}, sha256.Size),
		VerifiedImageDigest: childDigest, ImageOs: "linux", ImageArchitecture: "amd64",
		ListenEndpoint: "127.0.0.1:53", ReloadSha512: make([]byte, 64),
		ObservedAt: timestamppb.New(time.Unix(1, 0).UTC()),
		CatchAllQuery: &agentpb.DNSQueryProof{
			Name: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS, RecursionAvailable: true,
			SelectedUpstream: "1.1.1.1:53", Attempts: 1,
			Answers: []*agentpb.DNSAnswerRecord{{
				OwnerName: ".", Type: agentpb.DNSQueryType_DNS_QUERY_TYPE_NS,
				NameServer: "a.root-servers.net.",
			}},
			Counters: []*agentpb.DNSForwardCounter{{
				Upstream: "1.1.1.1:53", Before: 1, After: 2,
			}},
		},
	}
	if err := dnsproof.Seal(evidence); err != nil {
		t.Fatal(err)
	}
	return evidence
}

func dnsResolverShapePlan(
	mode agentpb.ComponentLifecycleMode,
) (*agentpb.ExecutionPlan, *agentpb.DNSResolverObservationEvidence, *agentpb.DNSResolverObservationEvidence) {
	candidateDigest := bytes.Repeat([]byte{1}, sha256.Size)
	previousDigest := bytes.Repeat([]byte{2}, sha256.Size)
	service := &agentpb.ComposeService{
		ServiceId: "svc_exact", ImageReference: "image@sha256:exact", ImageRepository: "image",
		ImageIndexDigest: bytes.Repeat([]byte{3}, sha256.Size),
		ImageChildDigest: bytes.Repeat([]byte{4}, sha256.Size), ImageOs: "linux", ImageArchitecture: "amd64",
	}
	managed := &agentpb.ComponentApply{
		ComponentId: "cmp_exact", ArtifactId: "cfg_exact", ArtifactDigest: candidateDigest,
		ExpectedPreviousArtifactDigest: previousDigest, Generation: 7, ManagedConfigContent: true,
		ExpectedPreviousArtifactId: "cfg_previous", ExpectedPreviousGeneration: 6,
	}
	observation := &agentpb.ComponentApply{
		ComponentId: "cmp_exact", ArtifactId: "cfg_exact", ArtifactDigest: candidateDigest, Generation: 7,
	}
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY, ComponentLifecycleMode: mode,
		Artifacts: []*agentpb.ComposeArtifact{{Services: []*agentpb.ComposeService{service}}},
		Steps: []*agentpb.ExecutionStep{
			{StepId: "publish", Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: managed}},
			{StepId: "observe", Payload: &agentpb.ExecutionStep_ComponentApply{ComponentApply: observation}},
		},
	}
	proof := func(digest []byte) *agentpb.DNSResolverObservationEvidence {
		return &agentpb.DNSResolverObservationEvidence{
			ComponentId: "cmp_exact", ServiceId: "svc_exact", ArtifactId: "cfg_exact",
			ArtifactSha256: digest, RenderGeneration: 7,
			ImageReference: service.GetImageReference(), ImageRepository: service.GetImageRepository(),
			ImageIndexDigest: service.GetImageIndexDigest(), VerifiedImageDigest: service.GetImageChildDigest(),
			ImageOs: service.GetImageOs(), ImageArchitecture: service.GetImageArchitecture(),
		}
	}
	rollback := proof(previousDigest)
	rollback.ArtifactId = managed.GetExpectedPreviousArtifactId()
	rollback.RenderGeneration = managed.GetExpectedPreviousGeneration()
	return plan, proof(candidateDigest), rollback
}

func dnsResolverShapeAck(
	terminal agentpb.TaskTerminal,
	failedStepID string,
	candidate *agentpb.DNSResolverObservationEvidence,
	rollback *agentpb.DNSResolverObservationEvidence,
) *agentpb.TaskAck {
	return &agentpb.TaskAck{
		Terminal: terminal,
		Result: &agentpb.TaskAck_ComposeResult{ComposeResult: &agentpb.ComposeTaskResult{
			FailedStepId: failedStepID, DnsResolverCandidateObservation: candidate,
			DnsResolverRollbackObservation: rollback,
		}},
	}
}

func dnsResolverDisableShapePlan() (*agentpb.ExecutionPlan, *agentpb.DNSResolverObservationEvidence) {
	digest := bytes.Repeat([]byte{2}, sha256.Size)
	service := &agentpb.ComposeService{
		ServiceId: "svc_exact", ImageReference: "image@sha256:exact", ImageRepository: "image",
		ImageIndexDigest: bytes.Repeat([]byte{3}, sha256.Size),
		ImageChildDigest: bytes.Repeat([]byte{4}, sha256.Size), ImageOs: "linux", ImageArchitecture: "amd64",
	}
	action := &agentpb.ComponentApply{
		ComponentId: "cmp_exact", ArtifactId: "cfg_exact", ArtifactDigest: digest, Generation: 7,
	}
	plan := &agentpb.ExecutionPlan{
		Operation:                    agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY,
		ComponentLifecycleMode:       agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE,
		ComponentRollbackObservation: action,
		Artifacts:                    []*agentpb.ComposeArtifact{{Services: []*agentpb.ComposeService{service}}},
		Steps:                        []*agentpb.ExecutionStep{{StepId: "restore"}, {StepId: "remove"}},
	}
	return plan, &agentpb.DNSResolverObservationEvidence{
		ComponentId: action.GetComponentId(), ServiceId: service.GetServiceId(), ArtifactId: action.GetArtifactId(),
		ArtifactSha256: digest, RenderGeneration: action.GetGeneration(),
		ImageReference: service.GetImageReference(), ImageRepository: service.GetImageRepository(),
		ImageIndexDigest: service.GetImageIndexDigest(), VerifiedImageDigest: service.GetImageChildDigest(),
		ImageOs: service.GetImageOs(), ImageArchitecture: service.GetImageArchitecture(),
	}
}
