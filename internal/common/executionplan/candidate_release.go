package executionplan

import (
	"bytes"
	"crypto/sha256"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	maximumCandidateReleaseMembers        = 32
	maximumCandidateReleaseHooks          = 16
	maximumCandidateReleaseProcedureBytes = 256 << 10
)

// CandidateReleaseDescriptor is the bounded immutable portion of a sealed
// execution plan required to claim and recover one candidate Release.
type CandidateReleaseDescriptor struct {
	PlanID                 string                `json:"plan_id"`
	PlanHash               []byte                `json:"plan_hash"`
	Operation              agentpb.PlanOperation `json:"operation"`
	ProcedureBytes         []byte                `json:"procedure"`
	ComponentActionStepIDs []string              `json:"component_action_step_ids"`
}

// DescribeCandidateRelease extracts a canonical descriptor only from a fully
// validated sealed plan.
func DescribeCandidateRelease(plan *agentpb.ExecutionPlan) (CandidateReleaseDescriptor, error) {
	sealed, err := Validate(plan)
	if err != nil {
		return CandidateReleaseDescriptor{}, err
	}
	procedure := sealed.GetCandidateReleaseProcedure()
	if procedure == nil || len(procedure.GetMembers()) > maximumCandidateReleaseMembers {
		return CandidateReleaseDescriptor{}, errs.New(
			errs.KindValidationFailed,
			"candidate Release descriptor member bound is invalid",
		)
	}
	hooks := 0
	for _, step := range sealed.GetSteps() {
		if step.GetRunScript() != nil {
			hooks++
		}
	}
	if hooks > maximumCandidateReleaseHooks {
		return CandidateReleaseDescriptor{}, errs.New(
			errs.KindValidationFailed,
			"candidate Release descriptor hook bound is invalid",
		)
	}
	procedureBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(procedure)
	if err != nil || len(procedureBytes) == 0 || len(procedureBytes) > maximumCandidateReleaseProcedureBytes {
		return CandidateReleaseDescriptor{}, errs.New(
			errs.KindValidationFailed,
			"candidate Release descriptor procedure is invalid",
		)
	}
	return CandidateReleaseDescriptor{
		PlanID: sealed.GetPlanId(), PlanHash: append([]byte(nil), sealed.GetPlanHash()...),
		Operation: sealed.GetOperation(), ProcedureBytes: append([]byte(nil), procedureBytes...),
		ComponentActionStepIDs: componentActionStepIDs(sealed),
	}, nil
}

// OpenCandidateReleaseDescriptor validates a stored descriptor and returns an
// owned procedure. It never supplies defaults or consults mutable state.
func OpenCandidateReleaseDescriptor(descriptor CandidateReleaseDescriptor) (*agentpb.CandidateReleaseProcedure, error) {
	if err := ValidateComponentActionStepIDs(descriptor.ComponentActionStepIDs); err != nil {
		return nil, err
	}
	if ids.Validate(ids.KindPlan, descriptor.PlanID) != nil || len(descriptor.PlanHash) != sha256.Size ||
		!candidateReleaseOperation(descriptor.Operation) || len(descriptor.ProcedureBytes) == 0 ||
		len(descriptor.ProcedureBytes) > maximumCandidateReleaseProcedureBytes {
		return nil, errs.New(errs.KindValidationFailed, "candidate Release descriptor is invalid")
	}
	procedure := new(agentpb.CandidateReleaseProcedure)
	if err := proto.Unmarshal(descriptor.ProcedureBytes, procedure); err != nil ||
		len(procedure.ProtoReflect().GetUnknown()) != 0 ||
		len(procedure.GetMembers()) > maximumCandidateReleaseMembers {
		return nil, errs.New(errs.KindValidationFailed, "candidate Release descriptor procedure is invalid")
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(procedure)
	if err != nil || !bytes.Equal(canonical, descriptor.ProcedureBytes) ||
		validateCandidateReleaseProcedure(descriptor.Operation, procedure) != nil {
		return nil, errs.New(errs.KindValidationFailed, "candidate Release descriptor procedure is invalid")
	}
	return proto.Clone(procedure).(*agentpb.CandidateReleaseProcedure), nil
}

// CandidateReleaseDescriptorMatchesPlan proves that a descriptor was derived
// from this exact sealed execution plan.
func CandidateReleaseDescriptorMatchesPlan(descriptor CandidateReleaseDescriptor, plan *agentpb.ExecutionPlan) error {
	expected, err := DescribeCandidateRelease(plan)
	if err != nil {
		return err
	}
	if descriptor.PlanID != expected.PlanID || descriptor.Operation != expected.Operation ||
		!bytes.Equal(
			descriptor.PlanHash,
			expected.PlanHash,
		) || !bytes.Equal(descriptor.ProcedureBytes, expected.ProcedureBytes) || !slices.Equal(descriptor.ComponentActionStepIDs, expected.ComponentActionStepIDs) {
		return errs.New(errs.KindValidationFailed, "candidate Release descriptor does not match execution plan")
	}
	return nil
}

// CloneCandidateReleaseDescriptor returns a deep copy suitable for a durable
// record without sharing caller-owned byte slices.
func CloneCandidateReleaseDescriptor(descriptor CandidateReleaseDescriptor) CandidateReleaseDescriptor {
	descriptor.PlanHash = append([]byte(nil), descriptor.PlanHash...)
	descriptor.ProcedureBytes = append([]byte(nil), descriptor.ProcedureBytes...)
	descriptor.ComponentActionStepIDs = slices.Clone(descriptor.ComponentActionStepIDs)
	return descriptor
}

// CandidateReleaseProcedureInput is the complete immutable authority for all
// candidate Release mutations in one execution plan.
type CandidateReleaseProcedureInput struct {
	Operation agentpb.PlanOperation
	Members   []CandidateReleaseMemberInput
}

type CandidateReleaseMemberInput struct {
	ServiceID           string
	CandidateReleaseID  string
	CandidateArtifactID string
	ForwardStepIDs      []string
	ServingPredecessor  *ServingPredecessorInput
	CandidateAbsence    *CandidateAbsenceInput
}

type ServingPredecessorInput struct {
	ProbeStepID             string
	CompensateStepID        string
	PriorArtifactID         string
	PriorReleaseID          string
	PriorTarget             string
	RetainedPriorArtifactID string
}

type CandidateAbsenceInput struct {
	ComposeProjectName string
	Services           []CandidateServiceIdentity
	ProbeStepID        string
	CompensateStepID   string
}

type CandidateServiceIdentity struct {
	ServiceID string
	ReleaseID string
}

// BuildCandidateReleaseProcedure returns an owned, canonically ordered
// concrete protobuf value. It does not inspect host or applied state.
func BuildCandidateReleaseProcedure(input CandidateReleaseProcedureInput) (*agentpb.CandidateReleaseProcedure, error) {
	members := append([]CandidateReleaseMemberInput(nil), input.Members...)
	sort.Slice(members, func(left, right int) bool { return members[left].ServiceID < members[right].ServiceID })
	procedure := &agentpb.CandidateReleaseProcedure{Members: make([]*agentpb.CandidateReleaseMember, len(members))}
	for index, inputMember := range members {
		member := &agentpb.CandidateReleaseMember{
			ServiceId: inputMember.ServiceID, CandidateReleaseId: inputMember.CandidateReleaseID,
			CandidateArtifactId: inputMember.CandidateArtifactID,
			ForwardStepIds:      append([]string(nil), inputMember.ForwardStepIDs...),
		}
		if value := inputMember.ServingPredecessor; value != nil {
			member.ServingPredecessor = &agentpb.ServingPredecessorRestoration{
				ProbeStepId: value.ProbeStepID, CompensateStepId: value.CompensateStepID,
				PriorArtifactId: value.PriorArtifactID, PriorReleaseId: value.PriorReleaseID,
				PriorTarget: value.PriorTarget, RetainedPriorArtifactId: value.RetainedPriorArtifactID,
			}
		}
		if value := inputMember.CandidateAbsence; value != nil {
			services := append([]CandidateServiceIdentity(nil), value.Services...)
			sort.Slice(services, func(left, right int) bool {
				if services[left].ServiceID == services[right].ServiceID {
					return services[left].ReleaseID < services[right].ReleaseID
				}
				return services[left].ServiceID < services[right].ServiceID
			})
			member.CandidateAbsence = &agentpb.CandidateAbsenceRestoration{
				ComposeProjectName: value.ComposeProjectName,
				ProbeStepId:        value.ProbeStepID, CompensateStepId: value.CompensateStepID,
				Services: make([]*agentpb.CandidateReleaseService, len(services)),
			}
			for serviceIndex, service := range services {
				member.CandidateAbsence.Services[serviceIndex] = &agentpb.CandidateReleaseService{
					ServiceId: service.ServiceID, ReleaseId: service.ReleaseID,
				}
			}
		}
		procedure.Members[index] = member
	}
	if err := validateCandidateReleaseProcedure(input.Operation, procedure); err != nil {
		return nil, err
	}
	return procedure, nil
}

func validateCandidateReleasePlan(plan *agentpb.ExecutionPlan, artifacts map[string]*agentpb.ComposeArtifact) error {
	if candidateReleaseOperation(plan.Operation) {
		if err := validateReleaseScriptPlan(plan, artifacts); err != nil {
			return err
		}
	}
	mutations, err := candidateReleaseMutations(plan)
	if err != nil {
		return err
	}
	procedure := plan.GetCandidateReleaseProcedure()
	if len(mutations) == 0 {
		if procedure != nil {
			return errs.New(errs.KindValidationFailed, "candidate Release procedure has no candidate mutation")
		}
		for _, step := range plan.GetSteps() {
			if step.GetCandidateRestorationProbe() != nil || step.GetCandidateRestorationCompensate() != nil {
				return errs.New(
					errs.KindValidationFailed,
					"candidate restoration step is not referenced by its procedure",
				)
			}
		}
		return nil
	}
	if procedure == nil || len(procedure.GetMembers()) != len(mutations) {
		return errs.New(errs.KindValidationFailed, "candidate Release mutations require exactly one complete procedure")
	}
	if err := validateCandidateReleaseProcedure(plan.Operation, procedure); err != nil {
		return err
	}
	stepsByID := make(map[string][]*agentpb.ExecutionStep, len(plan.Steps))
	for _, step := range plan.Steps {
		stepsByID[step.GetStepId()] = append(stepsByID[step.GetStepId()], step)
	}
	identities := make([]string, len(procedure.GetMembers()))
	referencedRestorationIDs := make(map[string]struct{}, len(procedure.GetMembers())*2)
	for index, member := range procedure.GetMembers() {
		artifact := artifacts[member.GetCandidateArtifactId()]
		if artifact == nil || mutations[member.GetServiceId()] != member.GetCandidateArtifactId() ||
			!artifactContainsCandidate(artifact, member.GetServiceId(), member.GetCandidateReleaseId()) {
			return errs.New(errs.KindValidationFailed, "candidate Release member does not bind its mutation artifact")
		}
		identities[index] = member.GetServiceId() + "\x00" + member.GetCandidateReleaseId()
		for _, stepID := range member.GetForwardStepIds() {
			steps := stepsByID[stepID]
			if len(steps) != 1 ||
				steps[0].GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD {
				return errs.New(
					errs.KindValidationFailed,
					"candidate Release forward anchor is not exactly one forward step",
				)
			}
		}
		probeID, compensateID := restorationStepIDs(member.GetServingPredecessor(), member.GetCandidateAbsence())
		if err := validateCandidateRestorationPair(member, stepsByID[probeID], stepsByID[compensateID]); err != nil {
			return err
		}
		if err := validateCandidateServingPredecessorReferences(
			member.GetServingPredecessor(), member.GetServiceId(), member.GetCandidateArtifactId(), artifacts,
		); err != nil {
			return err
		}
		referencedRestorationIDs[probeID] = struct{}{}
		referencedRestorationIDs[compensateID] = struct{}{}
	}
	for _, step := range plan.GetSteps() {
		if !candidateRestorationStep(step) {
			continue
		}
		if _, referenced := referencedRestorationIDs[step.GetStepId()]; !referenced {
			return errs.New(errs.KindValidationFailed, "candidate restoration step is not referenced by its procedure")
		}
	}
	for _, member := range procedure.GetMembers() {
		absence := member.GetCandidateAbsence()
		if absence == nil {
			continue
		}
		artifact := artifacts[member.GetCandidateArtifactId()]
		if artifact.GetProjectName() != absence.GetComposeProjectName() ||
			len(absence.GetServices()) != len(identities) {
			return errs.New(
				errs.KindValidationFailed,
				"candidate absence authority does not bind the complete candidate set",
			)
		}
		for index, service := range absence.GetServices() {
			if service.GetServiceId()+"\x00"+service.GetReleaseId() != identities[index] {
				return errs.New(
					errs.KindValidationFailed,
					"candidate absence authority does not bind the complete candidate set",
				)
			}
		}
	}
	return nil
}

// validateCandidateServingPredecessorReferences accepts either an unbound
// claim-time alternative or one complete, explicitly plan-bound historical
// reference. It deliberately does not compare the prior artifact with any
// source render artifact: a compiler may allocate a fresh prior topology.
func validateCandidateServingPredecessorReferences(
	serving *agentpb.ServingPredecessorRestoration,
	serviceID string,
	candidateArtifactID string,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if serving == nil {
		return nil
	}
	hasPrior := serving.GetPriorArtifactId() != "" || serving.GetPriorReleaseId() != "" ||
		serving.GetPriorTarget() != ""
	if !hasPrior {
		if serving.GetRetainedPriorArtifactId() != "" {
			return errs.New(errs.KindValidationFailed, "serving predecessor retained artifact lacks prior authority")
		}
		return nil
	}
	if ids.Validate(ids.KindConfig, serving.GetPriorArtifactId()) != nil ||
		ids.Validate(ids.KindDeployment, serving.GetPriorReleaseId()) != nil ||
		!validReleaseTarget(serving.GetPriorTarget()) ||
		serving.GetPriorArtifactId() == candidateArtifactID ||
		artifacts[serving.GetPriorArtifactId()] == nil {
		return errs.New(errs.KindValidationFailed, "serving predecessor prior artifact reference is invalid")
	}
	if retained := serving.GetRetainedPriorArtifactId(); retained != "" &&
		(ids.Validate(ids.KindConfig, retained) != nil || retained == serving.GetPriorArtifactId() ||
			retained == candidateArtifactID || artifacts[retained] == nil) {
		return errs.New(errs.KindValidationFailed, "serving predecessor retained artifact reference is invalid")
	}
	if err := validateNativePlanArtifactBinding(serving, serviceID, artifacts[serving.GetPriorArtifactId()]); err != nil {
		return err
	}
	if retained := serving.GetRetainedPriorArtifactId(); retained != "" {
		if err := validateNativePlanArtifactPair(
			serviceID, artifacts[serving.GetPriorArtifactId()], artifacts[retained],
		); err != nil {
			return err
		}
	}
	return nil
}

// validateNativePlanArtifactPair closes a populated current/inactive
// blue-green pair without pretending the inactive artifact has the current
// Release or slot. The shared witness validator owns the pair invariants.
func validateNativePlanArtifactPair(
	serviceID string,
	current, retained *agentpb.ComposeArtifact,
) error {
	if current == nil || retained == nil || len(current.GetServices()) == 0 || len(retained.GetServices()) == 0 {
		return nil
	}
	currentBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(current)
	if err != nil {
		return errs.New(errs.KindValidationFailed, "serving predecessor current artifact is invalid")
	}
	retainedBytes, err := (proto.MarshalOptions{Deterministic: true}).Marshal(retained)
	if err != nil {
		return errs.New(errs.KindValidationFailed, "serving predecessor retained artifact is invalid")
	}
	if err := ValidateNativePredecessorWitness(current.GetOwnerId(), serviceID, currentBytes, retainedBytes); err != nil {
		return errs.New(errs.KindValidationFailed, "serving predecessor artifact pair is invalid")
	}
	return nil
}

// validateNativePlanArtifactBinding validates populated native predecessor
// artifacts when a plan carries their concrete service topology. Empty
// artifacts remain legal claim-time placeholders for non-Blueprint callers.
func validateNativePlanArtifactBinding(
	serving *agentpb.ServingPredecessorRestoration,
	serviceID string,
	artifact *agentpb.ComposeArtifact,
) error {
	if artifact == nil || len(artifact.GetServices()) == 0 {
		return nil
	}
	var selected *agentpb.ComposeService
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID {
			if service.GetRole() == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				continue
			}
			if selected != nil {
				return errs.New(errs.KindValidationFailed, "serving predecessor artifact service is ambiguous")
			}
			selected = service
		}
	}
	if selected == nil || expectedReleaseLabel(selected) != serving.GetPriorReleaseId() {
		return errs.New(errs.KindValidationFailed, "serving predecessor artifact service identity diverges")
	}
	target := selected.GetSlot()
	if target == "" {
		target = "singleton"
	}
	if target != serving.GetPriorTarget() {
		return errs.New(errs.KindValidationFailed, "serving predecessor artifact target diverges")
	}
	return nil
}

func validateCandidateRestorationAnchor(
	member *agentpb.CandidateReleaseMember,
	steps []*agentpb.ExecutionStep,
	probe bool,
) error {
	if len(steps) != 1 {
		return errs.New(errs.KindValidationFailed, "candidate restoration anchor is not exactly one execution step")
	}
	step := steps[0]
	artifactID, serviceID, releaseID := "", "", ""
	if probe {
		value := step.GetCandidateRestorationProbe()
		if step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE ||
			value == nil {
			return errs.New(errs.KindValidationFailed, "candidate restoration probe policy or payload is invalid")
		}
		artifactID, serviceID, releaseID = value.GetCandidateArtifactId(), value.GetServiceId(), value.GetCandidateReleaseId()
	} else {
		value := step.GetCandidateRestorationCompensate()
		if step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE || value == nil {
			return errs.New(errs.KindValidationFailed, "candidate restoration compensation policy or payload is invalid")
		}
		artifactID, serviceID, releaseID = value.GetCandidateArtifactId(), value.GetServiceId(), value.GetCandidateReleaseId()
	}
	if artifactID != member.GetCandidateArtifactId() || serviceID != member.GetServiceId() ||
		releaseID != member.GetCandidateReleaseId() {
		return errs.New(errs.KindValidationFailed, "candidate restoration payload does not bind its procedure member")
	}
	return nil
}

func validateCandidateReleaseProcedure(
	operation agentpb.PlanOperation,
	procedure *agentpb.CandidateReleaseProcedure,
) error {
	if !candidateReleaseOperation(operation) || procedure == nil || len(procedure.GetMembers()) == 0 {
		return errs.New(errs.KindValidationFailed, "candidate Release procedure is invalid")
	}
	previous := ""
	for _, member := range procedure.GetMembers() {
		if member == nil || member.GetServiceId() <= previous ||
			ids.Validate(ids.KindService, member.GetServiceId()) != nil ||
			ids.Validate(ids.KindDeployment, member.GetCandidateReleaseId()) != nil ||
			ids.Validate(ids.KindConfig, member.GetCandidateArtifactId()) != nil ||
			len(member.GetForwardStepIds()) == 0 {
			return errs.New(errs.KindValidationFailed, "candidate Release member identity is invalid")
		}
		previous = member.GetServiceId()
		serving, absence := member.GetServingPredecessor(), member.GetCandidateAbsence()
		if operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
			if serving == nil || absence == nil {
				return errs.New(
					errs.KindValidationFailed,
					"Blueprint candidate Release must declare both restoration alternatives",
				)
			}
		} else if (serving == nil) == (absence == nil) {
			return errs.New(errs.KindValidationFailed, "ordinary candidate Release must declare exactly one restoration alternative")
		}
		seen := make(map[string]struct{}, len(member.GetForwardStepIds())+2)
		for _, stepID := range member.GetForwardStepIds() {
			if ids.Validate(ids.KindStep, stepID) != nil {
				return errs.New(errs.KindValidationFailed, "candidate Release forward step identity is invalid")
			}
			if _, duplicate := seen[stepID]; duplicate {
				return errs.New(errs.KindValidationFailed, "candidate Release forward step identity is duplicated")
			}
			seen[stepID] = struct{}{}
		}
		probeID, compensateID := restorationStepIDs(serving, absence)
		if ids.Validate(ids.KindStep, probeID) != nil || ids.Validate(ids.KindStep, compensateID) != nil ||
			probeID == compensateID {
			return errs.New(errs.KindValidationFailed, "candidate Release restoration step identity is invalid")
		}
		if _, overlaps := seen[probeID]; overlaps {
			return errs.New(errs.KindValidationFailed, "candidate Release restoration step overlaps forward execution")
		}
		if _, overlaps := seen[compensateID]; overlaps {
			return errs.New(errs.KindValidationFailed, "candidate Release restoration step overlaps forward execution")
		}
		if serving != nil && (serving.GetProbeStepId() != probeID || serving.GetCompensateStepId() != compensateID) {
			return errs.New(errs.KindValidationFailed, "candidate Release alternatives disagree on restoration steps")
		}
		if absence != nil {
			if absence.GetProbeStepId() != probeID || absence.GetCompensateStepId() != compensateID ||
				absence.GetComposeProjectName() == "" || len(absence.GetServices()) == 0 {
				return errs.New(errs.KindValidationFailed, "candidate absence restoration authority is incomplete")
			}
			priorIdentity := ""
			for _, service := range absence.GetServices() {
				identity := service.GetServiceId() + "\x00" + service.GetReleaseId()
				if service == nil || ids.Validate(ids.KindService, service.GetServiceId()) != nil ||
					ids.Validate(ids.KindDeployment, service.GetReleaseId()) != nil || identity <= priorIdentity {
					return errs.New(errs.KindValidationFailed, "candidate absence Service set is invalid")
				}
				priorIdentity = identity
			}
		}
	}
	return nil
}

func restorationStepIDs(
	serving *agentpb.ServingPredecessorRestoration,
	absence *agentpb.CandidateAbsenceRestoration,
) (string, string) {
	if serving != nil {
		return serving.GetProbeStepId(), serving.GetCompensateStepId()
	}
	return absence.GetProbeStepId(), absence.GetCompensateStepId()
}

func candidateReleaseMutations(plan *agentpb.ExecutionPlan) (map[string]string, error) {
	result := make(map[string]string)
	for _, step := range plan.GetSteps() {
		serviceID, artifactID := "", ""
		switch payload := step.GetPayload().(type) {
		case *agentpb.ExecutionStep_ComposeWorkloadApply:
			serviceID, artifactID = payload.ComposeWorkloadApply.GetServiceId(), payload.ComposeWorkloadApply.GetArtifactId()
		case *agentpb.ExecutionStep_ServiceProxySwitch:
			serviceID, artifactID = payload.ServiceProxySwitch.GetServiceId(), payload.ServiceProxySwitch.GetCandidateArtifactId()
		case *agentpb.ExecutionStep_ServiceRecreateAcknowledge:
			serviceID, artifactID = payload.ServiceRecreateAcknowledge.GetServiceId(), payload.ServiceRecreateAcknowledge.GetArtifactId()
		case *agentpb.ExecutionStep_ComposeApply:
			apply := payload.ComposeApply
			if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY || !apply.GetForceRecreate() || !apply.GetNoDependencies() {
				continue
			}
			for _, selectedServiceID := range apply.GetServiceIds() {
				if existing := result[selectedServiceID]; existing != "" && existing != apply.GetArtifactId() {
					return nil, errs.New(errs.KindValidationFailed, "candidate Release mutation artifacts diverge")
				}
				result[selectedServiceID] = apply.GetArtifactId()
			}
			continue
		default:
			continue
		}
		if existing := result[serviceID]; existing != "" && existing != artifactID {
			return nil, errs.New(errs.KindValidationFailed, "candidate Release mutation artifacts diverge")
		}
		result[serviceID] = artifactID
	}
	return result, nil
}

func artifactContainsCandidate(artifact *agentpb.ComposeArtifact, serviceID, releaseID string) bool {
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == serviceID && expectedReleaseLabel(service) == releaseID {
			return true
		}
	}
	return false
}

func validateCandidateRestorationStep(
	operation agentpb.PlanOperation,
	artifactID, serviceID, releaseID string,
	artifacts map[string]*agentpb.ComposeArtifact,
) error {
	if !candidateReleaseOperation(operation) || ids.Validate(ids.KindService, serviceID) != nil ||
		ids.Validate(
			ids.KindDeployment,
			releaseID,
		) != nil || !artifactContainsCandidate(artifacts[artifactID], serviceID, releaseID) {
		return errs.New(errs.KindValidationFailed, "candidate restoration step authority is invalid")
	}
	return nil
}

func candidateReleaseOperation(operation agentpb.PlanOperation) bool {
	return operation == agentpb.PlanOperation_PLAN_OPERATION_DEPLOY ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK ||
		operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
}
