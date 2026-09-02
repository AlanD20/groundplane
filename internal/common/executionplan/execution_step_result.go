package executionplan

import (
	"bytes"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/distribution/reference"
	"google.golang.org/protobuf/proto"
)

func SealExecutionStepResult(value *agentpb.ExecutionStepResult) (*agentpb.ExecutionStepResult, error) {
	if value == nil || len(value.GetControlPayloadSha256()) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result must be unhashed")
	}
	owned := proto.Clone(value).(*agentpb.ExecutionStepResult)
	if err := validateExecutionStepResultShape(owned); err != nil {
		return nil, err
	}
	encoded, err := executionStepResultHashInput(owned)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	owned.ControlPayloadSha256 = append([]byte(nil), digest[:]...)
	return owned, nil
}

func ValidateExecutionStepResult(value *agentpb.ExecutionStepResult) (*agentpb.ExecutionStepResult, error) {
	if value == nil || len(value.GetControlPayloadSha256()) != sha256.Size {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result digest is invalid")
	}
	owned := proto.Clone(value).(*agentpb.ExecutionStepResult)
	if err := validateExecutionStepResultShape(owned); err != nil {
		return nil, err
	}
	encoded, err := executionStepResultHashInput(owned)
	if err != nil {
		return nil, err
	}
	digest := sha256.Sum256(encoded)
	if subtle.ConstantTimeCompare(digest[:], owned.GetControlPayloadSha256()) != 1 {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result digest does not match")
	}
	return owned, nil
}

func ValidateExecutionStepResultRequest(
	request *agentpb.ExecutionStepResultRequest,
) (*agentpb.ExecutionStepResultRequest, error) {
	if request == nil || RejectUnknown(request) != nil ||
		validateID(ids.KindTask, request.GetTaskId()) != nil ||
		validateID(ids.KindAssignment, request.GetAssignmentId()) != nil {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result request is invalid")
	}
	result, err := ValidateExecutionStepResult(request.GetResult())
	if err != nil {
		return nil, err
	}
	owned := proto.Clone(request).(*agentpb.ExecutionStepResultRequest)
	owned.Result = result
	return owned, nil
}

func ValidateExecutionStepResultAck(
	ack *agentpb.ExecutionStepResultAck,
	request *agentpb.ExecutionStepResultRequest,
) (*agentpb.ExecutionStepResultAck, error) {
	validated, err := ValidateExecutionStepResultRequest(request)
	if err != nil || ack == nil || RejectUnknown(ack) != nil {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result acknowledgement is invalid")
	}
	result := validated.GetResult()
	if ack.GetTaskId() != validated.GetTaskId() ||
		ack.GetAssignmentId() != validated.GetAssignmentId() ||
		ack.GetOperationId() != result.GetOperationId() ||
		!bytes.Equal(ack.GetPlanHash(), result.GetPlanHash()) ||
		ack.GetStepId() != result.GetStepId() ||
		!bytes.Equal(ack.GetControlPayloadSha256(), result.GetControlPayloadSha256()) {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result acknowledgement identity is invalid")
	}
	return proto.Clone(ack).(*agentpb.ExecutionStepResultAck), nil
}

func ValidateExecutionStepResults(
	plan *agentpb.ExecutionPlan,
	values []*agentpb.ExecutionStepResult,
) ([]*agentpb.ExecutionStepResult, error) {
	seen := make(map[string]struct{}, len(values))
	result := make([]*agentpb.ExecutionStepResult, len(values))
	for index, value := range values {
		validated, err := ValidateExecutionStepResultForPlan(plan, value)
		if err != nil {
			return nil, err
		}
		if _, duplicate := seen[validated.GetStepId()]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "execution-step result set contains a duplicate step")
		}
		seen[validated.GetStepId()] = struct{}{}
		result[index] = validated
	}
	return result, nil
}

func ValidateExecutionStepResultForPlan(
	plan *agentpb.ExecutionPlan,
	value *agentpb.ExecutionStepResult,
) (*agentpb.ExecutionStepResult, error) {
	validatedPlan, err := Validate(plan)
	if err != nil {
		return nil, err
	}
	validated, err := ValidateExecutionStepResult(value)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(validated.GetPlanHash(), validatedPlan.GetPlanHash()) {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result plan hash differs")
	}
	authority, err := procedureServiceImageAuthorityForStep(validatedPlan, validated.GetStepId())
	if err != nil {
		return nil, err
	}
	evidence := validated.GetProcedureServiceImage()
	if evidence == nil || authority.GetServiceId() != evidence.GetServiceId() ||
		authority.GetReleaseId() != evidence.GetReleaseId() ||
		authority.GetRequestedReference() != evidence.GetRequestedReference() {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result evidence differs from its authority")
	}
	return validated, nil
}

func FindProcedureServiceImageResult(
	values []*agentpb.ExecutionStepResult,
	authority *agentpb.ProcedureServiceImageAuthority,
) (*agentpb.ProcedureServiceImageResult, bool, error) {
	if authority == nil {
		return nil, false, errs.New(errs.KindValidationFailed, "procedure-service image authority is missing")
	}
	for _, value := range values {
		if value == nil || value.GetStepId() != authority.GetComposeApplyStepId() {
			continue
		}
		validated, err := ValidateExecutionStepResult(value)
		if err != nil {
			return nil, false, err
		}
		evidence := validated.GetProcedureServiceImage()
		if evidence == nil || evidence.GetServiceId() != authority.GetServiceId() ||
			evidence.GetReleaseId() != authority.GetReleaseId() ||
			evidence.GetRequestedReference() != authority.GetRequestedReference() {
			return nil, false, errs.New(errs.KindStateConflict, "acknowledged procedure-service image differs from its authority")
		}
		return proto.Clone(evidence).(*agentpb.ProcedureServiceImageResult), true, nil
	}
	return nil, false, nil
}

func FindProcedureServiceImageAuthority(
	plan *agentpb.ExecutionPlan,
	stepID string,
) (*agentpb.ProcedureServiceImageAuthority, bool, error) {
	if plan == nil {
		return nil, false, nil
	}
	for _, snapshot := range plan.ScriptRunnerSnapshots {
		if snapshot != nil && snapshot.ProcedureServiceImage != nil &&
			snapshot.ProcedureServiceImage.GetComposeApplyStepId() == stepID {
			authority, err := procedureServiceImageAuthorityForStep(plan, stepID)
			return authority, err == nil, err
		}
	}
	return nil, false, nil
}

func procedureServiceImageAuthorityForStep(
	plan *agentpb.ExecutionPlan,
	stepID string,
) (*agentpb.ProcedureServiceImageAuthority, error) {
	var authority *agentpb.ProcedureServiceImageAuthority
	for _, snapshot := range plan.GetScriptRunnerSnapshots() {
		candidate := snapshot.GetProcedureServiceImage()
		if candidate == nil || candidate.GetComposeApplyStepId() != stepID {
			continue
		}
		if authority == nil {
			authority = proto.Clone(candidate).(*agentpb.ProcedureServiceImageAuthority)
		} else if !proto.Equal(authority, candidate) {
			return nil, errs.New(errs.KindValidationFailed, "ComposeApply has conflicting procedure-service image authorities")
		}
	}
	if authority == nil {
		return nil, errs.New(errs.KindValidationFailed, "execution-step result has no sealed procedure authority")
	}
	var step *agentpb.ExecutionStep
	for _, candidate := range plan.GetSteps() {
		if candidate.GetStepId() == stepID {
			step = candidate
			break
		}
	}
	apply := step.GetComposeApply()
	if step == nil || apply == nil || apply.GetArtifactId() != authority.GetArtifactId() ||
		!composeApplySelectsService(apply, authority.GetServiceId()) {
		return nil, errs.New(errs.KindValidationFailed, "procedure-service image authority does not identify its ComposeApply")
	}
	var artifact *agentpb.ComposeArtifact
	for _, candidate := range plan.GetArtifacts() {
		if candidate.GetArtifactId() == authority.GetArtifactId() {
			artifact = candidate
			break
		}
	}
	matches := 0
	for _, service := range artifact.GetServices() {
		if service.GetServiceId() == authority.GetServiceId() &&
			expectedReleaseLabel(service) == authority.GetReleaseId() &&
			service.GetImageReference() == authority.GetRequestedReference() {
			matches++
		}
	}
	if matches != 1 {
		return nil, errs.New(errs.KindValidationFailed, "procedure-service image authority does not identify one candidate service")
	}
	return authority, nil
}

func composeApplySelectsService(apply *agentpb.ComposeApply, serviceID string) bool {
	if apply.GetFullReconcile() {
		return true
	}
	for _, candidate := range apply.GetServiceIds() {
		if candidate == serviceID {
			return true
		}
	}
	return false
}

func validateExecutionStepResultShape(value *agentpb.ExecutionStepResult) error {
	if RejectUnknown(value) != nil || validateID(ids.KindOperation, value.GetOperationId()) != nil ||
		len(value.GetPlanHash()) != sha256.Size || validateID(ids.KindStep, value.GetStepId()) != nil {
		return errs.New(errs.KindValidationFailed, "execution-step result identity is invalid")
	}
	evidence := value.GetProcedureServiceImage()
	if evidence == nil || validateID(ids.KindService, evidence.GetServiceId()) != nil ||
		validateID(ids.KindDeployment, evidence.GetReleaseId()) != nil ||
		!validRequestedImageReference(evidence.GetRequestedReference()) ||
		!validImmutableImageEvidence(
			evidence.GetRequestedReference(), evidence.GetImmutableReference(),
			evidence.GetImageDigest(), evidence.GetLocalImageId(),
		) {
		return errs.New(errs.KindValidationFailed, "procedure-service image result is invalid")
	}
	return nil
}

func executionStepResultHashInput(value *agentpb.ExecutionStepResult) ([]byte, error) {
	owned := proto.Clone(value).(*agentpb.ExecutionStepResult)
	owned.ControlPayloadSha256 = nil
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return encoded, nil
}

func validRequestedImageReference(value string) bool {
	if value == "" || strings.TrimSpace(value) != value {
		return false
	}
	named, err := reference.ParseNormalizedNamed(value)
	return err == nil && named.String() == value
}

func validImmutableImageEvidence(requested, immutable string, digestBytes []byte, localID string) bool {
	if !validRequestedImageReference(requested) || !imageref.IsDigestPinned(immutable) ||
		len(digestBytes) != sha256.Size || !validLocalImageID(localID) {
		return false
	}
	separator := strings.LastIndex(immutable, "@sha256:")
	decoded, err := hex.DecodeString(immutable[separator+len("@sha256:"):])
	if err != nil || !bytes.Equal(decoded, digestBytes) {
		return false
	}
	requestedNamed, requestErr := reference.ParseNormalizedNamed(requested)
	immutableNamed, immutableErr := reference.ParseNormalizedNamed(immutable)
	return requestErr == nil && immutableErr == nil &&
		reference.TrimNamed(requestedNamed).String() == reference.TrimNamed(immutableNamed).String()
}

func validLocalImageID(value string) bool {
	if !strings.HasPrefix(value, "sha256:") || len(value) != len("sha256:")+sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(strings.TrimPrefix(value, "sha256:"))
	return err == nil && len(decoded) == sha256.Size
}
