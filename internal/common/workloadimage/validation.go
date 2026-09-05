// Package workloadimage validates the closed, read-only Agent image exchange.
package workloadimage

import (
	"crypto/sha256"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/distribution/reference"
	"google.golang.org/protobuf/proto"
)

const (
	MaximumSelectors     = 64
	MaximumEnvelopeBytes = 65536
	Timeout              = 30 * time.Second
)

func LocalIDValid(value string) bool {
	return strings.HasPrefix(value, "sha256:") && lowerHex(value[7:], sha256.Size*2)
}

func lowerHex(value string, length int) bool {
	if len(value) != length {
		return false
	}
	for _, char := range value {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func SelectorValue(selector *agentpb.WorkloadImageSelector) (string, error) {
	if selector == nil || len(selector.ProtoReflect().GetUnknown()) != 0 {
		return "", invalid()
	}
	switch value := selector.Selector.(type) {
	case *agentpb.WorkloadImageSelector_LocalImageId:
		if value != nil && LocalIDValid(value.LocalImageId) {
			return value.LocalImageId, nil
		}
	case *agentpb.WorkloadImageSelector_RequestedReference:
		if value == nil || len(value.RequestedReference) > 512 {
			return "", invalid()
		}
		for _, char := range value.RequestedReference {
			if char > 127 {
				return "", invalid()
			}
		}
		named, err := reference.ParseNormalizedNamed(value.RequestedReference)
		if err == nil && !reference.IsNameOnly(named) {
			return value.RequestedReference, nil
		}
	}
	return "", invalid()
}

func ValidateRequest(request *agentpb.ResolveWorkloadImages) error {
	if request == nil || !lowerHex(request.RequestId, 32) || request.RequestId == strings.Repeat("0", 32) ||
		len(request.Selectors) == 0 || len(request.Selectors) > MaximumSelectors ||
		len(request.ProtoReflect().GetUnknown()) != 0 {
		return invalid()
	}
	envelope := &agentpb.ControllerMessage{
		Payload: &agentpb.ControllerMessage_ResolveWorkloadImages{ResolveWorkloadImages: request},
	}
	if proto.Size(envelope) > MaximumEnvelopeBytes {
		return invalid()
	}
	seen := make(map[string]bool, len(request.Selectors))
	for _, selector := range request.Selectors {
		value, err := SelectorValue(selector)
		if err != nil {
			return err
		}
		key := "reference:" + value
		if selector.GetLocalImageId() != "" {
			key = "local:" + value
		}
		if seen[key] {
			return invalid()
		}
		seen[key] = true
	}
	return nil
}

func ValidateResult(request *agentpb.ResolveWorkloadImages, result *agentpb.WorkloadImageResolutionResult) error {
	if err := ValidateRequest(request); err != nil {
		return err
	}
	if result == nil || result.RequestId != request.RequestId || len(result.ProtoReflect().GetUnknown()) != 0 {
		return invalid()
	}
	envelope := &agentpb.AgentMessage{
		Payload: &agentpb.AgentMessage_WorkloadImageResolutionResult{WorkloadImageResolutionResult: result},
	}
	if proto.Size(envelope) > MaximumEnvelopeBytes {
		return invalid()
	}
	switch outcome := result.Outcome.(type) {
	case *agentpb.WorkloadImageResolutionResult_Success:
		if outcome == nil || outcome.Success == nil || len(outcome.Success.ProtoReflect().GetUnknown()) != 0 ||
			len(outcome.Success.Resolutions) != len(request.Selectors) {
			return invalid()
		}
		for index, resolution := range outcome.Success.Resolutions {
			if resolution == nil || len(resolution.ProtoReflect().GetUnknown()) != 0 ||
				!proto.Equal(resolution.Selector, request.Selectors[index]) || !LocalIDValid(resolution.LocalImageId) {
				return invalid()
			}
			if local := request.Selectors[index].GetLocalImageId(); local != "" && local != resolution.LocalImageId {
				return invalid()
			}
		}
		return nil
	case *agentpb.WorkloadImageResolutionResult_Failure:
		if outcome == nil || outcome.Failure == nil {
			return invalid()
		}
		failure := outcome.Failure
		if failure.SelectorOrdinal == nil || int(*failure.SelectorOrdinal) >= len(request.Selectors) ||
			len(failure.ProtoReflect().GetUnknown()) != 0 {
			return invalid()
		}
		switch failure.Kind {
		case agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_NOT_FOUND,
			agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_IDENTITY_MISMATCH,
			agentpb.WorkloadImageResolutionFailureKind_WORKLOAD_IMAGE_RESOLUTION_FAILURE_KIND_OBSERVATION_FAILED:
			return nil
		}
	}
	return invalid()
}

func invalid() error {
	return errs.New(errs.KindValidationFailed, "invalid workload image resolution exchange")
}
