package backingaccess

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/serviceobservation"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	ExchangeTimeout      = Timeout + 25*time.Second
	MaximumEnvelopeBytes = 128 << 10
	ExecutionFailure     = "execution"
	RuntimeUnavailable   = "runtime"
	OwnershipFailure     = "ownership"
	CleanupFailure       = "cleanup"
)

func HelperName(taskID string) string { return "gp-backing-check-" + taskID }

// ParametersFromMachine exposes transient bytes only during the consuming
// callback/exchange. The caller owns and clears the machine password buffer.
func ParametersFromMachine(input *agentpb.BackingAccessParameters) Request {
	if input == nil {
		return Request{}
	}
	return Request{AttachID: input.AttachId, CredentialAttachID: input.CredentialAttachId,
		EnvironmentID: input.EnvironmentId, ServiceID: input.ServiceId, BackingServiceID: input.BackingServiceId,
		Family: Family(input.Family), Authentication: Authentication(input.Authentication),
		Host: input.Host, Port: uint16(input.Port), Database: input.Database, Username: input.Username, Password: input.Password}
}

func ParametersForMachine(input Request) *agentpb.BackingAccessParameters {
	return &agentpb.BackingAccessParameters{AttachId: input.AttachID, CredentialAttachId: input.CredentialAttachID,
		EnvironmentId: input.EnvironmentID, ServiceId: input.ServiceID, BackingServiceId: input.BackingServiceID,
		Family: string(input.Family), Authentication: string(input.Authentication), Host: input.Host,
		Port: uint32(
			input.Port,
		), Database: input.Database, Username: input.Username, Password: append([]byte(nil), input.Password...)}
}

func ClearMachineRequest(request *agentpb.CheckBackingAccess) {
	if request != nil && request.Parameters != nil {
		clear(request.Parameters.Password)
		request.Parameters.Password = nil
	}
}

func ValidateMachineRequest(request *agentpb.CheckBackingAccess) error {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		ids.Validate(
			ids.KindOperation,
			"op_"+request.RequestId,
		) != nil || ids.Validate(ids.KindTask, request.TaskId) != nil ||
		!imageref.IsDigestPinned(request.HelperImage) || !validMachineContainerID(request.ConsumerContainerId) ||
		serviceobservation.ValidateTarget(
			request.ConsumerRuntime,
		) != nil || request.ConsumerRuntime.DatabaseProbe != nil ||
		request.Parameters == nil || len(request.Parameters.ProtoReflect().GetUnknown()) != 0 ||
		request.Parameters.Port > 65535 || request.ConsumerRuntime.EnvironmentId != request.Parameters.EnvironmentId ||
		request.ConsumerRuntime.ServiceId != request.Parameters.ServiceId || proto.Size(request) > MaximumEnvelopeBytes-16 {
		return invalidMachine()
	}
	input := ParametersFromMachine(request.Parameters)
	if request.CleanupOnly {
		// Cleanup authority is identity-only; it must not retain or send released
		// credentials or require a currently running consumer/database.
		if ids.Validate(ids.KindAttach, input.AttachID) != nil ||
			ids.Validate(ids.KindAttach, input.CredentialAttachID) != nil ||
			ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
			ids.Validate(ids.KindService, input.ServiceID) != nil ||
			ids.Validate(ids.KindService, input.BackingServiceID) != nil ||
			input.Family != "" ||
			input.Authentication != "" ||
			input.Host != "" ||
			input.Port != 0 ||
			input.Database != "" ||
			input.Username != "" ||
			len(input.Password) != 0 {
			return invalidMachine()
		}
		return nil
	}
	return input.Validate()
}

func ValidateMachineCancel(request *agentpb.CancelBackingAccess) error {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 ||
		ids.Validate(ids.KindOperation, "op_"+request.RequestId) != nil {
		return invalidMachine()
	}
	return nil
}

func ObservationForMachine(result Result) *agentpb.BackingAccessObservation {
	observation := &agentpb.BackingAccessObservation{
		StartedAt: timestamppb.New(result.StartedAt), FinishedAt: timestamppb.New(result.FinishedAt),
	}
	for _, stage := range result.Stages {
		observation.Stages = append(observation.Stages, &agentpb.BackingAccessStage{
			Name: string(
				stage.Name,
			), Outcome: string(stage.Outcome), Failure: string(stage.Failure), ElapsedNanoseconds: int64(stage.Elapsed),
		})
	}
	return observation
}

func ObservationFromMachine(observation *agentpb.BackingAccessObservation) (Result, error) {
	if observation == nil || len(observation.ProtoReflect().GetUnknown()) != 0 ||
		observation.StartedAt == nil || observation.FinishedAt == nil ||
		observation.StartedAt.CheckValid() != nil || observation.FinishedAt.CheckValid() != nil ||
		len(
			observation.StartedAt.ProtoReflect().GetUnknown(),
		) != 0 || len(observation.FinishedAt.ProtoReflect().GetUnknown()) != 0 ||
		len(observation.Stages) != 3 {
		return Result{}, invalidMachine()
	}
	result := Result{StartedAt: observation.StartedAt.AsTime(), FinishedAt: observation.FinishedAt.AsTime()}
	for _, stage := range observation.Stages {
		if stage == nil || len(stage.ProtoReflect().GetUnknown()) != 0 {
			return Result{}, invalidMachine()
		}
		result.Stages = append(result.Stages, Stage{Name: StageName(stage.Name), Outcome: Outcome(stage.Outcome),
			Failure: Failure(stage.Failure), Elapsed: time.Duration(stage.ElapsedNanoseconds)})
	}
	return result, result.Validate()
}

func ValidateMachineResult(request *agentpb.CheckBackingAccess, result *agentpb.BackingAccessResult) error {
	if request == nil || request.Parameters == nil || result == nil || len(result.ProtoReflect().GetUnknown()) != 0 ||
		result.RequestId != request.RequestId || result.TaskId != request.TaskId || result.HelperName != HelperName(request.TaskId) ||
		proto.Size(result) > MaximumResponseBytes || result.CleanupRequired && result.Failure == "" {
		return invalidMachine()
	}
	switch result.Failure {
	case "",
		ExecutionFailure,
		RuntimeUnavailable,
		OwnershipFailure,
		CleanupFailure,
		string(TimeoutFailure),
		string(Canceled):
	default:
		return invalidMachine()
	}
	if request.CleanupOnly {
		if result.Observation != nil || result.Failure == "" && result.CleanupRequired {
			return invalidMachine()
		}
		return nil
	}
	if result.Observation == nil {
		if result.Failure == "" {
			return invalidMachine()
		}
		return nil
	}
	observed, err := ObservationFromMachine(result.Observation)
	if err != nil {
		return err
	}
	// Result validation uses only the immutable authentication mode, never the
	// private credential buffer.
	return observed.ValidateFor(Request{Authentication: Authentication(request.Parameters.Authentication)})
}

func MachineFailure(err error) string {
	if err == nil {
		return ""
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return string(TimeoutFailure)
	}
	if errors.Is(err, context.Canceled) {
		return string(Canceled)
	}
	kind, _ := errs.KindOf(err)
	switch kind {
	case errs.KindStateConflict:
		return OwnershipFailure
	case errs.KindStorageUnavailable:
		return RuntimeUnavailable
	default:
		return ExecutionFailure
	}
}

func validMachineContainerID(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if !(character >= '0' && character <= '9') && !(character >= 'a' && character <= 'f') {
			return false
		}
	}
	return true
}

func invalidMachine() error {
	return errs.New(errs.KindValidationFailed, "Backing access machine message is invalid")
}
