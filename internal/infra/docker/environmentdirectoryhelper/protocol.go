// Package environmentdirectoryhelper owns the framed protocol and the
// descriptor-relative procedure for one Environment directory helper.
package environmentdirectoryhelper

import (
	"context"
	"encoding/binary"
	"io"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersion      = 1
	VolumeRootEnv      = "GROUNDPLANE_VOLUME_ROOT"
	maximumFramedBytes = executionplan.MaximumPlanBytes + 64*1024
	frameHeaderBytes   = 4
	maximumTimeout     = uint32(math.MaxInt32)
)

type DirectoryCreator interface {
	Create(context.Context, string, string) error
}

type ManagedVolumeDirectoryCreator interface {
	EnsureManagedVolumes(context.Context, string, string, []string) error
}

type DirectoryRemover interface {
	Remove(context.Context, string, string) error
}

func MarshalRequest(request *agentpb.EnvironmentDirectoryHelperRequest) ([]byte, error) {
	owned, _, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(encoded, "request")
}

func ReadRequest(ctx context.Context, input io.Reader) (*agentpb.EnvironmentDirectoryHelperRequest, error) {
	encoded, err := readFrame(ctx, input, "request")
	if err != nil {
		return nil, err
	}
	request := &agentpb.EnvironmentDirectoryHelperRequest{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, request); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Environment directory helper request protobuf is invalid")
	}
	owned, _, err := validateRequest(request)
	return owned, err
}

func WriteResponse(ctx context.Context, output io.Writer, response *agentpb.EnvironmentDirectoryHelperResponse) error {
	if ctx == nil || output == nil {
		return errs.New(errs.KindInternal, "Environment directory helper response output is not configured")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateResponse(response); err != nil {
		return err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	framed, err := frame(encoded, "response")
	if err != nil {
		return err
	}
	for len(framed) != 0 {
		written, writeErr := output.Write(framed)
		if writeErr != nil {
			return errs.Wrap(errs.KindInternal, writeErr)
		}
		if written <= 0 {
			return errs.New(errs.KindInternal, "Environment directory helper response writer made no progress")
		}
		framed = framed[written:]
	}
	return nil
}

func ReadResponse(ctx context.Context, input io.Reader) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	encoded, err := readFrame(ctx, input, "response")
	if err != nil {
		return nil, err
	}
	response := &agentpb.EnvironmentDirectoryHelperResponse{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(encoded, response); err != nil {
		return nil, errs.New(errs.KindValidationFailed, "Environment directory helper response protobuf is invalid")
	}
	if err := validateResponse(response); err != nil {
		return nil, err
	}
	return response, nil
}

func Execute(
	ctx context.Context,
	volumeRoot string,
	creator DirectoryCreator,
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	if ctx == nil || creator == nil {
		return nil, errs.New(errs.KindInternal, "Environment directory helper is not configured")
	}
	owned, step, err := validateRequest(request)
	if err != nil {
		return nil, err
	}
	if err := executionplan.AuthorizeVolumeDirectories(owned.Plan, volumeRoot); err != nil {
		return nil, err
	}
	executionCtx, cancel := context.WithTimeout(ctx, time.Duration(owned.TimeoutSeconds)*time.Second)
	defer cancel()
	if err := executeDirectoryMutation(executionCtx, volumeRoot, creator, owned.Plan, step); err != nil {
		if contextErr := executionCtx.Err(); contextErr != nil {
			return nil, contextErr
		}
		return &agentpb.EnvironmentDirectoryHelperResponse{
			Schema: SchemaVersion, ExitCode: 1, FailedStepId: step.StepId,
		}, nil
	}
	return &agentpb.EnvironmentDirectoryHelperResponse{Schema: SchemaVersion}, nil
}

func executeDirectoryMutation(
	ctx context.Context,
	volumeRoot string,
	creator DirectoryCreator,
	plan *agentpb.ExecutionPlan,
	step *agentpb.ExecutionStep,
) error {
	if create := step.GetEnvironmentDirectoryCreate(); create != nil {
		return creator.Create(ctx, volumeRoot, create.ExpectedVolumeDir)
	}
	if remove := step.GetEnvironmentDirectoryRemove(); remove != nil {
		remover, ok := creator.(DirectoryRemover)
		if !ok {
			return errs.New(errs.KindInternal, "Environment directory remover is not configured")
		}
		return remover.Remove(ctx, volumeRoot, remove.ExpectedVolumeDir)
	}
	ensure := step.GetManagedVolumeDirectoriesEnsure()
	managedCreator, ok := creator.(ManagedVolumeDirectoryCreator)
	if ensure == nil || !ok {
		return errs.New(errs.KindInternal, "Managed volume directory creator is not configured")
	}
	for _, artifact := range plan.Artifacts {
		if artifact.ArtifactId != ensure.ArtifactId {
			continue
		}
		names := make([]string, 0, len(artifact.Volumes))
		for _, volume := range artifact.Volumes {
			names = append(names, volume.ComposeName)
		}
		return managedCreator.EnsureManagedVolumes(
			ctx,
			volumeRoot,
			artifact.AuthorizedVolumeDir,
			names,
		)
	}
	return errs.New(errs.KindInternal, "Managed volume directory artifact disappeared after validation")
}

func validateRequest(
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperRequest, *agentpb.ExecutionStep, error) {
	if request == nil || request.Schema != SchemaVersion {
		return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper request schema is unsupported")
	}
	if err := executionplan.RejectUnknown(request); err != nil {
		return nil, nil, err
	}
	if ids.Validate(ids.KindTask, request.TaskId) != nil ||
		ids.Validate(ids.KindOperation, request.OperationId) != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper task identity is invalid")
	}
	if request.TimeoutSeconds == 0 || request.TimeoutSeconds > maximumTimeout {
		return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper timeout is invalid")
	}
	plan, err := executionplan.Validate(request.Plan)
	if err != nil {
		return nil, nil, err
	}
	var step *agentpb.ExecutionStep
	for _, candidate := range plan.Steps {
		if candidate.StepId == request.StepId {
			step = candidate
			break
		}
	}
	if step == nil || request.TimeoutSeconds > step.TimeoutSeconds {
		return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper step selection is invalid")
	}
	if step.GetEnvironmentDirectoryCreate() != nil {
		if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE || len(plan.Steps) != 1 {
			return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper plan is unsupported")
		}
	} else if step.GetEnvironmentDirectoryRemove() != nil {
		if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE {
			return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper plan is unsupported")
		}
	} else if step.GetManagedVolumeDirectoriesEnsure() == nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper step is unsupported")
	}
	owned := proto.Clone(request).(*agentpb.EnvironmentDirectoryHelperRequest)
	owned.Plan = plan
	for _, candidate := range owned.Plan.Steps {
		if candidate.StepId == request.StepId {
			return owned, candidate, nil
		}
	}
	return nil, nil, errs.New(errs.KindInternal, "Environment directory helper step disappeared after cloning")
}

func validateResponse(response *agentpb.EnvironmentDirectoryHelperResponse) error {
	if response == nil || response.Schema != SchemaVersion {
		return errs.New(errs.KindValidationFailed, "Environment directory helper response schema is unsupported")
	}
	if err := executionplan.RejectUnknown(response); err != nil {
		return err
	}
	if response.ExitCode == 0 {
		if response.FailedStepId != "" {
			return errs.New(errs.KindValidationFailed, "successful Environment directory helper response is inconsistent")
		}
		return nil
	}
	if response.ExitCode < 0 || ids.Validate(ids.KindStep, response.FailedStepId) != nil {
		return errs.New(errs.KindValidationFailed, "failed Environment directory helper response is inconsistent")
	}
	return nil
}

func frame(encoded []byte, kind string) ([]byte, error) {
	if len(encoded) == 0 || len(encoded) > maximumFramedBytes {
		return nil, errs.Newf(errs.KindValidationFailed, "Environment directory helper %s frame length is invalid", kind)
	}
	framed := make([]byte, frameHeaderBytes+len(encoded))
	binary.BigEndian.PutUint32(framed[:frameHeaderBytes], uint32(len(encoded)))
	copy(framed[frameHeaderBytes:], encoded)
	return framed, nil
}

func readFrame(ctx context.Context, input io.Reader, kind string) ([]byte, error) {
	if ctx == nil || input == nil {
		return nil, errs.New(errs.KindInternal, "Environment directory helper frame input is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var header [frameHeaderBytes]byte
	if _, err := io.ReadFull(input, header[:]); err != nil {
		return nil, errs.Newf(errs.KindValidationFailed, "Environment directory helper %s frame header is incomplete", kind)
	}
	length := binary.BigEndian.Uint32(header[:])
	if length == 0 || length > maximumFramedBytes {
		return nil, errs.Newf(errs.KindValidationFailed, "Environment directory helper %s frame length is invalid", kind)
	}
	encoded := make([]byte, int(length))
	if _, err := io.ReadFull(input, encoded); err != nil {
		return nil, errs.Newf(errs.KindValidationFailed, "Environment directory helper %s frame payload is incomplete", kind)
	}
	extra, err := io.ReadAll(io.LimitReader(input, 1))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(extra) != 0 {
		return nil, errs.Newf(errs.KindValidationFailed, "Environment directory helper %s frame has trailing bytes", kind)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return encoded, nil
}
