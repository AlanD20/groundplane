// Package environmentdirectoryhelper owns the framed protocol and the
// descriptor-relative procedure for one Environment directory helper.
package environmentdirectoryhelper

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	SchemaVersion        = 1
	VolumeRootEnv        = "GROUNDPLANE_VOLUME_ROOT"
	maximumFramedBytes   = executionplan.MaximumPlanBytes + 64*1024
	maximumResponseBytes = 768 * 1024
	frameHeaderBytes     = 4
	maximumTimeout       = uint32(math.MaxInt32)
)

type DirectoryCreator interface {
	Create(context.Context, string, string) error
}

type ManagedVolume struct {
	ID  string
	Key string
}

type ManagedVolumeEnsureRequest struct {
	TaskID       string
	OperationID  string
	IntentSHA256 []byte
	VolumeRoot   string
	VolumeDir    string
	Volumes      []ManagedVolume
}

type ManagedVolumeDirectoryCreator interface {
	EnsureManagedVolumes(context.Context, ManagedVolumeEnsureRequest) error
}

type DirectoryRemover interface {
	Remove(context.Context, string, string) error
}

type ManagedVolumeDirectoryRemoveRequest struct {
	TaskID       string
	OperationID  string
	IntentSHA256 []byte
	Cursor       []byte
	VolumeRoot   string
	VolumeDir    string
	VolumeID     string
	ComposeKey   string
}

type ManagedVolumeDirectoryRemoveResult struct {
	NextCursor     []byte
	MutationCount  uint32
	Complete       bool
	ResponseSHA256 []byte
}

type ManagedVolumeDirectoryRemover interface {
	RemoveManagedVolume(
		context.Context,
		ManagedVolumeDirectoryRemoveRequest,
	) (ManagedVolumeDirectoryRemoveResult, error)
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
	owned, err := withResponseDigest(response)
	if err != nil {
		return err
	}
	if err := validateResponse(owned); err != nil {
		return err
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
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
	timeout := time.Duration(owned.TimeoutSeconds) * time.Second
	if step.GetManagedVolumeDirectoryRemove() != nil && timeout > 30*time.Second {
		timeout = 30 * time.Second
	}
	executionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	result, err := executeDirectoryMutation(executionCtx, volumeRoot, creator, owned, step)
	if err != nil {
		if contextErr := executionCtx.Err(); contextErr != nil {
			return nil, contextErr
		}
		_, _ = fmt.Fprintf(os.Stderr, "Environment directory helper mutation failed: %v\n", err)
		return withResponseDigest(&agentpb.EnvironmentDirectoryHelperResponse{
			Schema: SchemaVersion, ExitCode: 1, FailedStepId: step.StepId,
		})
	}
	response := &agentpb.EnvironmentDirectoryHelperResponse{
		Schema:         SchemaVersion,
		NextCursor:     result.NextCursor,
		MutationCount:  result.MutationCount,
		Complete:       result.Complete,
		ResponseSha256: result.ResponseSHA256,
	}
	return withResponseDigest(response)
}

func executeDirectoryMutation(
	ctx context.Context,
	volumeRoot string,
	creator DirectoryCreator,
	request *agentpb.EnvironmentDirectoryHelperRequest,
	step *agentpb.ExecutionStep,
) (ManagedVolumeDirectoryRemoveResult, error) {
	if create := step.GetEnvironmentDirectoryCreate(); create != nil {
		return ManagedVolumeDirectoryRemoveResult{
				Complete: true,
			}, creator.Create(
				ctx,
				volumeRoot,
				create.ExpectedVolumeDir,
			)
	}
	if remove := step.GetEnvironmentDirectoryRemove(); remove != nil {
		remover, ok := creator.(DirectoryRemover)
		if !ok {
			return ManagedVolumeDirectoryRemoveResult{}, errs.New(
				errs.KindInternal,
				"Environment directory remover is not configured",
			)
		}
		return ManagedVolumeDirectoryRemoveResult{
				Complete: true,
			}, remover.Remove(
				ctx,
				volumeRoot,
				remove.ExpectedVolumeDir,
			)
	}
	ensure := step.GetManagedVolumeDirectoriesEnsure()
	managedCreator, ok := creator.(ManagedVolumeDirectoryCreator)
	if ensure == nil || !ok {
		if remove := step.GetManagedVolumeDirectoryRemove(); remove != nil {
			managedRemover, removerOK := creator.(ManagedVolumeDirectoryRemover)
			if !removerOK {
				return ManagedVolumeDirectoryRemoveResult{}, errs.New(
					errs.KindInternal,
					"Managed volume directory remover is not configured",
				)
			}
			for _, artifact := range request.Plan.Artifacts {
				if artifact.ArtifactId != remove.ArtifactId {
					continue
				}
				return managedRemover.RemoveManagedVolume(ctx, ManagedVolumeDirectoryRemoveRequest{
					TaskID: request.TaskId, OperationID: request.OperationId,
					IntentSHA256: append(
						[]byte(nil),
						remove.IntentSha256...), Cursor: append([]byte(nil), remove.Cursor...),
					VolumeRoot: volumeRoot, VolumeDir: artifact.AuthorizedVolumeDir,
					VolumeID: remove.VolumeId, ComposeKey: remove.ComposeKey,
				})
			}
			return ManagedVolumeDirectoryRemoveResult{}, errs.New(
				errs.KindInternal,
				"Managed volume directory removal artifact disappeared after validation",
			)
		}
		return ManagedVolumeDirectoryRemoveResult{}, errs.New(
			errs.KindInternal,
			"Managed volume directory creator is not configured",
		)
	}
	for _, artifact := range request.Plan.Artifacts {
		if artifact.ArtifactId != ensure.ArtifactId {
			continue
		}
		byID := make(map[string]string, len(artifact.Volumes))
		for _, volume := range artifact.Volumes {
			if volume != nil {
				byID[volume.VolumeId] = volume.ComposeName
			}
		}
		volumes := make([]ManagedVolume, 0, len(ensure.VolumeIds))
		for _, volumeID := range ensure.VolumeIds {
			volumes = append(volumes, ManagedVolume{ID: volumeID, Key: byID[volumeID]})
		}
		return ManagedVolumeDirectoryRemoveResult{
				Complete: true,
			}, managedCreator.EnsureManagedVolumes(
				ctx,
				ManagedVolumeEnsureRequest{
					TaskID: request.TaskId, OperationID: request.OperationId,
					IntentSHA256: append([]byte(nil), ensure.IntentSha256...), VolumeRoot: volumeRoot,
					VolumeDir: artifact.AuthorizedVolumeDir, Volumes: volumes,
				},
			)
	}
	return ManagedVolumeDirectoryRemoveResult{}, errs.New(
		errs.KindInternal,
		"Managed volume directory artifact disappeared after validation",
	)
}

func validateRequest(
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperRequest, *agentpb.ExecutionStep, error) {
	if request == nil || request.Schema != SchemaVersion {
		return nil, nil, errs.New(
			errs.KindValidationFailed,
			"Environment directory helper request schema is unsupported",
		)
	}
	if err := executionplan.RejectUnknown(request); err != nil {
		return nil, nil, err
	}
	if ids.Validate(ids.KindAssignment, request.AssignmentId) != nil ||
		ids.Validate(ids.KindTask, request.TaskId) != nil ||
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
		if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE {
			return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper plan is unsupported")
		}
	} else if step.GetEnvironmentDirectoryRemove() != nil {
		if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_REMOVE {
			return nil, nil, errs.New(errs.KindValidationFailed, "Environment directory helper plan is unsupported")
		}
	} else if step.GetManagedVolumeDirectoriesEnsure() == nil && step.GetManagedVolumeDirectoryRemove() == nil {
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
	if len(response.NextCursor) > 16*1024 || response.MutationCount > 128 {
		return errs.New(errs.KindValidationFailed, "Environment directory helper progress is outside its bounds")
	}
	if len(response.ResponseSha256) != sha256.Size {
		return errs.New(errs.KindValidationFailed, "Environment directory helper response digest is invalid")
	}
	withoutDigest := proto.Clone(response).(*agentpb.EnvironmentDirectoryHelperResponse)
	withoutDigest.ResponseSha256 = nil
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(withoutDigest)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	if subtle.ConstantTimeCompare(response.ResponseSha256, digest[:]) != 1 {
		return errs.New(
			errs.KindValidationFailed,
			"Environment directory helper response digest does not match its contents",
		)
	}
	if response.ExitCode == 0 {
		if response.FailedStepId != "" {
			return errs.New(
				errs.KindValidationFailed,
				"successful Environment directory helper response is inconsistent",
			)
		}
		if response.Complete && len(response.NextCursor) != 0 {
			return errs.New(
				errs.KindValidationFailed,
				"complete Environment directory helper response carries a cursor",
			)
		}
		if !response.Complete && len(response.NextCursor) == 0 && response.MutationCount != 0 {
			return errs.New(
				errs.KindValidationFailed,
				"incomplete Environment directory helper response lacks a cursor",
			)
		}
		return nil
	}
	if response.ExitCode < 0 || ids.Validate(ids.KindStep, response.FailedStepId) != nil {
		return errs.New(errs.KindValidationFailed, "failed Environment directory helper response is inconsistent")
	}
	return nil
}

func withResponseDigest(
	response *agentpb.EnvironmentDirectoryHelperResponse,
) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	if response == nil {
		return nil, errs.New(errs.KindValidationFailed, "Environment directory helper response is required")
	}
	owned := proto.Clone(response).(*agentpb.EnvironmentDirectoryHelperResponse)
	owned.ResponseSha256 = nil
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(owned)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	owned.ResponseSha256 = append([]byte(nil), digest[:]...)
	return owned, nil
}

func frame(encoded []byte, kind string) ([]byte, error) {
	maximum := maximumFramedBytes
	if kind == "response" {
		maximum = maximumResponseBytes
	}
	if len(encoded) == 0 || len(encoded) > maximum {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"Environment directory helper %s frame length is invalid",
			kind,
		)
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
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"Environment directory helper %s frame header is incomplete",
			kind,
		)
	}
	length := binary.BigEndian.Uint32(header[:])
	maximum := uint32(maximumFramedBytes)
	if kind == "response" {
		maximum = maximumResponseBytes
	}
	if length == 0 || length > maximum {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"Environment directory helper %s frame length is invalid",
			kind,
		)
	}
	encoded := make([]byte, int(length))
	if _, err := io.ReadFull(input, encoded); err != nil {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"Environment directory helper %s frame payload is incomplete",
			kind,
		)
	}
	extra, err := io.ReadAll(io.LimitReader(input, 1))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	if len(extra) != 0 {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"Environment directory helper %s frame has trailing bytes",
			kind,
		)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return encoded, nil
}
