package composehelper

import (
	"context"
	"io"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func MarshalEnvironmentProvisionRequest(request *agentpb.EnvironmentProvisionHelperRequest) ([]byte, error) {
	if _, err := environmentexecution.OpenHelper(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadEnvironmentProvisionRequest(
	ctx context.Context,
	input io.Reader,
) (*agentpb.EnvironmentProvisionHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > environmentexecution.MaximumInputBytes+8192 {
		return nil, errs.New(errs.KindValidationFailed, "environment provisioning helper request is oversized")
	}
	request := &agentpb.EnvironmentProvisionHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "environment provisioning helper request is invalid")
	}
	if _, err := environmentexecution.OpenHelper(request); err != nil {
		return nil, err
	}
	return request, nil
}

func ExecuteEnvironmentProvision(
	ctx context.Context,
	volumeRoot string,
	request *agentpb.EnvironmentProvisionHelperRequest,
) (*agentpb.EnvironmentProvisionHelperResponse, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "environment provisioning helper context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	input, err := environmentexecution.OpenHelper(request)
	if err != nil {
		return nil, err
	}
	if input.Creation.VolumeRoot != volumeRoot {
		return nil, errs.New(errs.KindStateConflict, "environment provisioning root differs from host policy")
	}
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	if !request.InspectOnly {
		cancel()
		bounded, cancel = context.WithDeadline(ctx, input.Attempt.Deadline)
	}
	defer cancel()
	directory := environmentdirectoryhelper.Creator{}
	if !request.InspectOnly {
		if err := directory.Create(bounded, volumeRoot, input.Creation.VolumeDirectory); err != nil {
			return nil, err
		}
	}
	absent, err := directory.Absent(bounded, volumeRoot, input.Creation.VolumeDirectory)
	if err != nil {
		return nil, err
	}
	reference, err := environmentexecution.EncodeReference(input.Attempt.Input)
	if err != nil {
		return nil, err
	}
	response := &agentpb.EnvironmentProvisionHelperResponse{
		EnvironmentId:  input.Creation.EnvironmentId,
		Input:          reference,
		DirectoryReady: !absent,
		Inspection:     request.InspectOnly,
		ObservedAt:     timestamppb.New(time.Now().UTC()),
	}
	if err := environmentexecution.ValidateHelperResponse(input, request.InspectOnly, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteEnvironmentProvisionResponse(
	ctx context.Context,
	output io.Writer,
	response *agentpb.EnvironmentProvisionHelperResponse,
) error {
	if response == nil || executionplan.RejectUnknown(response) != nil {
		return errs.New(errs.KindValidationFailed, "environment provisioning helper response is invalid")
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	framed, err := frame(raw)
	if err != nil {
		return err
	}
	for len(framed) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := output.Write(framed)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if n <= 0 || n > len(framed) {
			return errs.Wrap(errs.KindInternal, io.ErrShortWrite)
		}
		framed = framed[n:]
	}
	return nil
}

func ReadEnvironmentProvisionResponse(
	ctx context.Context,
	input io.Reader,
) (*agentpb.EnvironmentProvisionHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > 4096 {
		return nil, errs.New(errs.KindValidationFailed, "environment provisioning helper response is oversized")
	}
	response := &agentpb.EnvironmentProvisionHelperResponse{}
	if proto.Unmarshal(raw, response) != nil || executionplan.RejectUnknown(response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "environment provisioning helper response is invalid")
	}
	return response, nil
}
