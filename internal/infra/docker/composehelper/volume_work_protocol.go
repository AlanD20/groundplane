package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalVolumeWorkRequest(request *agentpb.VolumeWorkHelperRequest) ([]byte, error) {
	if _, err := executionplan.OpenVolumeHelperRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadVolumeWorkRequest(ctx context.Context, input io.Reader) (*agentpb.VolumeWorkHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.VolumeWorkHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Volume helper request is malformed")
	}
	if _, err := executionplan.OpenVolumeHelperRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func ReadVolumeWorkResponse(
	ctx context.Context,
	input io.Reader,
	captured executionplan.VolumeHelperInput,
) (*agentpb.VolumeWorkHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > volumeexecution.MaximumDirectoryBatchBytes+volumeexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Volume helper response exceeds its observation bound")
	}
	response := &agentpb.VolumeWorkHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Volume helper response is malformed")
	}
	if err := executionplan.ValidateVolumeHelperResponse(captured, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteVolumeWorkResponse(
	ctx context.Context,
	output io.Writer,
	captured executionplan.VolumeHelperInput,
	response *agentpb.VolumeWorkHelperResponse,
) error {
	if err := executionplan.ValidateVolumeHelperResponse(captured, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if len(raw) > volumeexecution.MaximumDirectoryBatchBytes+volumeexecution.MaximumProgressBytes {
		return errs.New(errs.KindValidationFailed, "Volume helper response exceeds its observation bound")
	}
	framed, err := frame(raw)
	if err != nil {
		return err
	}
	for len(framed) != 0 {
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
