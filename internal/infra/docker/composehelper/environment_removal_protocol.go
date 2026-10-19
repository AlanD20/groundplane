package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalEnvironmentRemovalRequest(request *agentpb.EnvironmentRemovalHelperRequest) ([]byte, error) {
	if _, _, err := hierarchyexecution.OpenEnvironmentHelperRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadEnvironmentRemovalRequest(
	ctx context.Context,
	input io.Reader,
) (*agentpb.EnvironmentRemovalHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.EnvironmentRemovalHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal helper request is invalid")
	}
	if _, _, err := hierarchyexecution.OpenEnvironmentHelperRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func WriteEnvironmentRemovalResponse(
	ctx context.Context,
	output io.Writer,
	response *agentpb.EnvironmentRemovalHelperResponse,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if response == nil || executionplan.RejectUnknown(response) != nil {
		return errs.New(errs.KindValidationFailed, "Environment removal helper response is invalid")
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
		written, err := output.Write(framed)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if written <= 0 || written > len(framed) {
			return errs.Wrap(errs.KindInternal, io.ErrShortWrite)
		}
		framed = framed[written:]
	}
	return nil
}

func ReadEnvironmentRemovalResponse(
	ctx context.Context,
	input io.Reader,
) (*agentpb.EnvironmentRemovalHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > hierarchyexecution.MaximumEnvironmentReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal helper response is oversized")
	}
	response := &agentpb.EnvironmentRemovalHelperResponse{}
	if proto.Unmarshal(raw, response) != nil || executionplan.RejectUnknown(response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Environment removal helper response is invalid")
	}
	return response, nil
}
