package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalRemovalRecoveryRequest(request *agentpb.ServiceRemovalRecoveryHelperRequest) ([]byte, error) {
	if _, err := executionplan.OpenRemovalRecoveryHelperRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadRemovalRecoveryRequest(
	ctx context.Context,
	input io.Reader,
) (*agentpb.ServiceRemovalRecoveryHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.ServiceRemovalRecoveryHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Service removal recovery helper request is malformed")
	}
	if _, err := executionplan.OpenRemovalRecoveryHelperRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func ReadRemovalRecoveryResponse(
	ctx context.Context,
	input io.Reader,
	captured executionplan.RemovalRecoveryHelperInput,
) (*agentpb.ServiceRemovalRecoveryHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > removalexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Service removal recovery response exceeds its bound")
	}
	response := &agentpb.ServiceRemovalRecoveryHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Service removal recovery response is malformed")
	}
	if _, err := executionplan.ReadRemovalRecoveryHelperReceipt(captured, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteRemovalRecoveryResponse(
	ctx context.Context,
	output io.Writer,
	captured executionplan.RemovalRecoveryHelperInput,
	response *agentpb.ServiceRemovalRecoveryHelperResponse,
) error {
	if _, err := executionplan.ReadRemovalRecoveryHelperReceipt(captured, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if len(raw) > removalexecution.MaximumProgressBytes {
		return errs.New(errs.KindValidationFailed, "Service removal recovery response exceeds its bound")
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
