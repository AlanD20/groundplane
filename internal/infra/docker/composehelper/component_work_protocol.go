package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalComponentWorkRequest(request *agentpb.ComponentWorkHelperRequest) ([]byte, error) {
	if _, err := componentexecution.OpenHelperRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadComponentWorkRequest(ctx context.Context, input io.Reader) (*agentpb.ComponentWorkHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.ComponentWorkHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Component helper request is malformed")
	}
	if _, err := componentexecution.OpenHelperRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func ReadComponentWorkResponse(
	ctx context.Context,
	input io.Reader,
	captured componentexecution.HelperInput,
) (*agentpb.ComponentWorkHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > componentexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Component helper response exceeds its observation bound")
	}
	response := &agentpb.ComponentWorkHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Component helper response is malformed")
	}
	if err := validateComponentHelperResponse(captured, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteComponentWorkResponse(
	ctx context.Context,
	output io.Writer,
	captured componentexecution.HelperInput,
	response *agentpb.ComponentWorkHelperResponse,
) error {
	if err := validateComponentHelperResponse(captured, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if len(raw) > componentexecution.MaximumProgressBytes {
		return errs.New(errs.KindValidationFailed, "Component helper response exceeds its observation bound")
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

func validateComponentHelperResponse(
	captured componentexecution.HelperInput,
	response *agentpb.ComponentWorkHelperResponse,
) error {
	if captured.Recovery != nil {
		_, err := componentexecution.ReadHelperRecovery(captured, response)
		return err
	}
	_, err := componentexecution.ReadHelperObservation(captured, response)
	return err
}
