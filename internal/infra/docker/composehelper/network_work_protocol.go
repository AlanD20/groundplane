package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalNetworkWorkRequest(request *agentpb.NetworkWorkHelperRequest) ([]byte, error) {
	if _, err := executionplan.OpenNetworkHelperRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadNetworkWorkRequest(ctx context.Context, input io.Reader) (*agentpb.NetworkWorkHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.NetworkWorkHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Network helper request is malformed")
	}
	if _, err := executionplan.OpenNetworkHelperRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func ReadNetworkWorkResponse(ctx context.Context, input io.Reader, captured executionplan.NetworkHelperInput) (*agentpb.NetworkWorkHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > networkexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Network helper response exceeds its observation bound")
	}
	response := &agentpb.NetworkWorkHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Network helper response is malformed")
	}
	if _, err := executionplan.ReadNetworkHelperObservation(captured, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteNetworkWorkResponse(ctx context.Context, output io.Writer, captured executionplan.NetworkHelperInput, response *agentpb.NetworkWorkHelperResponse) error {
	if _, err := executionplan.ReadNetworkHelperObservation(captured, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if len(raw) > networkexecution.MaximumProgressBytes {
		return errs.New(errs.KindValidationFailed, "Network helper response exceeds its observation bound")
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
