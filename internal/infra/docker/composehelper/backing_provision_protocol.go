package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalBackingProvisionRequest(request *agentpb.BackingProvisionHelperRequest) ([]byte, error) {
	if _, err := backingprovision.OpenHelperRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	return frame(raw)
}

func ReadBackingProvisionRequest(ctx context.Context, input io.Reader) (*agentpb.BackingProvisionHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.BackingProvisionHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "backing creation helper request is malformed")
	}
	if _, err := backingprovision.OpenHelperRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func ReadBackingProvisionResponse(ctx context.Context, input io.Reader, captured backingprovision.HelperInput) (*agentpb.BackingProvisionHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > 4*backingprovision.MaximumProgressBytes+32*1024 {
		return nil, errs.New(errs.KindValidationFailed, "backing creation helper response exceeds its observation bound")
	}
	response := &agentpb.BackingProvisionHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "backing creation helper response is malformed")
	}
	if err := validateBackingHelperResponse(captured, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteBackingProvisionResponse(ctx context.Context, output io.Writer, captured backingprovision.HelperInput,
	response *agentpb.BackingProvisionHelperResponse,
) error {
	if err := validateBackingHelperResponse(captured, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	framed, err := frame(raw)
	if err != nil {
		return err
	}
	defer clear(framed)
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

func validateBackingHelperResponse(captured backingprovision.HelperInput, response *agentpb.BackingProvisionHelperResponse) error {
	if captured.Assignment.Recovery != nil {
		_, err := backingprovision.ReadHelperRecovery(captured, response)
		return err
	}
	_, _, err := backingprovision.ReadHelperObservation(captured, response)
	return err
}
