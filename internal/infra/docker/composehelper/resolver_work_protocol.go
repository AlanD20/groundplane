package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalResolverRequest(request *agentpb.ResolverWorkHelperRequest) ([]byte, error) {
	if _, err := resolverexecution.OpenHelperRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	return frame(raw)
}

func ReadResolverRequest(ctx context.Context, input io.Reader) (*agentpb.ResolverWorkHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.ResolverWorkHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "DNS resolver helper request is malformed")
	}
	if _, err := resolverexecution.OpenHelperRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func validateResolverResponse(input resolverexecution.HelperInput, response *agentpb.ResolverWorkHelperResponse) error {
	if input.Assignment.Recovery != nil {
		_, err := resolverexecution.ReadHelperRestoration(input, response)
		return err
	}
	_, _, err := resolverexecution.ReadHelperObservation(input, response)
	return err
}

func ReadResolverResponse(
	ctx context.Context,
	source io.Reader,
	input resolverexecution.HelperInput,
) (*agentpb.ResolverWorkHelperResponse, error) {
	raw, err := readFrame(ctx, source)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > 2*resolverexecution.MaximumReportBytes+4096 {
		return nil, errs.New(errs.KindValidationFailed, "DNS resolver helper response exceeds its bound")
	}
	response := &agentpb.ResolverWorkHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "DNS resolver helper response is malformed")
	}
	if err := validateResolverResponse(input, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteResolverResponse(
	ctx context.Context,
	output io.Writer,
	input resolverexecution.HelperInput,
	response *agentpb.ResolverWorkHelperResponse,
) error {
	if err := validateResolverResponse(input, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer clear(raw)
	if len(raw) > 2*resolverexecution.MaximumReportBytes+4096 {
		return errs.New(errs.KindValidationFailed, "DNS resolver helper response exceeds its bound")
	}
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
