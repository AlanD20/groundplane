package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalAttachConsumerRequest(ctx context.Context, request *agentpb.AttachConsumerHelperRequest) ([]byte, error) {
	if _, err := attachexecution.OpenConsumerHelperRequest(ctx, request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadAttachConsumerRequest(ctx context.Context, input io.Reader) (*agentpb.AttachConsumerHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.AttachConsumerHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Detach consumer helper request is malformed")
	}
	if _, err := attachexecution.OpenConsumerHelperRequest(ctx, request); err != nil {
		return nil, err
	}
	return request, nil
}

func ReadAttachConsumerResponse(ctx context.Context, input io.Reader, captured attachexecution.ConsumerHelperInput) (*agentpb.AttachConsumerHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > attachexecution.MaximumRemovalReportBytes {
		return nil, errs.New(errs.KindValidationFailed, "Detach consumer helper response exceeds its observation bound")
	}
	response := &agentpb.AttachConsumerHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Detach consumer helper response is malformed")
	}
	if err := attachexecution.ValidateConsumerHelperResponse(captured, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteAttachConsumerResponse(ctx context.Context, output io.Writer, captured attachexecution.ConsumerHelperInput, response *agentpb.AttachConsumerHelperResponse) error {
	if err := attachexecution.ValidateConsumerHelperResponse(captured, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if len(raw) > attachexecution.MaximumRemovalReportBytes {
		return errs.New(errs.KindValidationFailed, "Detach consumer helper response exceeds its observation bound")
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
