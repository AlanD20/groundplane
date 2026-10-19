package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalEntryConfigurationWorkRequest(request *agentpb.EntryConfigurationWorkHelperRequest) ([]byte, error) {
	if _, err := entryexecution.OpenHelperRequest(request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadEntryConfigurationWorkRequest(
	ctx context.Context,
	input io.Reader,
) (*agentpb.EntryConfigurationWorkHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.EntryConfigurationWorkHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Entry helper request is malformed")
	}
	if _, err := entryexecution.OpenHelperRequest(request); err != nil {
		return nil, err
	}
	return request, nil
}

func ReadEntryConfigurationWorkResponse(
	ctx context.Context,
	input io.Reader,
	captured entryexecution.HelperInput,
) (*agentpb.EntryConfigurationWorkHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > entryexecution.MaximumProgressBytes {
		return nil, errs.New(errs.KindValidationFailed, "Entry helper response exceeds its observation bound")
	}
	response := &agentpb.EntryConfigurationWorkHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Entry helper response is malformed")
	}
	if err := validateEntryHelperResponse(captured, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteEntryConfigurationWorkResponse(
	ctx context.Context,
	output io.Writer,
	captured entryexecution.HelperInput,
	response *agentpb.EntryConfigurationWorkHelperResponse,
) error {
	if err := validateEntryHelperResponse(captured, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if len(raw) > entryexecution.MaximumProgressBytes {
		return errs.New(errs.KindValidationFailed, "Entry helper response exceeds its observation bound")
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

func validateEntryHelperResponse(
	captured entryexecution.HelperInput,
	response *agentpb.EntryConfigurationWorkHelperResponse,
) error {
	if captured.Recovery != nil {
		_, err := entryexecution.ReadHelperRestoration(captured, response)
		return err
	}
	_, err := entryexecution.ReadHelperObservation(captured, response)
	return err
}
