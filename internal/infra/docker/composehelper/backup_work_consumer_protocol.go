package composehelper

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func MarshalBackupConsumerRequest(ctx context.Context, request *agentpb.BackupConsumerHelperRequest) ([]byte, error) {
	if _, err := executionplan.OpenBackupConsumerHelper(ctx, request); err != nil {
		return nil, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(request)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	return frame(raw)
}

func ReadBackupConsumerRequest(ctx context.Context, input io.Reader) (*agentpb.BackupConsumerHelperRequest, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	request := &agentpb.BackupConsumerHelperRequest{}
	if proto.Unmarshal(raw, request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup Volume consumer helper request is malformed")
	}
	if _, err := executionplan.OpenBackupConsumerHelper(ctx, request); err != nil {
		return nil, err
	}
	return request, nil
}

func ReadBackupConsumerResponse(ctx context.Context, input io.Reader, captured executionplan.BackupConsumerHelperInput) (*agentpb.BackupConsumerHelperResponse, error) {
	raw, err := readFrame(ctx, input)
	if err != nil {
		return nil, err
	}
	defer clear(raw)
	if len(raw) > executionplan.MaximumPlanBytes {
		return nil, errs.New(errs.KindValidationFailed, "Backup Volume consumer helper response exceeds its observation bound")
	}
	response := &agentpb.BackupConsumerHelperResponse{}
	if proto.Unmarshal(raw, response) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Backup Volume consumer helper response is malformed")
	}
	if err := executionplan.ValidateBackupConsumerHelperResponse(captured, response); err != nil {
		return nil, err
	}
	return response, nil
}

func WriteBackupConsumerResponse(ctx context.Context, output io.Writer, captured executionplan.BackupConsumerHelperInput, response *agentpb.BackupConsumerHelperResponse) error {
	if err := executionplan.ValidateBackupConsumerHelperResponse(captured, response); err != nil {
		return err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(response)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	if len(raw) > executionplan.MaximumPlanBytes {
		return errs.New(errs.KindValidationFailed, "Backup Volume consumer helper response exceeds its observation bound")
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
