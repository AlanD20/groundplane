package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const AttachConsumerHelperArgument = "attach-consumer-helper"

func RunAttachConsumerHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := composehelper.ReadAttachConsumerRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := attachexecution.OpenConsumerHelperRequest(ctx, request)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteAttachConsumer(ctx, runner.New(nil), request)
	if err != nil {
		return err
	}
	return composehelper.WriteAttachConsumerResponse(ctx, output, captured, response)
}
