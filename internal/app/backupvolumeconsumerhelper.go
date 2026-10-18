package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const BackupConsumerHelperArgument = "backup-consumer-helper"

func RunBackupConsumerHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := composehelper.ReadBackupConsumerRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := executionplan.OpenBackupConsumerHelper(ctx, request)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteBackupConsumer(ctx, runner.New(nil), request)
	if err != nil {
		return err
	}
	return composehelper.WriteBackupConsumerResponse(ctx, output, captured, response)
}
