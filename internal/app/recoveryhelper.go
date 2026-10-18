package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const RecoveryHelperArgument = "release-recovery-helper"

func RunRecoveryHelper(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
	request, err := composehelper.ReadRecoveryRequest(ctx, stdin)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteRecovery(ctx, runner.New(nil), request)
	if err != nil {
		return err
	}
	input, report, err := composehelper.OpenRecoveryRequest(request)
	if err != nil {
		return err
	}
	return composehelper.WriteRecoveryResponse(ctx, stdout, input, report, response)
}
