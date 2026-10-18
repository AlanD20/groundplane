package hostcommands

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const RemovalRecoveryHelperArgument = "service-removal-recovery-helper"

func RunRemovalRecoveryHelper(ctx context.Context, input io.Reader, output io.Writer, root string) error {
	request, err := composehelper.ReadRemovalRecoveryRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := executionplan.OpenRemovalRecoveryHelperRequest(request)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteRemovalRuntime(ctx, runner.New(nil), root, request)
	if err != nil {
		return err
	}
	return composehelper.WriteRemovalRecoveryResponse(ctx, output, captured, response)
}
