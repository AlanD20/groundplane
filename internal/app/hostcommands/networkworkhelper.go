package hostcommands

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const NetworkWorkHelperArgument = "network-work-helper"

func RunNetworkWorkHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := composehelper.ReadNetworkWorkRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := executionplan.OpenNetworkHelperRequest(request)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteNetworkWork(ctx, runner.New(nil), request)
	if err != nil {
		return err
	}
	return composehelper.WriteNetworkWorkResponse(ctx, output, captured, response)
}
