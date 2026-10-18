package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const EnvironmentRemovalHelperArgument = "environment-removal-helper"

func RunEnvironmentRemovalHelper(ctx context.Context, input io.Reader, output io.Writer, volumeRoot string) error {
	request, err := composehelper.ReadEnvironmentRemovalRequest(ctx, input)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteEnvironmentRemoval(ctx, runner.New(nil), volumeRoot, request)
	if err != nil {
		return err
	}
	return composehelper.WriteEnvironmentRemovalResponse(ctx, output, response)
}
