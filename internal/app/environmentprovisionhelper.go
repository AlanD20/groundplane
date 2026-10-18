package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const EnvironmentProvisionHelperArgument = "environment-provision-helper"

func RunEnvironmentProvisionHelper(ctx context.Context, input io.Reader, output io.Writer, volumeRoot string) error {
	request, err := composehelper.ReadEnvironmentProvisionRequest(ctx, input)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteEnvironmentProvision(ctx, volumeRoot, request)
	if err != nil {
		return err
	}
	return composehelper.WriteEnvironmentProvisionResponse(ctx, output, response)
}
