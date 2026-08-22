package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
)

const (
	EnvironmentDirectoryHelperArgument = "environment-directory-helper"
	EnvironmentVolumeRootEnv           = environmentdirectoryhelper.VolumeRootEnv
)

func RunEnvironmentDirectoryHelper(
	ctx context.Context,
	input io.Reader,
	output io.Writer,
	volumeRoot string,
) error {
	request, err := environmentdirectoryhelper.ReadRequest(ctx, input)
	if err != nil {
		return err
	}
	response, err := environmentdirectoryhelper.Execute(
		ctx,
		volumeRoot,
		environmentdirectoryhelper.Creator{},
		request,
	)
	if err != nil {
		return err
	}
	return environmentdirectoryhelper.WriteResponse(ctx, output, response)
}
