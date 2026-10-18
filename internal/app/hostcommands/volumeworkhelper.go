package hostcommands

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const VolumeWorkHelperArgument = "volume-work-helper"

func RunVolumeWorkHelper(ctx context.Context, input io.Reader, output io.Writer, volumeRoot string) error {
	request, err := composehelper.ReadVolumeWorkRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := executionplan.OpenVolumeHelperRequest(request)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteVolumeWork(ctx, runner.New(nil), volumeRoot, request)
	if err != nil {
		return err
	}
	return composehelper.WriteVolumeWorkResponse(ctx, output, captured, response)
}
