package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const BackingProvisionHelperArgument = "backing-provision-helper"

func RunBackingProvisionHelper(ctx context.Context, input io.Reader, output io.Writer, volumeRoot string) error {
	request, err := composehelper.ReadBackingProvisionRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := backingprovision.OpenHelperRequest(request)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteBackingProvision(ctx, runner.New(nil), volumeRoot, request)
	if err != nil {
		return err
	}
	return composehelper.WriteBackingProvisionResponse(ctx, output, captured, response)
}
