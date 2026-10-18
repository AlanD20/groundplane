package hostcommands

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const LifecycleWorkHelperArgument = "lifecycle-work-helper"

func RunLifecycleWorkHelper(ctx context.Context, input io.Reader, output io.Writer, volumeRoot string) error {
	request, err := composehelper.ReadLifecycleWorkRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := executionplan.OpenLifecycleHelperRequest(request)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteLifecycleMember(ctx, runner.New(nil), volumeRoot, request)
	if err != nil {
		return err
	}
	return composehelper.WriteLifecycleWorkResponse(ctx, output, captured, response)
}
