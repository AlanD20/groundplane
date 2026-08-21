package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const ComposeHelperArgument = "compose-helper"

// RunComposeHelper executes the static helper protocol used only inside a
// Controller-created, task-scoped helper container.
func RunComposeHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := composehelper.ReadRequest(ctx, input)
	if err != nil {
		return err
	}
	response, err := composehelper.Execute(ctx, runner.New(nil), request)
	if err != nil {
		return err
	}
	return composehelper.WriteResponse(ctx, output, response)
}
