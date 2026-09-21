package app

import (
	"context"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
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
	catalog, err := componentregistration.NewCatalog()
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteWithComponentCatalog(
		ctx, runner.New(nil), request, composeHelperComponentCatalog{catalog: catalog},
	)
	if err != nil {
		return err
	}
	return composehelper.WriteResponse(ctx, output, response)
}
