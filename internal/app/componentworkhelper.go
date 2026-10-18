package app

import (
	"context"
	"github.com/AlanD20/groundplane/internal/app/componentregistration"
	"io"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const ComponentWorkHelperArgument = "component-work-helper"

func RunComponentWorkHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := composehelper.ReadComponentWorkRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := componentexecution.OpenHelperRequest(request)
	if err != nil {
		return err
	}
	catalog, err := componentregistration.NewCatalog()
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteComponentWork(ctx, runner.New(nil), request, composeHelperComponentCatalog{catalog: catalog})
	if err != nil {
		return err
	}
	return composehelper.WriteComponentWorkResponse(ctx, output, captured, response)
}
