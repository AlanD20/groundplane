package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
)

const ManagedConfigHelperArgument = "managed-config-helper"

func RunManagedConfigHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := managedconfighelper.ReadRequest(ctx, input)
	if err != nil {
		return err
	}
	defer clear(request.Content)
	response, err := managedconfighelper.Apply(ctx, request)
	if err != nil {
		return err
	}
	return managedconfighelper.WriteResponse(output, response)
}
