package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/infra/docker/managedconfighelper"
)

const ManagedConfigHelperArgument = "managed-config-helper"

func RunManagedConfigHelper(ctx context.Context, input io.Reader) error {
	request, err := managedconfighelper.ReadRequest(ctx, input)
	if err != nil {
		return err
	}
	defer clear(request.Content)
	return managedconfighelper.Apply(ctx, request)
}
