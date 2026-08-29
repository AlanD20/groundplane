package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/docker/hostresolutionhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const HostResolutionHelperArgument = "host-resolution-helper"

func RunHostResolutionHelper(ctx context.Context, operation string) error {
	switch operation {
	case "apply":
		return hostresolutionhelper.Apply(ctx)
	case "restore":
		return hostresolutionhelper.Restore(ctx)
	default:
		return errs.New(errs.KindValidationFailed, "host resolution helper operation is invalid")
	}
}
