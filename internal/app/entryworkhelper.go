package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
)

const EntryWorkHelperArgument = "entry-work-helper"

func RunEntryWorkHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := composehelper.ReadEntryConfigurationWorkRequest(ctx, input)
	if err != nil {
		return err
	}
	captured, err := entryexecution.OpenHelperRequest(request)
	if err != nil {
		return err
	}
	response, err := composehelper.ExecuteEntryConfigurationWork(ctx, runner.New(nil), request)
	if err != nil {
		return err
	}
	return composehelper.WriteEntryConfigurationWorkResponse(ctx, output, captured, response)
}
