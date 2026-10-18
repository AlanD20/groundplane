package hostcommands

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/runnerconnectioncheck"
	checkclient "github.com/AlanD20/groundplane/internal/infra/runnerconnectioncheck"
)

const RunnerConnectionCheckHelperArgument = runnerconnectioncheck.HelperArgument

func RunRunnerConnectionCheckHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := runnerconnectioncheck.ReadRequest(input)
	if err != nil {
		return err
	}
	result, err := checkclient.Check(ctx, request)
	if err != nil {
		return err
	}
	return runnerconnectioncheck.WriteResult(output, result, request)
}
