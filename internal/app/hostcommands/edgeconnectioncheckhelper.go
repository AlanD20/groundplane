package hostcommands

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/edgeconnectioncheck"
	edgecheck "github.com/AlanD20/groundplane/internal/infra/edgeconnectioncheck"
)

const EdgeConnectionCheckHelperArgument = edgeconnectioncheck.HelperArgument

func RunEdgeConnectionCheckHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := edgeconnectioncheck.ReadRequest(input)
	if err != nil {
		return err
	}
	result, err := edgecheck.CheckPrivate(ctx, request)
	if err != nil {
		return err
	}
	return edgeconnectioncheck.WritePrivateResult(output, result, request)
}
