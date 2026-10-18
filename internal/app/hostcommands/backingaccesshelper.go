package hostcommands

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	accessclient "github.com/AlanD20/groundplane/internal/infra/backingaccess"
)

const BackingAccessHelperArgument = backingaccess.HelperArgument

func RunBackingAccessHelper(ctx context.Context, input io.Reader, output io.Writer) error {
	request, err := backingaccess.ReadRequest(input)
	if err != nil {
		return err
	}
	defer request.Clear()
	result, err := accessclient.Check(ctx, request)
	if err != nil {
		return err
	}
	return backingaccess.WriteResult(output, result)
}
