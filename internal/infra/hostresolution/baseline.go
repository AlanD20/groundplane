package hostresolution

import (
	"context"
	"errors"
	"io"
	"os"

	componentdns "github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	ResolverPath         = "/etc/resolv.conf"
	maximumBaselineBytes = 64 * 1024
)

func CaptureBaseline(ctx context.Context) ([]byte, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "host resolution: capture context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	file, err := os.Open(ResolverPath)
	if err != nil {
		return nil, errs.Wrap(errs.KindRequestFailed, err)
	}
	content, readErr := io.ReadAll(io.LimitReader(file, maximumBaselineBytes+1))
	closeErr := file.Close()
	if err := errors.Join(readErr, closeErr); err != nil {
		clear(content)
		return nil, errs.Wrap(errs.KindRequestFailed, err)
	}
	if len(content) > maximumBaselineBytes {
		clear(content)
		return nil, errs.New(errs.KindValidationFailed, "host resolution: resolver baseline is oversized")
	}
	if _, err := componentdns.ParseResolverBaseline(content); err != nil {
		clear(content)
		return nil, errs.Wrap(errs.KindValidationFailed, err)
	}
	return content, nil
}
