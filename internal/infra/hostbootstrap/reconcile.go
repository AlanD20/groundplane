// Package hostbootstrap reconciles the fixed host services required before
// Controller capabilities can be constructed.
package hostbootstrap

import (
	"context"
	"net/netip"

	"github.com/AlanD20/groundplane/internal/infra/docker/registryhost"
	"github.com/AlanD20/groundplane/internal/infra/hostresolution"
)

func Reconcile(ctx context.Context, registryAddress netip.Addr) error {
	if err := registryhost.Reconcile(ctx, registryAddress); err != nil {
		return err
	}
	return hostresolution.EnsureDHCPHook(ctx)
}
