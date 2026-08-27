package agent

import (
	"context"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/components/coredns"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CaptureHostResolverBaseline reads the host resolver file once, validates its
// bounded nameserver snapshot, and returns pure data for Controller rendering.
// The Agent must capture this before applying an upstream_auto CoreDNS task.
func CaptureHostResolverBaseline(ctx context.Context, path string, generation uint64) (coredns.ResolverBaseline, error) {
	if ctx == nil {
		return coredns.ResolverBaseline{}, errs.New(errs.KindInternal, "coredns: baseline context is required")
	}
	if err := ctx.Err(); err != nil {
		return coredns.ResolverBaseline{}, err
	}
	if generation == 0 || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return coredns.ResolverBaseline{}, errs.New(errs.KindValidationFailed, "coredns: resolver baseline path or generation is invalid")
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return coredns.ResolverBaseline{}, errs.Wrap(errs.KindRequestFailed, err)
	}
	resolvers, err := coredns.ParseResolverBaseline(content)
	if err != nil {
		return coredns.ResolverBaseline{}, err
	}
	return coredns.ResolverBaseline{Generation: generation, Resolvers: resolvers}, nil
}
