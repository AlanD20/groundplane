package app

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/infra/entrymaterializer"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	EntryMaterializerArgument = "materialize"
)

// RunEntryMaterializer is the thin application boundary for the short-lived
// helper mode. The Docker runner fixes its mount and capabilities; this
// boundary fixes the protocol limits before any frame reaches the filesystem.
func RunEntryMaterializer(ctx context.Context, source io.ReadCloser) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "entry materializer context is required")
	}
	if source == nil {
		return errs.New(errs.KindInternal, "entry materializer source is required")
	}
	return entrymaterializer.Run(ctx, source, entrymaterialization.Limits{
		MaxContentBytes:     entrymaterialization.MaximumContentBytes,
		MaxDestinationBytes: entrymaterialization.MaximumDestinationBytes,
	})
}
