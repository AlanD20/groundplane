package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
)

func (s *store) Snapshot(ctx context.Context, w io.Writer) error {
	if w == nil {
		return errs.New(errs.KindValidationFailed, "snapshot writer is required")
	}

	reader, err := s.client.Snapshot(ctx)
	if err != nil {
		return wrap(ctx, err)
	}
	_, copyErr := io.Copy(w, reader)
	closeErr := reader.Close()
	if copyErr != nil {
		return wrap(ctx, copyErr)
	}
	return wrap(ctx, closeErr)
}
