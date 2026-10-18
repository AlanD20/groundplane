package materialization

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (runtime *Runtime) RunEntryFile(ctx context.Context, input entryexecution.FileInput,
	files []entrymaterialization.Header, index int, source io.ReadCloser, verifyOnly bool,
) (err error) {
	if source == nil {
		return errs.New(errs.KindInternal, "Entry configuration source is missing")
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	if runtime == nil || runtime.helper == nil || ctx == nil || index < 0 || index >= len(files) ||
		entryexecution.ValidateConfigurationFiles(input, files) != nil ||
		!verifyOnly && input.Recovery == nil && input.Progress.Stage != entryexecution.WriteConfiguration {
		return errs.New(errs.KindStateConflict, "Entry file does not match its authorized configuration stage")
	}
	return runtime.runFile(ctx, input.Procedure.Candidate.AuthorizedVolumeDir,
		Payload{Header: files[index], Source: source}, verifyOnly)
}
