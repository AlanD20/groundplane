package materialization

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (runtime *Runtime) RunComponentFile(ctx context.Context, input componentexecution.HelperInput, files []entrymaterialization.Header, index int, source io.ReadCloser, verifyOnly bool) (err error) {
	if source == nil {
		return errs.New(errs.KindInternal, "Component configuration source is missing")
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	if runtime == nil || runtime.helper == nil || ctx == nil || index < 0 || index >= len(files) ||
		componentexecution.ValidateConfigurationFiles(input, files) != nil || input.Procedure.Candidate == nil {
		return errs.New(errs.KindStateConflict, "Component file does not match its captured configuration boundary")
	}
	return runtime.runFile(ctx, input.Procedure.Candidate.AuthorizedVolumeDir, Payload{Header: files[index], Source: source}, verifyOnly)
}
