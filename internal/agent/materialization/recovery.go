package materialization

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (runtime *Runtime) RunRecoveryFile(ctx context.Context, input releaseexecution.RecoveryInput, report releaseexecution.RecoveryReport, header entrymaterialization.Header, source io.ReadCloser, verifyOnly bool) (err error) {
	if source == nil {
		return errs.New(errs.KindInternal, "Release recovery configuration source is missing")
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	if runtime == nil || runtime.helper == nil || ctx == nil || input.Input.Artifacts.Candidate == nil ||
		releaseexecution.ValidateRecoveryFile(input, report, header) != nil ||
		report.Progress.Phase == releaseexecution.VerifyRestoration && !verifyOnly {
		return errs.New(errs.KindStateConflict, "Release recovery file differs from its acknowledged captured selection")
	}
	return runtime.runFile(ctx, input.Input.Artifacts.Candidate.AuthorizedVolumeDir, Payload{Header: header, Source: source}, verifyOnly)
}
