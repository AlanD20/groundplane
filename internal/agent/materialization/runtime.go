package materialization

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"io"
)

type Helper interface {
	Run(context.Context, materializerrunner.Request) error
	StopCaptured(context.Context, materializerrunner.Ownership) error
}

type Runtime struct {
	helper         Helper
	componentFiles ComponentFileValidator
}

func New(
	helper Helper,
	componentFiles ComponentFileValidator,
) (*Runtime, error) {
	if helper == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: materialization helper is required")
	}
	return &Runtime{helper: helper, componentFiles: componentFiles}, nil
}

// RunConfigRestoreFile uses the ordinary descriptor-confined helper. The caller
// has proved the header against the sealed Config Restore file plan, not against
// current desired state or a generic command supplied by an operator.
func (runtime *Runtime) RunConfigRestoreFile(ctx context.Context, volumeDir string,
	header entrymaterialization.Header, source io.ReadCloser, verifyOnly bool,
) error {
	if runtime == nil || runtime.helper == nil || source == nil {
		return CloseSourceWithError(source, "agent: Config restore materializer is unavailable")
	}
	return runtime.runFile(ctx, volumeDir, Payload{Header: header, Source: source}, verifyOnly)
}

func (runtime *Runtime) runFile(ctx context.Context, volumeDir string, payload Payload, verifyOnly bool) error {
	reader, writer := io.Pipe()
	encoded := make(chan error, 1)
	go func() {
		encodeErr := entrymaterialization.Encode(
			ctx,
			writer,
			payload.Header,
			payload.Source,
			entrymaterialization.Limits{
				MaxContentBytes:     entrymaterialization.MaximumContentBytes,
				MaxDestinationBytes: entrymaterialization.MaximumDestinationBytes,
			},
		)
		closeErr := writer.CloseWithError(encodeErr)
		if encodeErr == nil {
			encodeErr = closeErr
		}
		encoded <- encodeErr
	}()
	helperErr := runtime.helper.Run(ctx, materializerrunner.Request{
		VolumeDir: volumeDir, Stream: reader, VerifyOnly: verifyOnly, Header: payload.Header,
	})
	readerCloseErr := reader.Close()
	encodeErr := <-encoded
	if err := errors.Join(helperErr, readerCloseErr, encodeErr); err != nil {
		if ctx != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		if verifyOnly && readerCloseErr == nil && encodeErr == nil &&
			errors.Is(helperErr, errs.New(errs.KindStateConflict, "")) {
			return helperErr
		}
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}
