package materialization

import (
	"context"
	"errors"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"io"
)

type Helper interface {
	Run(context.Context, materializerrunner.Request) error
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

func (runtime *Runtime) ExecuteStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	payload Payload,
) error {
	return runtime.runStep(ctx, assignment, step, payload, false)
}

func (runtime *Runtime) VerifyStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	payload Payload,
) error {
	return runtime.runStep(ctx, assignment, step, payload, true)
}

func (runtime *Runtime) runStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	payload Payload,
	verifyOnly bool,
) error {
	if runtime == nil || runtime.helper == nil || payload.Source == nil {
		return CloseSourceWithError(payload.Source, "agent: materialization runtime is not configured")
	}
	materialization := step.GetMaterializeFile()
	artifact := taskassignment.ComposeArtifact(assignment.Plan, materialization.GetArtifactId())
	if materialization == nil || artifact == nil || payload.Header.TaskID() != assignment.TaskID ||
		payload.Header.StepID() != step.GetStepId() {
		return CloseSourceWithError(payload.Source, "agent: materialization runtime input is invalid")
	}
	var err error
	if step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE &&
		step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
		payload, err = runtime.preflightComponentFile(ctx, assignment, step, payload)
		if err != nil {
			return err
		}
	}
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
		VolumeDir: artifact.GetAuthorizedVolumeDir(), Stream: reader, VerifyOnly: verifyOnly,
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
