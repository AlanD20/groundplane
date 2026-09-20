package agent

import (
	"context"
	"errors"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"io"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type MaterializationHelper interface {
	Run(context.Context, materializerrunner.Request) error
}

type MaterializationRuntime struct {
	helper         MaterializationHelper
	componentFiles ComponentFileValidator
}

func NewMaterializationRuntime(
	helper MaterializationHelper,
	componentFiles ComponentFileValidator,
) (*MaterializationRuntime, error) {
	if helper == nil {
		return nil, errs.New(errs.KindValidationFailed, "agent: materialization helper is required")
	}
	return &MaterializationRuntime{helper: helper, componentFiles: componentFiles}, nil
}

func (runtime *MaterializationRuntime) executeStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	payload materializationPayload,
) error {
	return runtime.runStep(ctx, assignment, step, payload, false)
}

func (runtime *MaterializationRuntime) verifyStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	payload materializationPayload,
) error {
	return runtime.runStep(ctx, assignment, step, payload, true)
}

func (runtime *MaterializationRuntime) runStep(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
	payload materializationPayload,
	verifyOnly bool,
) error {
	if runtime == nil || runtime.helper == nil || payload.Source == nil {
		return closeMaterializationSource(payload.Source, "agent: materialization runtime is not configured")
	}
	materialization := step.GetMaterializeFile()
	artifact := composeArtifact(assignment.Plan, materialization.GetArtifactId())
	if materialization == nil || artifact == nil || payload.Header.TaskID() != assignment.TaskID ||
		payload.Header.StepID() != step.GetStepId() {
		return closeMaterializationSource(payload.Source, "agent: materialization runtime input is invalid")
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

func closeMaterializationSource(source io.ReadCloser, message string) error {
	if source == nil {
		return errs.New(errs.KindInternal, message)
	}
	if err := source.Close(); err != nil {
		return errs.Wrap(errs.KindInternal, errors.Join(errs.New(errs.KindInternal, message), err))
	}
	return errs.New(errs.KindInternal, message)
}
