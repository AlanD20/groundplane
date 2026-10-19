package materialization

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/infra/docker/materializerrunner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (runtime *Runtime) RunBackingFile(ctx context.Context, input backingprovision.HelperInput,
	file *agentpb.MaterializeFile, source io.ReadCloser, verifyOnly bool,
) (err error) {
	if source == nil {
		return errs.New(errs.KindInternal, "backing creation configuration source is missing")
	}
	defer func() { err = errors.Join(err, source.Close()) }()
	if runtime == nil || runtime.helper == nil || ctx == nil {
		return errs.New(errs.KindInternal, "backing creation materializer is unavailable")
	}
	header, err := backingprovision.ConfigurationHeader(input, file)
	if err != nil {
		return err
	}
	return runtime.runFile(
		ctx,
		input.Assignment.Procedure.VolumeDirectory,
		Payload{Header: header, Source: source},
		verifyOnly,
	)
}

func (runtime *Runtime) StopBackingFiles(ctx context.Context, input backingprovision.HelperInput) error {
	if runtime == nil || runtime.helper == nil || ctx == nil {
		return errs.New(errs.KindInternal, "backing creation file helper cleanup is unavailable")
	}
	for _, file := range input.Assignment.Procedure.Files {
		header, err := backingprovision.ConfigurationHeader(input, file)
		if err != nil {
			return err
		}
		if err := runtime.helper.StopCaptured(ctx, materializerrunner.OwnershipFromHeader(header)); err != nil {
			return err
		}
	}
	return nil
}

func (runtime *Runtime) StopBackingRecoveryFiles(ctx context.Context, input backingprovision.HelperInput) error {
	if runtime == nil || runtime.helper == nil || ctx == nil || input.Assignment.Recovery == nil ||
		input.Assignment.Recovery.ValidateFor(input.Assignment.Attempt, input.Layout) != nil {
		return errs.New(errs.KindStateConflict, "backing creation file cleanup has no captured failed execution")
	}
	forward := input.Assignment.Recovery.Forward
	stage, err := backingprovision.StageID(forward.WorkItemID, backingprovision.WriteConfiguration)
	if err != nil {
		return err
	}
	for _, file := range input.Assignment.Procedure.Files {
		if err := runtime.helper.StopCaptured(ctx, materializerrunner.Ownership{TaskID: forward.TaskID, StageID: stage,
			EnvironmentID: file.EnvironmentId, Destination: file.Destination, Generation: input.Assignment.Procedure.RenderGeneration}); err != nil {
			return err
		}
	}
	return nil
}
