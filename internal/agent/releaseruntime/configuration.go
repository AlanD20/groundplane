package releaseruntime

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ConfigurationSource authenticates metadata and streams from the immutable
// configuration reference, never the Environment's current configuration head.
type ConfigurationSource interface {
	Describe(context.Context, releaseexecution.Input, releaseexecution.CheckpointReport) (*agentpb.WorkConfigurationReference, []entrymaterialization.Header, error)
	Open(context.Context, releaseexecution.Input, entrymaterialization.Header) (io.ReadCloser, error)
}

type ConfigurationEvidence interface {
	Resume(context.Context, releaseexecution.CheckpointReport) (workexecution.Input, bool, error)
	RecordConfiguration(context.Context, releaseexecution.CheckpointReport, []entrymaterialization.Header) (workexecution.Input, error)
}

type Configuration struct {
	runtime  *materialization.Runtime
	source   ConfigurationSource
	evidence ConfigurationEvidence
}

func NewConfiguration(runtime *materialization.Runtime, source ConfigurationSource, evidence ConfigurationEvidence) (*Configuration, error) {
	if runtime == nil || source == nil || evidence == nil {
		return nil, errs.New(errs.KindInternal, "Release configuration requires materializer and captured sources")
	}
	return &Configuration{runtime: runtime, source: source, evidence: evidence}, nil
}

func (configuration *Configuration) Apply(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport) (workexecution.Input, error) {
	return configuration.run(ctx, input, report, false)
}

func (configuration *Configuration) Inspect(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport) (workexecution.Input, error) {
	return configuration.run(ctx, input, report, true)
}

func (configuration *Configuration) run(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport, inspect bool) (workexecution.Input, error) {
	if configuration == nil || ctx == nil || report.Ack == nil || report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(input) != nil || !report.Progress.InFlight || report.Progress.Phase != releaseexecution.Configuration {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release configuration has no acknowledged boundary")
	}
	if reference, found, err := configuration.evidence.Resume(ctx, report); err != nil || found {
		return reference, err
	}
	bounded, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	defer cancel()
	stage, err := report.Progress.StageID(input)
	if err != nil || stage != report.StageID {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release configuration receipt names another boundary")
	}
	reference, files, err := configuration.source.Describe(bounded, input, report)
	if err != nil {
		return workexecution.Input{}, err
	}
	if !proto.Equal(reference, input.Release.Configuration) || len(files) != len(input.Release.ConfigurationDestinations) {
		return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release configuration source differs from the assigned file set")
	}
	for index, file := range files {
		if file.Destination() != input.Release.ConfigurationDestinations[index] || file.TaskID() != input.Attempt.TaskID || file.StepID() != report.StageID ||
			file.EnvironmentID() != input.Release.EnvironmentId || file.Generation() != reference.Generation {
			return workexecution.Input{}, errs.New(errs.KindStateConflict, "Release configuration file metadata differs from its captured selection")
		}
	}
	// Validate the entire selection before writing the first file. After all
	// writes, verify the complete set again; partial application is not success.
	if !inspect {
		for _, file := range files {
			if err := configuration.file(bounded, input, report, file, false); err != nil {
				return workexecution.Input{}, err
			}
		}
	}
	for _, file := range files {
		if err := configuration.file(bounded, input, report, file, true); err != nil {
			return workexecution.Input{}, err
		}
	}
	return configuration.evidence.RecordConfiguration(bounded, report, files)
}

func (configuration *Configuration) file(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport, header entrymaterialization.Header, verify bool) error {
	source, err := configuration.source.Open(ctx, input, header)
	if err != nil {
		if source != nil {
			return errors.Join(err, source.Close())
		}
		return err
	}
	return configuration.runtime.RunReleaseFile(ctx, input, report, header, source, verify)
}
