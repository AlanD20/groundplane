package releaseruntime

import (
	"context"
	"errors"
	"io"

	"github.com/AlanD20/groundplane/internal/agent/materialization"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentownership"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// RecoveryConfigurationSource authenticates the captured prior snapshot and
// supplies explicit removal descriptors for destinations absent in that snapshot.
type RecoveryConfigurationSource interface {
	DescribeRecovery(context.Context, releaseexecution.RecoveryInput, releaseexecution.RecoveryReport) (*agentpb.WorkConfigurationReference, []entrymaterialization.Header, error)
	OpenRecoveryFile(context.Context, releaseexecution.RecoveryInput, entrymaterialization.Header) (io.ReadCloser, error)
}

type RecoveryConfiguration struct {
	runtime   *materialization.Runtime
	source    RecoveryConfigurationSource
	ownership *agentownership.Lease
}

func NewRecoveryConfiguration(runtime *materialization.Runtime, source RecoveryConfigurationSource, ownership *agentownership.Lease) (*RecoveryConfiguration, error) {
	if runtime == nil || source == nil {
		return nil, errs.New(errs.KindInternal, "Release recovery configuration requires materializer and captured sources")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	return &RecoveryConfiguration{runtime: runtime, source: source, ownership: ownership}, nil
}

func (configuration *RecoveryConfiguration) Run(ctx context.Context, input releaseexecution.RecoveryInput, report releaseexecution.RecoveryReport, inspectOnly bool) (releaseexecution.RecoveryConfigurationProof, error) {
	if configuration == nil || ctx == nil || report.Validate(input) != nil || report.Ack == nil || !report.Progress.InFlight ||
		(report.Progress.Phase != releaseexecution.RestoreConfiguration && report.Progress.Phase != releaseexecution.VerifyRestoration) ||
		report.Progress.Phase == releaseexecution.VerifyRestoration && !inspectOnly {
		return releaseexecution.RecoveryConfigurationProof{}, errs.New(errs.KindStateConflict, "Release recovery configuration has no acknowledged file boundary")
	}
	bounded, cancel := context.WithDeadline(ctx, input.Input.Attempt.Deadline)
	defer cancel()
	reference, files, err := configuration.source.DescribeRecovery(bounded, input, report)
	if err != nil {
		return releaseexecution.RecoveryConfigurationProof{}, err
	}
	if !proto.Equal(reference, input.Input.Release.PriorConfiguration) {
		return releaseexecution.RecoveryConfigurationProof{}, errs.New(errs.KindStateConflict, "Release recovery source differs from the captured prior configuration")
	}
	// Validate the complete selection before changing the first file.
	proof, err := releaseexecution.RecoveryConfigurationResult(input, report, files)
	if err != nil {
		return releaseexecution.RecoveryConfigurationProof{}, err
	}
	if err := configuration.ownership.CheckRestoration(input); err != nil {
		return releaseexecution.RecoveryConfigurationProof{}, err
	}
	if err := configuration.runtime.StopRecoveryFiles(bounded, input, report, files); err != nil {
		return releaseexecution.RecoveryConfigurationProof{}, err
	}
	if !inspectOnly {
		for _, file := range files {
			if err := configuration.file(bounded, input, report, file, false); err != nil {
				return releaseexecution.RecoveryConfigurationProof{}, err
			}
		}
	}
	for _, file := range files {
		if err := configuration.file(bounded, input, report, file, true); err != nil {
			return releaseexecution.RecoveryConfigurationProof{}, err
		}
	}
	if err := configuration.ownership.CheckRestoration(input); err != nil {
		return releaseexecution.RecoveryConfigurationProof{}, err
	}
	return proof, nil
}

func (configuration *RecoveryConfiguration) file(ctx context.Context, input releaseexecution.RecoveryInput, report releaseexecution.RecoveryReport, header entrymaterialization.Header, verifyOnly bool) error {
	source, err := configuration.source.OpenRecoveryFile(ctx, input, header)
	if err != nil {
		if source != nil {
			return errors.Join(err, source.Close())
		}
		return err
	}
	return configuration.runtime.RunRecoveryFile(ctx, input, report, header, source, verifyOnly)
}
