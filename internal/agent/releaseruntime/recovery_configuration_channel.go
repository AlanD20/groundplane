package releaseruntime

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"io"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Recovery uses the same bounded authenticated file transport as forward work,
// with the snapshot selected by the captured recovery disposition.
func (source *ChannelConfigurationSource) DescribeRecovery(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	report releaseexecution.RecoveryReport,
) (*agentpb.WorkConfigurationReference, []entrymaterialization.Header, error) {
	if source == nil || input.Validate() != nil || report.Validate(input) != nil || report.Ack == nil ||
		!report.Progress.InFlight ||
		(report.Progress.Phase != releaseexecution.RestoreConfiguration && report.Progress.Phase != releaseexecution.VerifyRestoration) {
		return nil, nil, invalidConfigurationDelivery()
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return nil, nil, err
	}
	manifest, err := source.exchange.Configuration(
		ctx,
		&agentpb.ReleaseConfigurationRequest{Attempt: identity, StageId: report.StageID},
	)
	if err != nil {
		return nil, nil, err
	}
	if manifest == nil || executionplan.RejectUnknown(manifest) != nil || !proto.Equal(manifest.Attempt, identity) ||
		manifest.StageId != report.StageID ||
		!proto.Equal(manifest.Configuration, input.RecoveryConfigurationReference()) ||
		len(manifest.Files) > 512 ||
		len(manifest.Files) != len(input.Input.Release.ConfigurationDestinations) {
		return nil, nil, invalidConfigurationDelivery()
	}
	files := make([]entrymaterialization.Header, len(manifest.Files))
	for index, file := range manifest.Files {
		header, err := releaseexecution.DecodeRecoveryConfigurationFile(input, report, file)
		if err != nil {
			return nil, nil, err
		}
		if header.Destination() != input.Input.Release.ConfigurationDestinations[index] {
			return nil, nil, invalidConfigurationDelivery()
		}
		files[index] = header
	}
	return proto.CloneOf(manifest.Configuration), files, nil
}

func (source *ChannelConfigurationSource) OpenRecoveryFile(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	header entrymaterialization.Header,
) (io.ReadCloser, error) {
	if source == nil || input.Validate() != nil || input.Input.Release.Configuration == nil ||
		header.TaskID() != input.Input.Attempt.TaskID ||
		header.EnvironmentID() != input.Input.Release.EnvironmentId ||
		header.Length() > entrymaterialization.MaximumContentBytes ||
		!slices.Contains(input.Input.Release.ConfigurationDestinations, header.Destination()) {
		return nil, invalidConfigurationDelivery()
	}
	generation := input.Input.Release.Configuration.Generation
	if selected := input.RecoveryConfigurationReference(); selected != nil {
		generation = selected.Generation
	}
	if header.Generation() != generation {
		return nil, invalidConfigurationDelivery()
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Input.Attempt)
	if err != nil {
		return nil, err
	}
	delivery, err := source.exchange.ConfigurationFile(
		ctx,
		&agentpb.ReleaseConfigurationFileRequest{
			Attempt:     identity,
			StageId:     header.StepID(),
			Destination: header.Destination(),
		},
	)
	defer releaseexecution.ClearConfigurationDelivery(delivery)
	if err != nil {
		return nil, err
	}
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil || !proto.Equal(delivery.Attempt, identity) ||
		delivery.StageId != header.StepID() ||
		!proto.Equal(delivery.Configuration, input.RecoveryConfigurationReference()) ||
		!proto.Equal(delivery.File, releaseexecution.EncodeConfigurationFile(header)) ||
		uint64(len(delivery.Content)) != header.Length() {
		return nil, invalidConfigurationDelivery()
	}
	digest, expected := sha256.Sum256(delivery.Content), header.Digest()
	if subtle.ConstantTimeCompare(digest[:], expected[:]) != 1 {
		return nil, invalidConfigurationDelivery()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	content := delivery.Content
	delivery.Content = nil
	return entrymaterialization.OwnBytes(content), nil
}
