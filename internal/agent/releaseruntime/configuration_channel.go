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
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ConfigurationExchange interface {
	Configuration(context.Context, *agentpb.ReleaseConfigurationRequest) (*agentpb.ReleaseConfigurationManifest, error)
	ConfigurationFile(context.Context, *agentpb.ReleaseConfigurationFileRequest) (*agentpb.ReleaseConfigurationFileDelivery, error)
}

type ChannelConfigurationSource struct{ exchange ConfigurationExchange }

func NewChannelConfigurationSource(exchange ConfigurationExchange) (*ChannelConfigurationSource, error) {
	if exchange == nil {
		return nil, errs.New(errs.KindInternal, "Release configuration exchange is not configured")
	}
	return &ChannelConfigurationSource{exchange: exchange}, nil
}

func (source *ChannelConfigurationSource) Describe(ctx context.Context, input releaseexecution.Input, report releaseexecution.CheckpointReport) (*agentpb.WorkConfigurationReference, []entrymaterialization.Header, error) {
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, nil, err
	}
	manifest, err := source.exchange.Configuration(ctx, &agentpb.ReleaseConfigurationRequest{Attempt: identity, StageId: report.StageID})
	if err != nil {
		return nil, nil, err
	}
	if manifest == nil || executionplan.RejectUnknown(manifest) != nil || !proto.Equal(manifest.Attempt, identity) || manifest.StageId != report.StageID ||
		!proto.Equal(manifest.Configuration, input.Release.Configuration) || len(manifest.Files) > 512 || len(manifest.Files) != len(input.Release.ConfigurationDestinations) {
		return nil, nil, invalidConfigurationDelivery()
	}
	files := make([]entrymaterialization.Header, len(manifest.Files))
	for index, file := range manifest.Files {
		header, err := releaseexecution.DecodeConfigurationFile(input, report, file)
		if err != nil {
			return nil, nil, err
		}
		if header.Destination() != input.Release.ConfigurationDestinations[index] {
			return nil, nil, invalidConfigurationDelivery()
		}
		files[index] = header
	}
	return proto.CloneOf(manifest.Configuration), files, nil
}

func (source *ChannelConfigurationSource) Open(ctx context.Context, input releaseexecution.Input, header entrymaterialization.Header) (io.ReadCloser, error) {
	if input.Release == nil || input.Release.Configuration == nil || header.TaskID() != input.Attempt.TaskID ||
		header.EnvironmentID() != input.Release.EnvironmentId || header.Generation() != input.Release.Configuration.Generation ||
		!slices.Contains(input.Release.ConfigurationDestinations, header.Destination()) || header.Length() > entrymaterialization.MaximumContentBytes {
		return nil, invalidConfigurationDelivery()
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	delivery, err := source.exchange.ConfigurationFile(ctx, &agentpb.ReleaseConfigurationFileRequest{Attempt: identity, StageId: header.StepID(), Destination: header.Destination()})
	defer releaseexecution.ClearConfigurationDelivery(delivery)
	if err != nil {
		return nil, err
	}
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil || !proto.Equal(delivery.Attempt, identity) || delivery.StageId != header.StepID() ||
		!proto.Equal(delivery.Configuration, input.Release.Configuration) || !proto.Equal(delivery.File, releaseexecution.EncodeConfigurationFile(header)) ||
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
	delivery.Content = nil // ownership transfers to the clearing reader
	return entrymaterialization.OwnBytes(content), nil
}

func invalidConfigurationDelivery() error {
	return errs.New(errs.KindStateConflict, "Release configuration response differs from the captured file selection")
}
