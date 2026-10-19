package componentruntime

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"io"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type ConfigurationExchange interface {
	ComponentConfiguration(
		context.Context,
		*agentpb.ComponentConfigurationRequest,
	) (*agentpb.ComponentConfigurationManifest, error)
	ComponentConfigurationFile(
		context.Context,
		*agentpb.ComponentConfigurationFileRequest,
	) (*agentpb.ComponentConfigurationFileDelivery, error)
}

type ChannelConfigurationSource struct{ exchange ConfigurationExchange }

func NewChannelConfigurationSource(exchange ConfigurationExchange) (*ChannelConfigurationSource, error) {
	if exchange == nil {
		return nil, errs.New(errs.KindInternal, "Component configuration exchange is not configured")
	}
	return &ChannelConfigurationSource{exchange: exchange}, nil
}

func (source *ChannelConfigurationSource) DescribeComponent(
	ctx context.Context,
	input componentexecution.HelperInput,
) (*agentpb.WorkConfigurationReference, []entrymaterialization.Header, error) {
	reference, stage, _, err := componentexecution.ConfigurationSelection(input)
	if err != nil {
		return nil, nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, nil, err
	}
	manifest, err := source.exchange.ComponentConfiguration(
		ctx,
		&agentpb.ComponentConfigurationRequest{Attempt: identity, StageId: stage},
	)
	if err != nil {
		return nil, nil, err
	}
	if manifest == nil || executionplan.RejectUnknown(manifest) != nil || !proto.Equal(manifest.Attempt, identity) ||
		manifest.StageId != stage ||
		!proto.Equal(manifest.Configuration, reference) ||
		len(manifest.Files) > 512 ||
		len(manifest.Files) != len(input.Procedure.Files) {
		return nil, nil, invalidConfigurationDelivery()
	}
	files := make([]entrymaterialization.Header, len(manifest.Files))
	for index, file := range manifest.Files {
		header, err := componentexecution.DecodeConfigurationFile(input, file)
		if err != nil {
			return nil, nil, err
		}
		if header.Destination() != input.Procedure.Files[index].Destination {
			return nil, nil, invalidConfigurationDelivery()
		}
		files[index] = header
	}
	if err := componentexecution.ValidateConfigurationFiles(input, files); err != nil {
		return nil, nil, err
	}
	return proto.CloneOf(manifest.Configuration), files, nil
}

func (source *ChannelConfigurationSource) OpenComponentFile(
	ctx context.Context,
	input componentexecution.HelperInput,
	header entrymaterialization.Header,
) (io.ReadCloser, error) {
	reference, _, generation, err := componentexecution.ConfigurationSelection(input)
	if err != nil {
		return nil, err
	}
	if header.TaskID() != input.Attempt.TaskID ||
		header.EnvironmentID() != input.Procedure.EnvironmentId || header.Generation() != generation ||
		!componentFileSelected(input, header) || header.Length() > entrymaterialization.MaximumContentBytes {
		return nil, invalidConfigurationDelivery()
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return nil, err
	}
	delivery, err := source.exchange.ComponentConfigurationFile(
		ctx,
		&agentpb.ComponentConfigurationFileRequest{
			Attempt:     identity,
			StageId:     header.StepID(),
			Destination: header.Destination(),
		},
	)
	defer componentexecution.ClearConfigurationDelivery(delivery)
	if err != nil {
		return nil, err
	}
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil || !proto.Equal(delivery.Attempt, identity) ||
		delivery.StageId != header.StepID() ||
		!proto.Equal(delivery.Configuration, reference) ||
		!proto.Equal(delivery.File, componentexecution.EncodeConfigurationFile(header)) ||
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
	return errs.New(errs.KindStateConflict, "Component configuration response differs from the captured file selection")
}

func componentFileSelected(input componentexecution.HelperInput, header entrymaterialization.Header) bool {
	_, stage, _, err := componentexecution.ConfigurationSelection(input)
	if err != nil || header.StepID() != stage {
		return false
	}
	for _, file := range input.Procedure.Files {
		if file.Destination == header.Destination() {
			if input.Recovery != nil {
				return true
			}
			digest := header.Digest()
			return header.Length() == file.Length && subtle.ConstantTimeCompare(digest[:], file.Sha256) == 1
		}
	}
	return false
}
