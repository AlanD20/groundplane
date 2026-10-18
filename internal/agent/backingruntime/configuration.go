package backingruntime

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (executor *Executor) configure(ctx context.Context, input backingprovision.HelperInput, exchange Exchange, inspectOnly bool) (backingprovision.Observation, error) {
	attempt, procedure := input.Assignment.Attempt, input.Assignment.Procedure
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	stage, err := backingprovision.StageID(attempt.WorkItemID, backingprovision.WriteConfiguration)
	if err != nil {
		return backingprovision.Observation{}, err
	}
	manifest, err := exchange.BackingProvisionConfiguration(ctx, &agentpb.BackingProvisionConfigurationRequest{Attempt: identity, StageId: stage})
	if err != nil {
		return backingprovision.Observation{}, err
	}
	if manifest == nil || executionplan.RejectUnknown(manifest) != nil || !proto.Equal(manifest.Attempt, identity) ||
		manifest.StageId != stage || !proto.Equal(manifest.Configuration, procedure.Configuration) || len(manifest.Files) != len(procedure.Files) {
		return backingprovision.Observation{}, invalidInput()
	}
	for index, file := range manifest.Files {
		if !proto.Equal(file, procedure.Files[index]) {
			return backingprovision.Observation{}, invalidInput()
		}
		if _, err := backingprovision.ConfigurationHeader(input, file); err != nil {
			return backingprovision.Observation{}, err
		}
	}
	if inspectOnly {
		if err := executor.materializer.StopBackingFiles(ctx, input); err != nil {
			return backingprovision.Observation{}, err
		}
	}
	ready := true
	for _, file := range manifest.Files {
		if err := executor.ownership.Check(); err != nil {
			return backingprovision.Observation{}, err
		}
		source, err := backingFileSource(ctx, exchange, input, file)
		if err != nil {
			return backingprovision.Observation{}, err
		}
		err = executor.materializer.RunBackingFile(ctx, input, file, source, inspectOnly)
		if inspectOnly && errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			ready = false
			continue
		}
		if err != nil {
			return backingprovision.Observation{}, err
		}
		if !inspectOnly {
			source, err := backingFileSource(ctx, exchange, input, file)
			if err != nil {
				return backingprovision.Observation{}, err
			}
			if err := executor.materializer.RunBackingFile(ctx, input, file, source, true); err != nil {
				return backingprovision.Observation{}, err
			}
		}
	}
	if err := executor.ownership.Check(); err != nil {
		return backingprovision.Observation{}, err
	}
	return backingprovision.Observation{ServiceID: input.Layout.ServiceID, Input: attempt.Input,
		Stage: backingprovision.WriteConfiguration, FilesWritten: ready, At: time.Now().UTC()}, nil
}

func backingFileSource(ctx context.Context, exchange Exchange, input backingprovision.HelperInput,
	file *agentpb.MaterializeFile,
) (io.ReadCloser, error) {
	header, err := backingprovision.ConfigurationHeader(input, file)
	if err != nil || header.Length() > entrymaterialization.MaximumContentBytes {
		return nil, invalidInput()
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Assignment.Attempt)
	if err != nil {
		return nil, err
	}
	delivery, err := exchange.BackingProvisionFile(ctx, &agentpb.BackingProvisionFileRequest{Attempt: identity,
		StageId: header.StepID(), Destination: header.Destination()})
	defer func() {
		if delivery != nil {
			clear(delivery.Content)
			delivery.Content = nil
		}
	}()
	if err != nil {
		return nil, err
	}
	if delivery == nil || executionplan.RejectUnknown(delivery) != nil || !proto.Equal(delivery.Attempt, identity) ||
		delivery.StageId != header.StepID() || !proto.Equal(delivery.Configuration, input.Assignment.Procedure.Configuration) ||
		!proto.Equal(delivery.File, file) || uint64(len(delivery.Content)) != header.Length() {
		return nil, invalidInput()
	}
	digest, expected := sha256.Sum256(delivery.Content), header.Digest()
	if subtle.ConstantTimeCompare(digest[:], expected[:]) != 1 {
		return nil, invalidInput()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	content := delivery.Content
	delivery.Content = nil
	return entrymaterialization.OwnBytes(content), nil
}
