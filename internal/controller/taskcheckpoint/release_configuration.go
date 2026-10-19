package taskcheckpoint

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	materialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type CapturedFileReader interface {
	OpenCapturedFile(
		context.Context,
		string,
		string,
		uint64,
		taskmaterialization.Record,
	) (entrymaterialization.Header, io.ReadCloser, error)
}

func (service *ReleaseCheckpointService) LoadReleaseConfiguration(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ReleaseConfigurationRequest,
) (*agentpb.ReleaseConfigurationManifest, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, invalidConfigurationRequest()
	}
	selection, err := service.activeFileSelection(ctx, authorization, request.Attempt, request.StageId)
	if err != nil {
		return nil, err
	}
	manifest := &agentpb.ReleaseConfigurationManifest{
		Attempt:       proto.CloneOf(request.Attempt),
		StageId:       selection.stage,
		Configuration: proto.CloneOf(selection.reference),
	}
	for _, file := range selection.files {
		header, err := materialization.CapturedFileHeader(
			selection.attempt.TaskID,
			selection.stage,
			selection.generation,
			file,
		)
		if err != nil {
			return nil, err
		}
		manifest.Files = append(manifest.Files, releaseexecution.EncodeConfigurationFile(header))
	}
	return manifest, nil
}

func (service *ReleaseCheckpointService) LoadReleaseConfigurationFile(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ReleaseConfigurationFileRequest,
) (delivery *agentpb.ReleaseConfigurationFileDelivery, resultErr error) {
	if request == nil || executionplan.RejectUnknown(request) != nil ||
		entrymaterialization.ValidateDesiredDestination(request.Destination) != nil {
		return nil, invalidConfigurationRequest()
	}
	selection, err := service.activeFileSelection(ctx, authorization, request.Attempt, request.StageId)
	if err != nil {
		return nil, err
	}
	index := slices.IndexFunc(
		selection.files,
		func(file taskmaterialization.Record) bool { return file.Destination == request.Destination },
	)
	if index < 0 {
		return nil, invalidConfigurationRequest()
	}
	expected, err := materialization.CapturedFileHeader(
		selection.attempt.TaskID,
		selection.stage,
		selection.generation,
		selection.files[index],
	)
	if err != nil {
		return nil, err
	}
	header, source, err := service.files.OpenCapturedFile(
		ctx,
		selection.attempt.TaskID,
		selection.stage,
		selection.generation,
		selection.files[index],
	)
	if source != nil {
		defer func() {
			resultErr = errors.Join(resultErr, source.Close())
			if resultErr != nil {
				releaseexecution.ClearConfigurationDelivery(delivery)
				delivery = nil
			}
		}()
	}
	if err != nil {
		return nil, err
	}
	if source == nil || header != expected || header.Length() > entrymaterialization.MaximumContentBytes {
		return nil, invalidConfigurationRequest()
	}
	content := make([]byte, int(header.Length()))
	owned := false
	defer func() {
		if !owned {
			clear(content)
		}
	}()
	if _, err := io.ReadFull(source, content); err != nil {
		return nil, errs.New(errs.KindStateConflict, "captured file transfer ended early")
	}
	var extra [1]byte
	n, err := source.Read(extra[:])
	clear(extra[:])
	digest, expectedDigest := sha256.Sum256(content), header.Digest()
	if n != 0 || !errors.Is(err, io.EOF) || subtle.ConstantTimeCompare(digest[:], expectedDigest[:]) != 1 {
		return nil, errs.New(errs.KindStateConflict, "captured file transfer differs from its metadata")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	delivery = &agentpb.ReleaseConfigurationFileDelivery{
		Attempt: proto.CloneOf(request.Attempt),
		StageId: selection.stage,
		Configuration: proto.CloneOf(
			selection.reference,
		),
		File:    releaseexecution.EncodeConfigurationFile(header),
		Content: content,
	}
	owned = true
	return delivery, nil
}

func (service *ReleaseCheckpointService) activeConfiguration(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity,
	stage string,
) (releaseexecution.Input, releaseexecution.CheckpointReport, []taskmaterialization.Record, error) {
	var empty releaseexecution.Input
	var noReport releaseexecution.CheckpointReport
	if service == nil || ctx == nil || service.files == nil {
		return empty, noReport, nil, invalidConfigurationRequest()
	}
	if _, err := authorization.Conditions(); err != nil {
		return empty, noReport, nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return empty, noReport, nil, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return empty, noReport, nil, err
	}
	if !view.ActiveAttempt || !view.CurrentExecutor || !workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		view.Attempt.Attempt.StartedAt == nil ||
		view.Work.Task.Task.FinishedAt != nil ||
		view.Work.Task.Task.CollectionStartedAt != nil {
		return empty, noReport, nil, invalidConfigurationRequest()
	}
	owner := service.forwards[view.Work.Item.Item.Operation]
	if owner == nil {
		return empty, noReport, nil, invalidConfigurationRequest()
	}
	input, report, files, err := owner.LoadActiveConfiguration(ctx, view)
	if err != nil {
		return empty, noReport, nil, err
	}
	if !workexecution.SameAssignment(input.Attempt, view.Attempt.Attempt) || report.StageID != stage ||
		report.Ack == nil ||
		report.ValidateEnvelope(input.Attempt) != nil ||
		report.Progress.Validate(input) != nil ||
		!report.Progress.InFlight ||
		report.Progress.Phase != releaseexecution.Configuration ||
		len(files) > 512 ||
		len(files) != len(input.Release.ConfigurationDestinations) {
		return empty, noReport, nil, invalidConfigurationRequest()
	}
	for index, file := range files {
		if file.EnvironmentID != input.Release.EnvironmentId ||
			file.Destination != input.Release.ConfigurationDestinations[index] {
			return empty, noReport, nil, invalidConfigurationRequest()
		}
	}
	return input, report, files, nil
}

func invalidConfigurationRequest() error {
	return errs.New(errs.KindStateConflict, "configuration transfer does not match the active captured Release")
}
