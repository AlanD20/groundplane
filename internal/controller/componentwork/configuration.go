package componentwork

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
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

func (service *WorkReports) LoadComponentConfiguration(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ComponentConfigurationRequest,
) (*agentpb.ComponentConfigurationManifest, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, invalidConfigurationRequest()
	}
	selection, err := service.activeFileSelection(ctx, authorization, request.Attempt, request.StageId)
	if err != nil {
		return nil, err
	}
	manifest := &agentpb.ComponentConfigurationManifest{
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
		manifest.Files = append(manifest.Files, componentexecution.EncodeConfigurationFile(header))
	}
	return manifest, nil
}

func (service *WorkReports) LoadComponentConfigurationFile(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ComponentConfigurationFileRequest,
) (delivery *agentpb.ComponentConfigurationFileDelivery, resultErr error) {
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
				componentexecution.ClearConfigurationDelivery(delivery)
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
	delivery = &agentpb.ComponentConfigurationFileDelivery{
		Attempt: proto.CloneOf(request.Attempt),
		StageId: selection.stage,
		Configuration: proto.CloneOf(
			selection.reference,
		),
		File:    componentexecution.EncodeConfigurationFile(header),
		Content: content,
	}
	owned = true
	return delivery, nil
}

func (service *WorkReports) activeFileSelection(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity,
	stage string,
) (componentFileSelection, error) {
	if service == nil || ctx == nil || service.files == nil {
		return componentFileSelection{}, invalidConfigurationRequest()
	}
	if _, err := authorization.Conditions(); err != nil {
		return componentFileSelection{}, err
	}
	view, err := service.reportView(ctx, authorization, identity)
	if err != nil {
		return componentFileSelection{}, err
	}
	var input componentexecution.HelperInput
	var files []taskmaterialization.Record
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		input, files, err = service.records.LoadRecoveryConfiguration(ctx, view)
	} else {
		input, files, err = service.records.LoadActiveConfiguration(ctx, view)
	}
	if err != nil {
		return componentFileSelection{}, err
	}
	reference, selectedStage, generation, err := componentexecution.ConfigurationSelection(input)
	if err != nil || selectedStage != stage {
		return componentFileSelection{}, invalidConfigurationRequest()
	}
	headers := make([]entrymaterialization.Header, len(files))
	for index, file := range files {
		header, err := materialization.CapturedFileHeader(input.Attempt.TaskID, stage, generation, file)
		if err != nil {
			return componentFileSelection{}, err
		}
		headers[index] = header
	}
	if err := componentexecution.ValidateConfigurationFiles(input, headers); err != nil {
		return componentFileSelection{}, err
	}
	return componentFileSelection{
		attempt:    input.Attempt,
		stage:      stage,
		generation: generation,
		reference:  reference,
		files:      files,
	}, nil
}

type componentFileSelection struct {
	attempt    workexecution.Attempt
	stage      string
	generation uint64
	reference  *agentpb.WorkConfigurationReference
	files      []taskmaterialization.Record
}

func invalidConfigurationRequest() error {
	return errs.New(errs.KindStateConflict, "configuration transfer does not match the active captured Component")
}
