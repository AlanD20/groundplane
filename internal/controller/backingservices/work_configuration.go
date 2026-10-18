package backingservices

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type CapturedFileReader interface {
	OpenCapturedFile(context.Context, string, string, uint64, taskmaterialization.Record) (entrymaterialization.Header, io.ReadCloser, error)
}

func (service *WorkReports) LoadBackingProvisionConfiguration(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackingProvisionConfigurationRequest,
) (*agentpb.BackingProvisionConfigurationManifest, error) {
	if request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, invalidBackingDelivery()
	}
	selection, err := service.activeDelivery(ctx, authorization, request.Attempt, request.StageId, backingprovision.WriteConfiguration)
	if err != nil {
		return nil, err
	}
	if _, err := service.records.LoadConfiguration(ctx, selection.view.Work); err != nil {
		return nil, err
	}
	procedure := selection.input.Assignment.Procedure
	manifest := &agentpb.BackingProvisionConfigurationManifest{Attempt: proto.CloneOf(request.Attempt), StageId: request.StageId,
		Configuration: proto.CloneOf(procedure.Configuration)}
	for _, file := range procedure.Files {
		if _, err := backingprovision.ConfigurationHeader(selection.input, file); err != nil {
			return nil, err
		}
		manifest.Files = append(manifest.Files, proto.CloneOf(file))
	}
	return manifest, service.grantDelivery(ctx, selection)
}

func (service *WorkReports) LoadBackingProvisionFile(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.BackingProvisionFileRequest,
) (delivery *agentpb.BackingProvisionFileDelivery, resultErr error) {
	if request == nil || executionplan.RejectUnknown(request) != nil || entrymaterialization.ValidateDesiredDestination(request.Destination) != nil {
		return nil, invalidBackingDelivery()
	}
	selection, err := service.activeDelivery(ctx, authorization, request.Attempt, request.StageId, backingprovision.WriteConfiguration)
	if err != nil {
		return nil, err
	}
	files, err := service.records.LoadConfiguration(ctx, selection.view.Work)
	if err != nil {
		return nil, err
	}
	index := slices.IndexFunc(files, func(file taskmaterialization.Record) bool { return file.Destination == request.Destination })
	if index < 0 {
		return nil, invalidBackingDelivery()
	}
	procedure := selection.input.Assignment.Procedure
	metadata := procedure.Files[index]
	expected, err := backingprovision.ConfigurationHeader(selection.input, metadata)
	if err != nil {
		return nil, err
	}
	header, source, err := service.files.OpenCapturedFile(ctx, procedure.TaskId, request.StageId, procedure.RenderGeneration, files[index])
	if source != nil {
		defer func() {
			resultErr = errors.Join(resultErr, source.Close())
			if resultErr != nil && delivery != nil {
				clear(delivery.Content)
				delivery = nil
			}
		}()
	}
	if err != nil {
		return nil, err
	}
	if source == nil || header != expected || header.Length() > entrymaterialization.MaximumContentBytes {
		return nil, invalidBackingDelivery()
	}
	content := make([]byte, int(header.Length()))
	transferred := false
	defer func() {
		if !transferred {
			clear(content)
		}
	}()
	if _, err := io.ReadFull(source, content); err != nil {
		return nil, errs.New(errs.KindStateConflict, "captured backing configuration transfer ended early")
	}
	var extra [1]byte
	n, err := source.Read(extra[:])
	clear(extra[:])
	digest, wanted := sha256.Sum256(content), expected.Digest()
	if n != 0 || !errors.Is(err, io.EOF) || subtle.ConstantTimeCompare(digest[:], wanted[:]) != 1 {
		return nil, errs.New(errs.KindStateConflict, "captured backing configuration transfer differs from its metadata")
	}
	if err := service.grantDelivery(ctx, selection); err != nil {
		return nil, err
	}
	delivery = &agentpb.BackingProvisionFileDelivery{Attempt: proto.CloneOf(request.Attempt), StageId: request.StageId,
		Configuration: proto.CloneOf(procedure.Configuration), File: proto.CloneOf(metadata), Content: content}
	transferred = true
	return delivery, nil
}
