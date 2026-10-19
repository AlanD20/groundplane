package serviceremoval

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	materialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (service *WorkReports) LoadRemovalConfigurationFile(
	ctx context.Context,
	authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceRemovalConfigurationFileRequest,
) (delivery *agentpb.ServiceRemovalConfigurationFileDelivery, resultErr error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil ||
		entrymaterialization.ValidateDesiredDestination(request.Destination) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Removal configuration file request is invalid")
	}
	selection, err := service.removalFiles(ctx, authorization, request.Attempt, request.StageId)
	if err != nil {
		return nil, err
	}
	index := slices.IndexFunc(
		selection.files,
		func(file taskmaterialization.Record) bool { return file.Destination == request.Destination },
	)
	if index < 0 || selection.reference == nil {
		return nil, errs.New(errs.KindStateConflict, "Requested file is not part of captured removal recovery")
	}
	expected, err := materialization.CapturedFileHeader(
		request.Attempt.TaskId,
		request.StageId,
		selection.reference.Generation,
		selection.files[index],
	)
	if err != nil {
		return nil, err
	}
	header, source, err := service.files.OpenCapturedFile(
		ctx,
		request.Attempt.TaskId,
		request.StageId,
		selection.reference.Generation,
		selection.files[index],
	)
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
		return nil, errs.New(errs.KindStateConflict, "Captured removal file differs from its metadata")
	}
	content := make([]byte, int(header.Length()))
	owned := false
	defer func() {
		if !owned {
			clear(content)
		}
	}()
	if _, err := io.ReadFull(source, content); err != nil {
		return nil, errs.New(errs.KindStateConflict, "Captured removal file transfer ended early")
	}
	var extra [1]byte
	n, err := source.Read(extra[:])
	clear(extra[:])
	digest, expectedDigest := sha256.Sum256(content), header.Digest()
	if n != 0 || !errors.Is(err, io.EOF) || subtle.ConstantTimeCompare(digest[:], expectedDigest[:]) != 1 {
		return nil, errs.New(errs.KindStateConflict, "Captured removal file content differs from its digest")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := service.records.AuthorizeServiceRemovalConfiguration(ctx, selection.guards); err != nil {
		return nil, err
	}
	delivery = &agentpb.ServiceRemovalConfigurationFileDelivery{
		Attempt:       proto.CloneOf(request.Attempt),
		StageId:       request.StageId,
		Configuration: selection.reference,
		File:          removalFileHeader(header),
		Content:       content,
	}
	owned = true
	return delivery, nil
}
