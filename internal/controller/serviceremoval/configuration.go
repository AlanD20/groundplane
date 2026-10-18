package serviceremoval

import (
	"context"
	"encoding/hex"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	materialization "github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type removalFileSelection struct {
	reference *agentpb.WorkConfigurationReference
	files     []taskmaterialization.Record
	guards    []keyvalue.Condition
}

func (service *WorkReports) removalFiles(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity, stage string,
) (removalFileSelection, error) {
	guards, err := authorization.Conditions()
	if err != nil {
		return removalFileSelection{}, err
	}
	view, err := service.recoveryView(ctx, authorization, identity)
	if err != nil {
		return removalFileSelection{}, err
	}
	if view.Checkpoint.Record == nil || view.Checkpoint.Record.StageID != stage || !time.Now().UTC().Before(view.Attempt.Attempt.Deadline) {
		return removalFileSelection{}, errs.New(errs.KindStateConflict, "Removal file delivery requires its active recovery step")
	}
	captured, _, err := service.inputs.Load(ctx, view.Work)
	if err != nil {
		return removalFileSelection{}, err
	}
	input, err := BuildHostInput(captured)
	if err != nil {
		return removalFileSelection{}, err
	}
	reference, files, owner, err := service.records.LoadServiceRemovalRecoveryConfiguration(ctx, view, input)
	if err != nil {
		return removalFileSelection{}, err
	}
	selection := removalFileSelection{files: files, guards: append(guards, owner...)}
	if reference != nil {
		digest, err := hex.DecodeString(reference.SHA256)
		if err != nil {
			return removalFileSelection{}, err
		}
		selection.reference = &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID,
			EnvironmentId: reference.EnvironmentID, Generation: reference.Generation, Sha256: digest}
	} else if len(files) != 0 {
		return removalFileSelection{}, errs.New(errs.KindStateConflict, "Removal recovery files have no captured configuration")
	}
	return selection, nil
}

func (service *WorkReports) LoadRemovalConfiguration(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.ServiceRemovalConfigurationRequest,
) (*agentpb.ServiceRemovalConfigurationManifest, error) {
	if ctx == nil || request == nil || executionplan.RejectUnknown(request) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Removal configuration request is invalid")
	}
	selection, err := service.removalFiles(ctx, authorization, request.Attempt, request.StageId)
	if err != nil {
		return nil, err
	}
	manifest := &agentpb.ServiceRemovalConfigurationManifest{Attempt: proto.CloneOf(request.Attempt), StageId: request.StageId, Configuration: selection.reference}
	for _, file := range selection.files {
		header, err := materialization.CapturedFileHeader(request.Attempt.TaskId, request.StageId, selection.reference.Generation, file)
		if err != nil {
			return nil, err
		}
		manifest.Files = append(manifest.Files, removalFileHeader(header))
	}
	if err := service.records.AuthorizeServiceRemovalConfiguration(ctx, selection.guards); err != nil {
		return nil, err
	}
	return manifest, nil
}

func removalFileHeader(header entrymaterialization.Header) *agentpb.ServiceRemovalConfigurationFile {
	digest := header.Digest()
	return &agentpb.ServiceRemovalConfigurationFile{Destination: header.Destination(), ServiceId: header.ServiceID(), ServiceName: header.ServiceName(),
		OutputKind: header.OutputKind().String(), Uid: header.UID(), Gid: header.GID(), Mode: uint32(header.Mode()), Length: header.Length(), Sha256: slices.Clone(digest[:])}
}
