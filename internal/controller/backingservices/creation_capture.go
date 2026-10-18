package backingservices

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type creationCapture struct {
	PlanID      string
	Environment hierarchy.EnvironmentRecord
	Service     core.Service
	Artifact    *agentpb.ComposeArtifact
	Entries     []entries.Record
	Resolved    map[string]string
	Allocator   *desiredrevision.BlueprintIdentityAllocator
}

func (service *CreationService) captureCreation(ctx context.Context, capture creationCapture) (backingprovisionwork.Publication, error) {
	placement, err := service.repository.ResolveResourceHost(ctx)
	if err != nil {
		return backingprovisionwork.Publication{}, err
	}
	environment, allocator := capture.Environment, capture.Allocator
	workID, operationID := allocator.Named(ids.KindWorkItem, "backing-create"), allocator.Named(ids.KindOperation, "operation")
	budget := time.Duration(desiredrevision.TaskTimeoutSeconds) * time.Second
	procedure := &agentpb.BackingProvisionInput{TaskId: environment.CreateTaskID, WorkItemId: workID, OperationId: operationID,
		ProjectId: environment.ProjectID, EnvironmentId: environment.ID, ServiceId: capture.Service.ID,
		HostId: placement.HostID, VolumeRoot: service.volumeRoot, VolumeDirectory: environment.VolumeDir,
		RenderGeneration: 1, Runtime: capture.Artifact, CreatedAt: timestamppb.New(environment.CreatedAt)}
	for _, volume := range capture.Artifact.Volumes {
		procedure.ManagedVolumeIds = append(procedure.ManagedVolumeIds, volume.VolumeId)
	}
	var files []taskmaterialization.Record
	if len(capture.Entries) != 0 {
		record, file, err := backingEnvironmentMaterialization(environment.ID, capture.Artifact.ArtifactId, workID,
			capture.Entries, capture.Resolved, allocator)
		if err != nil {
			return backingprovisionwork.Publication{}, err
		}
		files, procedure.Files = []taskmaterialization.Record{record}, []*agentpb.MaterializeFile{file}
	}
	var envelope *taskconfiguration.BackingHookEncryptedInputs
	if hooks := capture.Service.Hooks; hooks != nil && hooks.AfterStart != nil {
		if service.hookInputs == nil {
			return backingprovisionwork.Publication{}, errs.New(errs.KindInternal, "backing creation hook capture is unavailable")
		}
		definition := hooks.AfterStart
		procedure.AfterStart = &agentpb.BackingProvisionAfterStart{Command: append([]string(nil), definition.Command...), TimeoutSeconds: definition.TimeoutSeconds}
		if minimum := time.Duration(definition.TimeoutSeconds)*time.Second + 30*time.Second; budget < minimum {
			budget = minimum
		}
		envelope, err = service.hookInputs.SealBackingHookTaskInputs(ctx, operationID, environment.ProjectID, *hooks)
		if err != nil {
			return backingprovisionwork.Publication{}, err
		}
		if envelope != nil {
			defer clear(envelope.Ciphertext)
		}
	}
	procedure.Deadline = timestamppb.New(environment.CreatedAt.Add(budget))
	targetName := capture.Service.Label
	if targetName == "" {
		targetName = capture.Service.Name
	}
	return service.workInputs.PrepareCreation(ctx, backingprovisionwork.CreationCapture{PlanID: capture.PlanID,
		TargetName: targetName, ConfigurationID: allocator.Named(ids.KindConfig, "backing-configuration"),
		Environment: environment, Procedure: procedure, Files: files, Hook: envelope})
}
