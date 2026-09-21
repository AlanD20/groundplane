package backingservices

import (
	"crypto/sha256"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"

	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backingCreationStepInput struct {
	Environment                                           hierarchyrecord.EnvironmentRecord
	Spec                                                  adapters.CreationSpec
	TaskID, ServiceID, ArtifactID, VolumeID, IntentDigest string
	Volume                                                *core.Volume
	Entries                                               []entryrecord.Record
	Resolved                                              map[string]string
	Allocator                                             *desiredrevision.BlueprintIdentityAllocator
}
type backingCreationSteps struct {
	steps            []*agentpb.ExecutionStep
	records          []taskjournal.TaskStepRecord
	params           map[string]string
	materializations []materializationrecord.Record
}

func prepareBackingCreationSteps(input backingCreationStepInput) (backingCreationSteps, error) {
	environment, spec, allocator := input.Environment, input.Spec, input.Allocator
	serviceID, artifactID, volume, volumeID := input.ServiceID, input.ArtifactID, input.Volume, input.VolumeID
	entries, resolved := input.Entries, input.Resolved
	environmentStepID := allocator.Named(ids.KindStep, "environment-directory")
	applyStepID := allocator.Named(ids.KindStep, "compose-apply")
	steps := []*agentpb.ExecutionStep{
		{
			StepId:         environmentStepID,
			TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId:     environment.ID,
					ExpectedVolumeDir: environment.VolumeDir,
				},
			},
		},
	}
	stepRecords := []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: environmentStepID}}
	taskParams := map[string]string{
		blueprints.EnvironmentDesiredRevisionParam:         input.TaskID,
		taskjournal.TaskMaterializationEnvironmentParam:    environment.ID,
		taskjournal.TaskBackingServiceCreationParam:        serviceID,
		taskjournal.TaskBackingServiceVolumeDirectoryParam: environment.VolumeDir,
		taskcontract.EnvironmentBlueprintArtifactParam:     artifactID,
		taskcontract.EnvironmentBlueprintProcedureParam: string(
			taskcontract.BlueprintComposeProcedureFullReconcile,
		),
	}
	if volume != nil {
		intentDigest, decodeErr := hex.DecodeString(input.IntentDigest)
		if decodeErr != nil || len(intentDigest) != sha256.Size {
			return backingCreationSteps{}, errs.New(
				errs.KindInternal,
				"Backing-service protected intent digest is invalid",
			)
		}
		volumeStepID := allocator.Named(ids.KindStep, "managed-volume-directories")
		steps = append(steps, &agentpb.ExecutionStep{
			StepId:         volumeStepID,
			TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
				ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
					ArtifactId:   artifactID,
					VolumeIds:    []string{volumeID},
					IntentSha256: append([]byte(nil), intentDigest...),
				},
			},
		})
		stepRecords = append(
			stepRecords,
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: volumeStepID},
		)
		taskParams[taskcontract.EnvironmentBlueprintManagedVolumesParam] = volumeID
		taskParams[taskplanning.VolumeTaskIntentSHA256Param] = hex.EncodeToString(intentDigest)
	}
	materializations := []materializationrecord.Record(nil)
	if spec.HasEnvironment() {
		materialization, materializeStep, materializeErr := backingEnvironmentMaterialization(
			environment.ID, artifactID, entries, resolved, allocator,
		)
		if materializeErr != nil {
			return backingCreationSteps{}, materializeErr
		}
		steps = append(steps, materializeStep)
		stepRecords = append(
			stepRecords,
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: materializeStep.StepId},
		)
		materializations = []materializationrecord.Record{materialization}
	}
	steps = append(steps, &agentpb.ExecutionStep{
		StepId:         applyStepID,
		TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
		Payload: &agentpb.ExecutionStep_ComposeApply{
			ComposeApply: &agentpb.ComposeApply{ArtifactId: artifactID, FullReconcile: true},
		},
	})
	stepRecords = append(stepRecords, taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: applyStepID})
	if spec.HasHealthcheck() {
		healthStepID := allocator.Named(ids.KindStep, "wait-healthy")
		steps = append(steps, &agentpb.ExecutionStep{
			StepId:         healthStepID,
			TimeoutSeconds: uint32(desiredrevision.TaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_WaitHealthy{
				WaitHealthy: &agentpb.WaitHealthy{ArtifactId: artifactID, ServiceIds: []string{serviceID}},
			},
		})
		stepRecords = append(
			stepRecords,
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: healthStepID},
		)
		taskParams[taskjournal.TaskBackingServiceHealthParam] = serviceID
	}

	return backingCreationSteps{
		steps:            steps,
		records:          stepRecords,
		params:           taskParams,
		materializations: materializations,
	}, nil
}
