package volume

import (
	"encoding/json"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"net/http"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removal "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func buildVolumeMutationPlan(
	volumeRoot string,
	planID string,
	generation uint64,
	request volumeMutationRequest,
	oldArtifact *agentpb.ComposeArtifact,
	newArtifact *agentpb.ComposeArtifact,
	intentDigest []byte,
	consumerIDs []string,
) (*agentpb.ExecutionPlan, []etcd.TaskStepRecord, error) {
	steps := make([]*agentpb.ExecutionStep, 0, 4)
	timeout := volumeMutationTimeoutSeconds
	if request.action == volumeMutationActionRemove {
		timeout = removal.TimeoutSeconds
	}
	appendStep := func(payload isVolumeExecutionStepPayload) {
		steps = append(steps, payload.step(ids.New(ids.KindStep), uint32(timeout)))
	}
	if request.action == volumeMutationActionAdd {
		appendStep(
			volumeEnsurePayload{artifactID: newArtifact.ArtifactId, volumeID: request.volumeID, intent: intentDigest},
		)
	}
	operation := agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	artifacts := []*agentpb.ComposeArtifact{newArtifact}
	if request.action == volumeMutationActionRemove {
		operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
		artifacts = append(artifacts, oldArtifact)
		if len(consumerIDs) != 0 {
			appendStep(volumeComposeApplyPayload{artifactID: newArtifact.ArtifactId, serviceIDs: consumerIDs})
		}
		appendStep(volumeDockerRemovePayload{volumeID: request.volumeID})
		appendStep(volumeDirectoryRemovePayload{
			artifactID: oldArtifact.ArtifactId, volumeID: request.volumeID, key: request.key, intent: intentDigest,
		})
	} else {
		appendStep(volumeDockerEnsurePayload{artifactID: newArtifact.ArtifactId, volumeID: request.volumeID,
			requireExisting: request.action == volumeMutationActionEdit})
	}
	plan, err := taskplan.Build(taskplan.BuildInput{
		VolumeRoot: volumeRoot, PlanID: planID, RenderGeneration: generation,
		Operation: operation, TargetID: request.volumeID, Artifacts: artifacts, Steps: steps,
	})
	if err != nil {
		return nil, nil, err
	}
	records := make([]etcd.TaskStepRecord, len(steps))
	for index, step := range steps {
		records[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: step.StepId}
	}
	return plan, records, nil
}

type isVolumeExecutionStepPayload interface {
	step(string, uint32) *agentpb.ExecutionStep
}

type volumeEnsurePayload struct {
	artifactID, volumeID string
	intent               []byte
}

func (value volumeEnsurePayload) step(id string, timeout uint32) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId:         id,
		TimeoutSeconds: timeout,
		Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
			ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{
				ArtifactId: value.artifactID, VolumeIds: []string{value.volumeID}, IntentSha256: append([]byte(nil), value.intent...),
			},
		},
	}
}

type volumeComposeApplyPayload struct {
	artifactID string
	serviceIDs []string
}

func (value volumeComposeApplyPayload) step(id string, timeout uint32) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{StepId: id, TimeoutSeconds: timeout, Payload: &agentpb.ExecutionStep_ComposeApply{
		ComposeApply: &agentpb.ComposeApply{
			ArtifactId: value.artifactID, ServiceIds: append([]string(nil), value.serviceIDs...), NoDependencies: true,
		},
	}}
}

type volumeDockerEnsurePayload struct {
	artifactID, volumeID string
	requireExisting      bool
}

func (value volumeDockerEnsurePayload) step(id string, timeout uint32) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{StepId: id, TimeoutSeconds: timeout,
		Payload: &agentpb.ExecutionStep_ManagedVolumeEnsure{ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{
			ArtifactId: value.artifactID, VolumeId: value.volumeID, RequireExisting: value.requireExisting,
		}}}
}

type volumeDockerRemovePayload struct{ volumeID string }

func (value volumeDockerRemovePayload) step(id string, timeout uint32) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId:         id,
		TimeoutSeconds: timeout,
		Payload: &agentpb.ExecutionStep_ManagedVolumeRemove{
			ManagedVolumeRemove: &agentpb.ManagedVolumeRemove{
				VolumeId: value.volumeID, DockerName: "gp_vol_" + strings.ToLower(value.volumeID),
			},
		},
	}
}

type volumeDirectoryRemovePayload struct {
	artifactID, volumeID, key string
	intent                    []byte
}

func (value volumeDirectoryRemovePayload) step(id string, timeout uint32) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId:         id,
		TimeoutSeconds: timeout,
		Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoryRemove{
			ManagedVolumeDirectoryRemove: &agentpb.ManagedVolumeDirectoryRemove{
				ArtifactId: value.artifactID, VolumeId: value.volumeID, ComposeKey: value.key,
				IntentSha256: append([]byte(nil), value.intent...),
			},
		},
	}
}

func volumeMutationTaskType(action string) etcd.TaskType {
	switch action {
	case volumeMutationActionAdd:
		return etcd.TaskCreate
	case volumeMutationActionRemove:
		return etcd.TaskRemove
	default:
		return etcd.TaskUpdate
	}
}

func stableIDFromTask(kind ids.Kind, taskID string) string {
	if ids.Validate(ids.KindTask, taskID) != nil || len(taskID) <= len("task_") {
		return ""
	}
	return string(kind) + "_" + strings.TrimPrefix(taskID, "task_")
}

func volumeMutationConsumerIDs(projection etcd.EnvironmentComposeProjection, volumeID string) []string {
	seen := make(map[string]struct{})
	for _, mount := range projection.VolumeMounts {
		if mount.VolumeID == volumeID {
			seen[mount.ServiceID] = struct{}{}
		}
	}
	result := make([]string, 0, len(seen))
	for serviceID := range seen {
		result = append(result, serviceID)
	}
	sort.Strings(result)
	return result
}

func volumeMutationResponse(
	request volumeMutationRequest,
	environment hierarchyrecord.EnvironmentRecord,
	taskID string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if request.action == volumeMutationActionRemove {
		value, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
		if err != nil {
			return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
		}
		return idempotencyrecord.IdempotencyResponse{
			Status: http.StatusAccepted, ContentKind: "application/json", Body: value,
		}, nil
	}
	status := http.StatusCreated
	state := "creating"
	if request.action == volumeMutationActionEdit {
		status = http.StatusOK
		state = "active"
	}
	originTaskID := ""
	if request.action == volumeMutationActionAdd {
		originTaskID = taskID
	}
	volume := volumeView(environment, etcd.VolumeRecord{
		ID: request.volumeID, EnvironmentID: request.environmentID, Slug: request.slug, Key: request.key,
	}, originTaskID, state)
	currentTaskID := taskID
	volume.CurrentTaskID = &currentTaskID
	if request.action == volumeMutationActionAdd {
		createTaskID := taskID
		volume.CreateTaskID = &createTaskID
	}
	body := apiTypes.VolumeMutationResponse{
		Volume: volume,
		TaskID: taskID,
	}
	value, err := json.Marshal(body)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	return idempotencyrecord.IdempotencyResponse{Status: status, ContentKind: "application/json", Body: value}, nil
}
