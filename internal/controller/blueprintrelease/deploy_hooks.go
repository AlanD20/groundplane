package blueprintrelease

import (
	"context"
	"crypto/sha256"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	taskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"sort"
	"strconv"
	"strings"
)

type preparedHooks struct {
	preStepIDs  [][]string
	task        etcd.TaskRecord
	members     []etcd.ReleaseTaskRenderMember
	postStepIDs [][]string
	sources     map[string]etcd.ScriptExecutionSources
	executions  int
}

func (service *Service) prepareDeployHooks(
	ctx context.Context,
	input PrepareInput,
	manifest etcd.VersionedReleaseManifest,
	task etcd.TaskRecord,
	members []etcd.ReleaseTaskRenderMember,
) (preparedHooks, error) {
	result := preparedHooks{
		task: task, members: members, preStepIDs: make([][]string, len(members)), postStepIDs: make([][]string, len(members)),
		sources: make(map[string]etcd.ScriptExecutionSources),
	}
	candidateByService := make(map[string]blueprints.EnvironmentBlueprintServiceChange, len(input.ServiceChanges))
	for _, change := range input.ServiceChanges {
		candidateByService[change.Record.Desired.ID] = change
	}
	scriptsByService := deployScriptsByService(input.Scripts)
	projectionValue, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(input.Projection)
	if err != nil {
		return preparedHooks{}, err
	}
	projectionDigest := sha256.Sum256(projectionValue)
	clear(projectionValue)
	var bodyBytes uint64
	for memberIndex := range result.members {
		member := &result.members[memberIndex]
		candidate, exists := candidateByService[member.Render.ServiceID]
		if !exists {
			return preparedHooks{}, errs.New(
				errs.KindInternal,
				"Blueprint candidate Service source is missing",
			)
		}
		serviceValue, err := servicerecord.EncodeServiceRuntimeRecordStorage(candidate.Record)
		if err != nil {
			return preparedHooks{}, err
		}
		serviceDigest := sha256.Sum256(serviceValue)
		clear(serviceValue)
		for _, script := range scriptsByService[member.Render.ServiceID] {
			sources, err := service.scripts.LoadBlueprintReleaseHookExecutionSources(
				ctx,
				manifest.Record.PublicationID,
				script,
				candidate.Record,
				*member,
				input.Tenant,
				input.Project,
				input.Environment,
				input.Projection,
				input.IntendedAttaches,
				manifest.ReadRevision,
			)
			if err != nil {
				return preparedHooks{}, err
			}
			prepared, err := service.preparation.Prepare(ctx, sources)
			if err != nil {
				return preparedHooks{}, err
			}
			stepID := input.AllocateNamed(
				ids.KindStep,
				fmt.Sprintf(
					"blueprint-%s/%s/%s",
					script.Desired.When,
					member.Render.ServiceID,
					sources.Script.Record.Desired.Slug,
				),
			)
			executionID, idErr := allocatedRawULID(
				input.AllocateNamed,
				"blueprint-script-execution/"+member.Render.ServiceID+"/"+sources.Script.Record.Desired.ID,
			)
			if idErr != nil {
				return preparedHooks{}, idErr
			}
			snapshotID, idErr := allocatedRawULID(
				input.AllocateNamed,
				"blueprint-script-snapshot/"+member.Render.ServiceID+"/"+sources.Script.Record.Desired.ID,
			)
			if idErr != nil {
				return preparedHooks{}, idErr
			}
			hook, err := taskplanning.BuildReleaseHookRenderInput(ctx, taskplanning.ManualScriptPlanInput{
				TaskID: task.ID, OperationID: task.OperationID, PlanID: task.PlanID,
				StepID: stepID, ExecutionID: executionID, SnapshotID: snapshotID,
				Sources: sources, Preparation: prepared,
				Candidate: &taskplanning.BlueprintScriptCandidateSources{
					EnvironmentID:     input.Environment.Record.ID,
					RevisionID:        input.Projection.RevisionID,
					RenderGeneration:  input.Projection.RenderGeneration,
					FixedReadRevision: manifest.ReadRevision,
					ServiceSHA256:     serviceDigest,
					ProjectionSHA256:  projectionDigest,
				},
			})
			if err != nil {
				return preparedHooks{}, err
			}
			member.Render.Hooks = append(member.Render.Hooks, hook)
			if script.Desired.When == core.ScriptPreDeploy {
				result.preStepIDs[memberIndex] = append(result.preStepIDs[memberIndex], stepID)
			} else {
				result.postStepIDs[memberIndex] = append(result.postStepIDs[memberIndex], stepID)
			}
			result.sources[executionID] = sources
			result.task.Params[etcd.ReleaseHookStepMemberParam(stepID)] = strconv.Itoa(memberIndex + 1)
			result.task.Params[etcd.ReleaseHookStepExecutionParam(stepID)] = executionID
			result.executions++
			bodyBytes += uint64(hook.BodySize)
			if err := validateDeployHookBounds(result.executions, bodyBytes); err != nil {
				return preparedHooks{}, err
			}
		}
	}
	return result, nil
}

func allocatedRawULID(allocate func(ids.Kind, string) string, name string) (string, error) {
	value := strings.TrimPrefix(allocate(ids.KindTask, name), string(ids.KindTask)+"_")
	if len(value) != 26 {
		return "", errs.New(errs.KindInternal, "Blueprint Script identity allocator is invalid")
	}
	return value, nil
}

func deployScriptsByService(
	scripts []scriptrecord.Record,
) map[string][]scriptrecord.Record {
	result := make(map[string][]scriptrecord.Record)
	for _, script := range scripts {
		if script.Desired.When != core.ScriptPostDeploy && script.Desired.When != core.ScriptPreDeploy {
			continue
		}
		result[script.ServiceID] = append(result[script.ServiceID], script)
	}
	for serviceID := range result {
		sort.Slice(result[serviceID], func(left, right int) bool {
			return core.ScriptBefore(result[serviceID][left].Desired, result[serviceID][right].Desired)
		})
	}
	return result
}

func validateDeployHookBounds(executions int, bodyBytes uint64) error {
	if executions > taskcontract.MaximumBlueprintPostDeployHooks || bodyBytes > 1<<20 {
		return errs.New(
			errs.KindValidationFailed,
			"Blueprint deploy Script selection exceeds its operation bounds",
		)
	}
	return nil
}
