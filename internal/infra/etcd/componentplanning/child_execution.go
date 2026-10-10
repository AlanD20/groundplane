package componentplanning

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/core"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ChildExecution is the private, immutable authority of one Component unit.
// Plan contains only the closed machine recipe and materialization metadata;
// Secret bytes remain in the existing transient materialization channel.
type ChildExecution struct {
	TaskID           string                               `json:"task_id"`
	ParentTaskID     string                               `json:"parent_task_id"`
	EnvironmentID    string                               `json:"environment_id"`
	Component        componentrecord.Record               `json:"component"`
	Zones            []zonerecord.Record                  `json:"zones"`
	Plan             []byte                               `json:"plan"`
	Configuration    *taskconfiguration.TaskConfiguration `json:"configuration,omitempty"`
	Materializations []taskmaterialization.Record         `json:"materializations,omitempty"`
}

func ChildExecutionKey(taskID string) string {
	return "/v1/records/blueprint-component-executions/" + taskID
}

func (repository *Planner) GetBlueprintComponentExecution(ctx context.Context, taskID string) (keyvalue.Versioned[ChildExecution], error) {
	return ReadChildExecution(ctx, repository.store, taskID, 0)
}

func (publication Publication) ValidateChildExecution(taskID, parentID, planID, planHash string, generation uint64, configuration *taskconfiguration.TaskConfiguration, materializations []taskmaterialization.Record) error {
	for _, mutation := range publication.mutations {
		if mutation.Key != ChildExecutionKey(taskID) || mutation.Type != keyvalue.MutationPut {
			continue
		}
		record, err := recordcodec.Decode[ChildExecution](mutation.Value, "blueprint-component-execution")
		if err != nil {
			return err
		}
		plan, err := record.ExecutionPlan()
		if err != nil {
			return err
		}
		if record.TaskID == taskID && record.ParentTaskID == parentID && plan.GetPlanId() == planID &&
			hex.EncodeToString(plan.GetPlanHash()) == planHash && plan.GetRenderGeneration() == generation {
			return record.ValidateTaskInputs(configuration, materializations)
		}
		return errs.New(errs.KindValidationFailed, "Blueprint Component Task differs from its execution authority")
	}
	return errs.New(errs.KindValidationFailed, "Blueprint Component publication has no execution authority")
}

// ExecutionPlan validates the stored recipe, its ownership and the one-unit
// mutation boundary before either dispatch or terminal settlement consumes it.
func (record ChildExecution) ExecutionPlan() (*agentpb.ExecutionPlan, error) {
	if ids.Validate(ids.KindTask, record.TaskID) != nil ||
		ids.Validate(ids.KindTask, record.ParentTaskID) != nil || record.TaskID == record.ParentTaskID ||
		ids.Validate(ids.KindEnvironment, record.EnvironmentID) != nil ||
		componentrecord.ValidateRecord(record.Component) != nil ||
		record.Component.Desired.Owner != core.ComponentOwnerEnvironment ||
		record.Component.Desired.OwnerID != record.EnvironmentID || len(record.Plan) == 0 ||
		len(record.Plan) > executionplan.MaximumPlanBytes {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Component execution authority is invalid")
	}
	decoded := &agentpb.ExecutionPlan{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(record.Plan, decoded); err != nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Component execution recipe is unreadable")
	}
	plan, err := executionplan.Validate(decoded)
	if err != nil {
		return nil, err
	}
	if err := record.validateMaterializations(plan); err != nil {
		return nil, err
	}
	fileCount := 0
	for _, step := range plan.GetSteps() {
		if step.GetMaterializeFile() != nil {
			fileCount++
		}
	}
	if (fileCount != 0) != (record.Configuration != nil) {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Component file-source authority is incomplete")
	}
	if record.Configuration != nil {
		configuration := record.Configuration
		if runtimeconfiguration.ValidateReference(configuration.Current) != nil ||
			configuration.Current.EnvironmentID != record.EnvironmentID ||
			configuration.Current.Generation != plan.GetRenderGeneration() ||
			(configuration.Prior == nil) != (configuration.PriorRevision == 0) || configuration.PriorRevision < 0 ||
			configuration.SecretPins != nil || configuration.BackingHookInputs != nil {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Component captured configuration is invalid")
		}
		if configuration.Prior != nil && (runtimeconfiguration.ValidateReference(*configuration.Prior) != nil ||
			configuration.Prior.EnvironmentID != record.EnvironmentID || configuration.Prior.Generation > configuration.Current.Generation) {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Component configuration predecessor changed")
		}
	}
	if plan.GetOperation() != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY ||
		plan.GetTargetId() != record.EnvironmentID || plan.GetCandidateReleaseProcedure() != nil ||
		plan.GetEntryMutationProcedure() != nil || plan.GetServiceLifecycleProcedure() != nil {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Component recipe has unrelated runtime authority")
	}
	previousZone := ""
	for _, zone := range record.Zones {
		if zonerecord.ValidateRecord(zone) != nil || zone.EnvironmentID != record.EnvironmentID || zone.Desired.ID <= previousZone {
			return nil, errs.New(errs.KindStateConflict, "Blueprint Component Zone authority is invalid")
		}
		previousZone = zone.Desired.ID
	}
	artifacts := make(map[string]*agentpb.ComposeArtifact, len(plan.GetArtifacts()))
	for _, artifact := range plan.GetArtifacts() {
		artifacts[artifact.GetArtifactId()] = artifact
	}
	for _, step := range plan.GetSteps() {
		var artifactID string
		var services []string
		switch {
		case step.GetMaterializeFile() != nil:
			artifactID = step.GetMaterializeFile().GetArtifactId()
			if serviceID := step.GetMaterializeFile().GetServiceId(); serviceID != "" {
				services = []string{serviceID}
			}
		case step.GetComposeApply() != nil:
			apply := step.GetComposeApply()
			if !apply.GetNoDependencies() || apply.GetFullReconcile() || len(apply.GetServiceIds()) != 1 {
				return nil, errs.New(errs.KindStateConflict, "Blueprint Component Apply exceeds its unit")
			}
			artifactID, services = apply.GetArtifactId(), apply.GetServiceIds()
		case step.GetComposeRemove() != nil:
			remove := step.GetComposeRemove()
			if remove.GetWholeProject() || len(remove.GetServiceIds()) != 1 {
				return nil, errs.New(errs.KindStateConflict, "Blueprint Component removal exceeds its unit")
			}
			artifactID, services = remove.GetArtifactId(), remove.GetServiceIds()
		case step.GetWaitHealthy() != nil:
			artifactID, services = step.GetWaitHealthy().GetArtifactId(), step.GetWaitHealthy().GetServiceIds()
		case step.GetComponentApply() != nil:
			if step.GetComponentApply().GetComponentId() != record.Component.Desired.ID {
				return nil, errs.New(errs.KindStateConflict, "Blueprint Component action belongs to another unit")
			}
			continue
		default:
			return nil, errs.New(errs.KindStateConflict, "Blueprint Component recipe contains an unrelated operation")
		}
		for _, serviceID := range services {
			found := false
			for _, service := range artifacts[artifactID].GetServices() {
				if service.GetServiceId() == serviceID && service.GetOwnerComponentId() == record.Component.Desired.ID {
					found = true
				}
			}
			if !found {
				return nil, errs.New(errs.KindStateConflict, "Blueprint Component recipe targets an unowned Service")
			}
		}
	}
	return plan, nil
}

// Task Secret pins are activated during publication. They cannot alter the
// candidate or predecessor file-source references already sealed in this recipe.
func (record ChildExecution) ValidateTaskInputs(configuration *taskconfiguration.TaskConfiguration, materializations []taskmaterialization.Record) error {
	if !taskmaterialization.EqualRecords(record.Materializations, materializations) {
		return errs.New(errs.KindStateConflict, "Blueprint Component Task file sources changed")
	}
	sealed := record.Configuration
	if (sealed == nil) != (configuration == nil) {
		return errs.New(errs.KindStateConflict, "Blueprint Component Task configuration changed")
	}
	if sealed == nil {
		return nil
	}
	if sealed.Current != configuration.Current || sealed.PriorRevision != configuration.PriorRevision ||
		(sealed.Prior == nil) != (configuration.Prior == nil) ||
		sealed.Prior != nil && *sealed.Prior != *configuration.Prior || configuration.BackingHookInputs != nil {
		return errs.New(errs.KindStateConflict, "Blueprint Component Task file-source selection differs from its recipe")
	}
	return nil
}

func ReadChildExecution(ctx context.Context, store interface {
	GetMany(context.Context, keyvalue.GetManyRequest) (*keyvalue.GetManyResult, error)
}, taskID string, revision int64) (keyvalue.Versioned[ChildExecution], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[ChildExecution]{}, err
	}
	if store == nil || ids.Validate(ids.KindTask, taskID) != nil || revision < 0 {
		return keyvalue.Versioned[ChildExecution]{}, errs.New(errs.KindValidationFailed, "Blueprint Component execution lookup is invalid")
	}
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{ChildExecutionKey(taskID)}, Revision: revision})
	if err != nil {
		return keyvalue.Versioned[ChildExecution]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || revision > 0 && read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return keyvalue.Versioned[ChildExecution]{}, errs.New(errs.KindStateConflict, "Blueprint Component execution authority is unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	record, err := recordcodec.Decode[ChildExecution](read.Values[0].Value, "blueprint-component-execution")
	if err != nil {
		return keyvalue.Versioned[ChildExecution]{}, err
	}
	if record.TaskID != taskID {
		return keyvalue.Versioned[ChildExecution]{}, errs.New(errs.KindStateConflict, "Blueprint Component execution identity changed")
	}
	if _, err := record.ExecutionPlan(); err != nil {
		return keyvalue.Versioned[ChildExecution]{}, err
	}
	return keyvalue.Versioned[ChildExecution]{Record: record, Revision: read.Values[0].ModRevision, ReadRevision: read.ReadRevision}, nil
}

// WithChildExecution joins the recipe to the same CAS publication as the
// Component intent, address claims and hidden Task. It cannot be written later.
func (publication Publication) WithChildExecution(record ChildExecution, planID, planHash string) (Publication, error) {
	plan, err := record.ExecutionPlan()
	if err != nil {
		return Publication{}, err
	}
	if len(publication.preparation.Intent.Candidates) != 1 || publication.preparation.Intent.TaskID != record.TaskID ||
		publication.preparation.Intent.EnvironmentID != record.EnvironmentID ||
		!componentrecord.EqualRecord(publication.preparation.Intent.Candidates[0].Candidate, record.Component) ||
		plan.GetPlanId() != planID || hex.EncodeToString(plan.GetPlanHash()) != planHash {
		return Publication{}, errs.New(errs.KindValidationFailed, "Blueprint Component recipe differs from its publication")
	}
	value, err := recordcodec.Encode("blueprint-component-execution", record)
	if err != nil {
		return Publication{}, err
	}
	publication.conditions = append(publication.conditions, keyvalue.Condition{Key: ChildExecutionKey(record.TaskID)})
	publication.mutations = append(publication.mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ChildExecutionKey(record.TaskID), Value: value})
	publication.values = append(publication.values, value)
	return publication, nil
}
