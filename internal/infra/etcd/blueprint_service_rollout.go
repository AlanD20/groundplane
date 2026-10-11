package etcd

import (
	"bytes"
	"context"
	"encoding/hex"
	"maps"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// A staged rollout is inert until the child publisher compares this exact
// record and publishes its Task, source references and Release marker together.
// The immutable desired revision owns abandoned captures and their cleanup.
func BlueprintServiceRolloutKey(environmentID, parentID, taskID string) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(environmentID, parentID) + "service-rollouts/" + taskID
}

func (ledger *ReleaseLedger) StageBlueprintServiceRollout(ctx context.Context,
	capture releaserender.BlueprintServiceRolloutCapture,
) (keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	if ledger == nil || ledger.store == nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindInternal,
			"Blueprint rollout storage is not configured",
		)
	}
	value, err := releaserender.EncodeBlueprintServiceRolloutCapture(capture)
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	defer clear(value)
	key := BlueprintServiceRolloutKey(
		capture.Owner.EnvironmentID,
		capture.Params[taskjournal.TaskBlueprintParentParam],
		capture.TaskID,
	)
	result, err := ledger.store.Transact(ctx, []keyvalue.Condition{{Key: key}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}})
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	defer keyvalue.ClearValues(result.FailureReads)
	revision := result.Revision
	if !result.Succeeded {
		if len(result.FailureReads) != 1 || result.FailureReads[0] == nil ||
			result.FailureReads[0].Key != key || !bytes.Equal(result.FailureReads[0].Value, value) {
			return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
				errs.KindStateConflict,
				"Blueprint Service rollout identity already has different prepared work",
			)
		}
		revision = result.FailureReads[0].ModRevision
	}
	// Decode the owned bytes rather than retaining the producer's mutable maps.
	owned, err := releaserender.DecodeBlueprintServiceRolloutCapture(value)
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{
		Record:       owned,
		Revision:     revision,
		ReadRevision: result.Revision,
	}, nil
}

func (ledger *ReleaseLedger) GetBlueprintServiceRollout(ctx context.Context,
	task TaskRecord,
) (keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture], error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	parentID := task.Params[taskjournal.TaskBlueprintParentParam]
	if ledger == nil || ledger.store == nil || ids.Validate(ids.KindTask, task.ID) != nil ||
		ids.Validate(
			ids.KindTask,
			parentID,
		) != nil || ids.Validate(ids.KindEnvironment, task.Owner.EnvironmentID) != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindValidationFailed,
			"Blueprint rollout read identity is invalid",
		)
	}
	key := BlueprintServiceRolloutKey(task.Owner.EnvironmentID, parentID, task.ID)
	read, err := ledger.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}})
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 || read.Values[0] == nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Service has no captured rollout; its native work cannot be reconstructed",
		)
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0].Key != key {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, errs.New(
			errs.KindInternal,
			"Blueprint rollout storage identity is invalid",
		)
	}
	capture, err := releaserender.DecodeBlueprintServiceRolloutCapture(read.Values[0].Value)
	if err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	if err := validateBlueprintServiceRolloutTask(capture, task); err != nil {
		return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{}, err
	}
	return keyvalue.Versioned[releaserender.BlueprintServiceRolloutCapture]{
		Record:       capture,
		Revision:     read.Values[0].ModRevision,
		ReadRevision: read.ReadRevision,
	}, nil
}

func validateBlueprintServiceRolloutTask(capture releaserender.BlueprintServiceRolloutCapture, task TaskRecord) error {
	plan, err := releaserender.OpenBlueprintServiceRolloutCapture(capture)
	if err != nil {
		return err
	}
	if capture.TaskID != task.ID || capture.OperationID != task.OperationID || capture.Owner != task.Owner ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > capture.TimeoutSeconds || !maps.Equal(capture.Params, task.Params) ||
		plan.PlanId != task.PlanID || plan.TargetId != task.Target || plan.RenderGeneration != uint64(task.RenderGeneration) ||
		task.Actor != taskjournal.TaskActorSystem || task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskUpdate ||
		len(task.Steps) < len(capture.Steps) {
		return errs.New(errs.KindStateConflict, "Blueprint rollout belongs to another Task or execution input")
	}
	for index, step := range capture.Steps {
		actual := task.Steps[index]
		if actual.ID != step.ID || actual.Kind != step.Kind || actual.ScriptID != step.ScriptID ||
			actual.ScriptSlug != step.ScriptSlug {
			return errs.New(errs.KindStateConflict, "Blueprint rollout Script or step identity changed")
		}
	}
	return nil
}

func blueprintServiceRolloutPublicationCondition(
	evidence BlueprintReleasePublicationEvidence,
) (*keyvalue.Condition, error) {
	child := taskjournal.IsBlueprintChild(evidence.Task.Params)
	if !child {
		if evidence.ServiceRollout != nil {
			return nil, errs.New(
				errs.KindValidationFailed,
				"aggregate Blueprint cannot publish a Service child rollout capture",
			)
		}
		return nil, nil
	}
	stored := evidence.ServiceRollout
	if stored == nil || stored.Revision <= 0 || stored.ReadRevision < stored.Revision ||
		len(evidence.Manifest.Record.Members) != 1 {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Service publication has no persisted native rollout")
	}
	if err := validateBlueprintServiceRolloutPlan(stored.Record, evidence.Task, evidence.Plan); err != nil {
		return nil, err
	}
	native, err := releaserender.OpenBlueprintServiceRolloutCapture(stored.Record)
	if err != nil {
		return nil, err
	}
	if native.CandidateReleaseProcedure.Members[0].ServiceId != evidence.Manifest.Record.Members[0].ServiceID {
		return nil, errs.New(errs.KindStateConflict, "Blueprint rollout capture belongs to another Service")
	}
	return &keyvalue.Condition{Key: BlueprintServiceRolloutKey(
		evidence.EnvironmentID,
		evidence.Task.Params[taskjournal.TaskBlueprintParentParam],
		evidence.Task.ID,
	), ModRevision: stored.Revision}, nil
}

// Final sealing may append only pinned-file recovery. Every native effect and
// Script snapshot must be identical to the persisted preparation.
func validateBlueprintServiceRolloutPlan(capture releaserender.BlueprintServiceRolloutCapture,
	task TaskRecord, final *agentpb.ExecutionPlan,
) error {
	if err := validateBlueprintServiceRolloutTask(capture, task); err != nil {
		return err
	}
	native, err := releaserender.OpenBlueprintServiceRolloutCapture(capture)
	if err != nil {
		return err
	}
	if final == nil || final.GetCandidateReleaseProcedure() == nil || len(final.Steps) < len(native.Steps) ||
		hex.EncodeToString(final.PlanHash) != task.PlanHash {
		return errs.New(errs.KindStateConflict, "Blueprint final rollout differs from its Task")
	}
	if _, err := executionplan.Validate(final); err != nil {
		return err
	}
	files := final.CandidateReleaseProcedure.GetConfigurationRestoration().GetFiles()
	if len(final.Steps)-len(native.Steps) != len(files)*2 {
		return errs.New(errs.KindStateConflict, "Blueprint final rollout has unrelated extra steps")
	}
	for index, file := range files {
		probe, compensate := final.Steps[len(native.Steps)+index*2], final.Steps[len(native.Steps)+index*2+1]
		if probe.StepId != file.ProbeStepId || compensate.StepId != file.CompensateStepId ||
			probe.GetMaterializeFile() == nil || compensate.GetMaterializeFile() == nil {
			return errs.New(errs.KindStateConflict, "Blueprint final rollout file recovery order changed")
		}
	}
	actual := proto.CloneOf(final)
	actual.CandidateReleaseProcedure.ConfigurationRestoration = nil
	actual.Steps = actual.Steps[:len(native.Steps)]
	actual.PlanHash, native.PlanHash = nil, nil
	if !proto.Equal(actual, native) {
		return errs.New(errs.KindStateConflict, "Blueprint native rollout changed after capture")
	}
	return nil
}
