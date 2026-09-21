package etcd

import (
	"context"
	"encoding/hex"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type platformComponentTaskChange struct {
	applies     bool
	conditions  []etcdstore.Condition
	mutations   []etcdstore.Mutation
	values      [][]byte
	promoted    *componentrecord.Record
	observation *ComponentObservationRecord
}

func (repository *TaskRepository) preparePlatformComponentTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	result *taskjournal.TaskResultRecord,
	revision int64,
) (platformComponentTaskChange, error) {
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceComponent ||
		ids.Validate(ids.KindComponent, task.Target) != nil {
		return platformComponentTaskChange{}, nil
	}
	stateKeys := []string{
		componentrecord.RecordKey(task.Target),
		platformComponentTaskRenderInputKey(task.PlanID),
		platformComponentTaskActiveKey(task.Target),
		componentObservationKey(task.Target),
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: stateKeys, Revision: revision})
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	if state == nil || len(state.Values) != 4 || state.Values[0] == nil || state.Values[1] == nil {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"platform Component Task state is incomplete",
		)
	}
	componentValue := state.Values[0]
	renderInputValue := state.Values[1]
	component, err := componentrecord.DecodeRecord(componentValue.Value)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	input, err := decodePlatformComponentTaskRenderInput(renderInputValue.Value)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	if component.Desired.Owner != core.ComponentOwnerPlatform || component.Desired.OwnerID != "" ||
		(input.TaskID != task.ID && task.RetryOf == "") ||
		input.PlanID != task.PlanID || input.ComponentID != task.Target || task.PlanHash != input.ExecutionPlanSHA256 ||
		task.Params[TaskPlatformComponentDesiredSHA256Param] != input.DesiredSHA256 {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"platform Component Task no longer matches its render input",
		)
	}
	if state.Values[2] == nil || state.Values[2].ModRevision <= 0 ||
		string(state.Values[2].Value) != task.ID {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict, "platform Component Task ownership changed",
		)
	}
	desiredSHA256, err := PlatformComponentDesiredDigest(component)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	if desiredSHA256 != input.DesiredSHA256 {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"platform Component desired state changed during reconciliation",
		)
	}

	change := platformComponentTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: componentrecord.RecordKey(task.Target), ModRevision: componentValue.ModRevision},
			{Key: platformComponentTaskRenderInputKey(task.PlanID), ModRevision: renderInputValue.ModRevision},
		},
	}
	change.conditions = append(change.conditions, etcdstore.Condition{
		Key: platformComponentTaskActiveKey(task.Target), ModRevision: state.Values[2].ModRevision,
	})
	if terminalStatus != taskjournal.TaskStatusCompleted {
		return change, nil
	}
	priorObservation := state.Values[3]
	priorModRevision := int64(0)
	var decodedPrior *ComponentObservationRecord
	if priorObservation != nil {
		priorModRevision = priorObservation.ModRevision
		decoded, err := decodeComponentObservation(priorObservation.Value)
		if err != nil {
			return platformComponentTaskChange{}, err
		}
		decodedPrior = &decoded
		if input.PredecessorTaskID != "" && decoded.TaskID != input.PredecessorTaskID {
			return platformComponentTaskChange{}, errs.New(
				errs.KindStateConflict, "platform Component prior observation ownership changed",
			)
		}
	}
	if input.PriorObservationModRevision != 0 && priorModRevision != input.PriorObservationModRevision ||
		input.PredecessorTaskID != "" && priorObservation == nil ||
		decodedPrior != nil && (decodedPrior.Revision != input.PriorObservationRevision ||
			decodedPrior.CorefileSHA256 != input.ExpectedPreviousArtifactSHA256) {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"platform Component prior observation changed during reconciliation",
		)
	}
	if result == nil {
		return platformComponentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"completed platform Component Task result is missing",
		)
	}
	if err := validatePlatformComponentObservation(input, uint64(task.RenderGeneration), *result); err != nil {
		return platformComponentTaskChange{}, err
	}
	observation, err := newPlatformComponentObservation(component, componentValue.ModRevision, input, task, *result)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	observationValue, err := encodeComponentObservation(observation)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	change.conditions = append(change.conditions, etcdstore.Condition{
		Key: componentObservationKey(task.Target), ModRevision: priorModRevision,
	})
	change.values = append(change.values, observationValue)
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type: etcdstore.MutationPut, Key: componentObservationKey(task.Target), Value: observationValue,
	})
	change.observation = &observation
	generatedServices := []string(nil)
	if component.Desired.Enabled {
		generatedServices = []string{input.GeneratedServiceID}
	}
	promoted, err := componentrecord.SetRuntime(
		component,
		generatedServices,
		"",
		component.Desired.Enabled,
	)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	value, err := componentrecord.EncodeRecord(promoted)
	if err != nil {
		return platformComponentTaskChange{}, err
	}
	change.values = append(change.values, value)
	change.mutations = append(change.mutations, etcdstore.Mutation{
		Type:  etcdstore.MutationPut,
		Key:   componentrecord.RecordKey(task.Target),
		Value: value,
	}, componentrecord.WriteFenceMutation(task.Target))
	change.promoted = &promoted
	return change, nil
}

func newPlatformComponentObservation(
	component componentrecord.Record,
	desiredRevision int64,
	input PlatformComponentTaskRenderInput,
	task TaskRecord,
	result taskjournal.TaskResultRecord,
) (ComponentObservationRecord, error) {
	if desiredRevision <= 0 || task.TerminalAssignment == nil || task.FinishedAt == nil || len(task.Steps) == 0 {
		return ComponentObservationRecord{}, errs.New(
			errs.KindStateConflict,
			"completed platform Component Task assignment evidence is missing",
		)
	}
	observedAt := *task.FinishedAt
	if !input.DisableService {
		observedAt = result.DNSResolverCandidateObservation.ObservedAt
	}
	record := ComponentObservationRecord{
		ComponentID: input.ComponentID, ServiceID: input.GeneratedServiceID,
		PlanID: input.OwnershipPlanID, ComposeArtifactID: input.ComposeArtifactID,
		Enabled: component.Desired.Enabled, Healthy: component.Desired.Enabled,
		DesiredGeneration: uint64(desiredRevision), RenderGeneration: uint64(task.RenderGeneration),
		AgentID: task.TerminalAssignment.AgentID, AgentGeneration: task.TerminalAssignment.AgentGeneration,
		BaselineGeneration: input.BaselineGeneration, OwnershipGeneration: input.OwnershipGeneration,
		ObservedAt: observedAt, TaskID: task.ID, StepID: task.Steps[len(task.Steps)-1].ID,
		Revision:          input.PriorObservationRevision + 1,
		PredecessorTaskID: input.PredecessorTaskID,
	}
	if component.Desired.Enabled {
		proof := *result.DNSResolverCandidateObservation
		proof.CanonicalEvidence = append([]byte(nil), proof.CanonicalEvidence...)
		record.DNSResolverProof = &proof
		record.CorefileSHA256 = proof.ArtifactSHA256
		record.InputSHA256 = input.HostResolutionSHA256
		if input.ComposeArtifact != nil {
			record.ComposeArtifact = proto.Clone(input.ComposeArtifact).(*agentpb.ComposeArtifact)
		}
	} else {
		record.CorefileSHA256 = input.ExpectedPreviousArtifactSHA256
	}
	if err := validateComponentObservation(record); err != nil {
		return ComponentObservationRecord{}, err
	}
	return record, nil
}

func validatePlatformComponentObservation(
	input PlatformComponentTaskRenderInput,
	renderGeneration uint64,
	result taskjournal.TaskResultRecord,
) error {
	if input.DisableService {
		if result.DNSResolverCandidateObservation != nil {
			return errs.New(errs.KindStateConflict, "disabled platform Component carries an observation proof")
		}
		return nil
	}
	if result.Kind != taskjournal.TaskResultCompose || result.DNSResolverCandidateObservation == nil {
		return errs.New(errs.KindStateConflict, "platform Component observation is missing")
	}
	evidence := result.DNSResolverCandidateObservation
	canonical, err := dnsproof.Unmarshal(evidence.CanonicalEvidence)
	if err != nil {
		return errs.New(errs.KindStateConflict, "platform Component observation proof is corrupt")
	}
	if evidence.ComponentID != input.ComponentID || evidence.ServiceID != input.GeneratedServiceID ||
		evidence.ArtifactID != input.ArtifactID || evidence.ArtifactSHA256 != input.ArtifactSHA256 ||
		evidence.ImageConfigDigest != input.ImageConfigDigest ||
		evidence.RenderGeneration != renderGeneration || !evidence.RecursiveQuerySucceeded ||
		evidence.ForwarderSuccessCount != evidence.ForwarderQueryCount || evidence.ObservedAt.IsZero() ||
		canonical.GetComponentId() != input.ComponentID || canonical.GetArtifactId() != input.ArtifactID ||
		hex.EncodeToString(canonical.GetImageConfigDigest()) != input.ImageConfigDigest {
		return errs.New(
			errs.KindStateConflict,
			"platform Component full observation proof does not match its candidate",
		)
	}
	return nil
}

func (repository *TaskRepository) validatePlatformComponentTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) error {
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceComponent ||
		ids.Validate(ids.KindComponent, task.Target) != nil {
		return nil
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			platformComponentTaskRenderInputKey(task.PlanID),
			platformComponentTaskActiveKey(task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 2 || state.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "platform Component Task render input is missing")
	}
	input, err := decodePlatformComponentTaskRenderInput(state.Values[0].Value)
	if err != nil {
		return err
	}
	if (input.TaskID != task.ID && task.RetryOf == "") || input.PlanID != task.PlanID ||
		input.ComponentID != task.Target || task.PlanHash != input.ExecutionPlanSHA256 ||
		input.DesiredSHA256 != task.Params[TaskPlatformComponentDesiredSHA256Param] {
		return errs.New(errs.KindStateConflict, "platform Component Task replay evidence changed")
	}
	if task.Status == taskjournal.TaskStatusCompleted {
		if task.Result == nil {
			return errs.New(errs.KindStateConflict, "platform Component immutable Task result is missing")
		}
		if err := validatePlatformComponentObservation(input, uint64(task.RenderGeneration), *task.Result); err != nil {
			return err
		}
	}
	if state.Values[1] != nil && string(state.Values[1].Value) == task.ID {
		return errs.New(errs.KindStateConflict, "terminal platform Component Task retained its active fence")
	}
	return nil
}

func clearPlatformComponentTaskChange(change platformComponentTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}
