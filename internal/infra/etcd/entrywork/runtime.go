package entrywork

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ConsumerConditions prevents publication, assignment or completion against a
// replaced serving runtime. The admission owner also fences selection of the
// complete running-consumer set with the Environment mutation epoch.
func (repository *InputRepository) ConsumerConditions(ctx context.Context, capture Capture, revision int64) ([]keyvalue.Condition, error) {
	if len(capture.Consumers) == 0 {
		return nil, nil
	}
	keys := make([]string, len(capture.Consumers))
	for index, consumer := range capture.Consumers {
		keys[index] = serviceruntimerecord.Key(consumer.Previous.Runtime.ServiceID)
	}
	read, err := repository.read(ctx, keys, revision)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, len(keys))
	for index, consumer := range capture.Consumers {
		value := read.Values[index]
		encoded, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", consumer.Previous)
		if err != nil {
			return nil, err
		}
		matches := value != nil && value.ModRevision == consumer.Revision && bytes.Equal(value.Value, encoded)
		clear(encoded)
		if !matches {
			return nil, conflict("Entry consumer runtime changed after capture")
		}
		conditions[index] = keyvalue.Condition{Key: keys[index], ModRevision: consumer.Revision}
	}
	return conditions, nil
}

// PrepareAppliedState returns one atomic settlement fragment. It changes only
// acknowledged file/runtime state, never a Release or latest desired definition.
// The caller joins queue completion and any Entry removal settlement.
func (repository *InputRepository) PrepareAppliedState(ctx context.Context, view workqueue.ResumeView,
	input *agentpb.EntryConfigurationWorkInput, at time.Time,
) (workqueue.Change, error) {
	captured, capture, guards, err := repository.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !proto.Equal(input, captured) {
		return workqueue.Change{}, conflict("Entry completion differs from captured input")
	}
	proof, err := PrepareCompletionProof(ctx, repository.store, view, captured, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	keep := false
	defer func() {
		if !keep {
			keyvalue.ClearMutationValues(proof.Mutations)
		}
	}()
	proof.Conditions = append(proof.Conditions, guards...)
	consumers, err := repository.ConsumerConditions(ctx, capture, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	proof.Conditions = append(proof.Conditions, consumers...)
	removal, err := repository.prepareRemoval(ctx, view.Work, captured, capture)
	if err != nil {
		return workqueue.Change{}, err
	}
	proof.Conditions = append(proof.Conditions, removal.Conditions...)
	proof.Mutations = append(proof.Mutations, removal.Mutations...)
	sources, err := runtimeconfiguration.New(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	configuration, err := sources.WorkConditions(ctx, capture.Configuration, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	for _, condition := range configuration {
		proof.Conditions = append(proof.Conditions, keyvalue.Condition{Key: condition.Key, ModRevision: condition.ModRevision})
	}
	if capture.Configuration.Prior == nil || capture.Configuration.Current != *capture.Configuration.Prior {
		value, err := runtimeconfiguration.EncodeReference(capture.Configuration.Current)
		if err != nil {
			return workqueue.Change{}, err
		}
		proof.Mutations = append(proof.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: runtimeconfiguration.HeadKey(input.EnvironmentId), Value: value})
	}
	for _, consumer := range capture.Consumers {
		runtime, err := executionplan.PrepareEntryRuntime(captured.Candidate, consumer.Previous.Runtime,
			consumer.CurrentArtifactID, consumer.RetainedArtifactID)
		if err != nil {
			return workqueue.Change{}, err
		}
		attempt := view.Attempt.Attempt
		record := serviceruntimerecord.Record{EnvironmentID: input.EnvironmentId, Runtime: runtime,
			Configuration: &capture.Configuration.Current, Source: serviceruntimerecord.Acknowledgement{
				TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, AttemptID: attempt.ID,
				Executor: attempt.Executor, Input: attempt.Input, Checkpoint: *view.Checkpoint.Record,
				RenderGeneration: input.RenderGeneration, AcknowledgedAt: at}}
		if err := serviceruntimerecord.Validate(record); err != nil {
			return workqueue.Change{}, err
		}
		value, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", record)
		if err != nil {
			return workqueue.Change{}, err
		}
		proof.Mutations = append(proof.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: serviceruntimerecord.Key(runtime.ServiceID), Value: value})
	}
	keep = true
	return proof, nil
}
