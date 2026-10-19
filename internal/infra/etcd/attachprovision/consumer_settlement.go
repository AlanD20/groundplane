package attachprovision

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintattachinputs"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func ConsumerSettlementKey(workID string) string {
	return "/v1/attach-provision-consumer-results/" + workID
}

// A successful network checkpoint and its applied-runtime record commit
// together. Retry may reuse this work's result, never overwrite a newer Deploy.
func (repository *InputRepository) prepareConsumerCheckpoint(
	ctx context.Context,
	view workqueue.ResumeView,
	capture blueprintattachinputs.Generation,
	input attachexecution.ProvisionInput,
	report attachexecution.ProvisionReport,
) (workqueue.Change, error) {
	consumer, err := capture.Consumer()
	if err != nil {
		return workqueue.Change{}, err
	}
	if consumer == nil {
		return workqueue.Change{}, nil
	}
	if !proto.Equal(consumer, input.Provision.Consumer) || capture.ConsumerRuntime == nil {
		return workqueue.Change{}, checkpointConflict("Attach connection changed after capture")
	}
	authority, err := capture.ProvisionAuthority()
	if err != nil {
		return workqueue.Change{}, err
	}
	progress := report.Progress
	confirms := !progress.InFlight && progress.LastResult != nil &&
		authority.IsConsumerCommand(progress.LastResult.Ordinal)
	if !progress.InFlight && !confirms {
		return workqueue.Change{}, nil
	}
	var source *serviceruntimerecord.Acknowledgement
	if confirms {
		checkpoint, err := report.CheckpointAuthority(input.Attempt, authority)
		if err != nil {
			return workqueue.Change{}, err
		}
		source = &serviceruntimerecord.Acknowledgement{
			TaskID:           input.Attempt.TaskID,
			WorkItemID:       input.Attempt.WorkItemID,
			AttemptID:        input.Attempt.ID,
			Executor:         input.Attempt.Executor,
			Input:            input.Attempt.Input,
			Checkpoint:       checkpoint,
			RenderGeneration: capture.ConsumerRuntime.Source.RenderGeneration,
			AcknowledgedAt:   progress.UpdatedAt,
		}
	}
	return repository.prepareConsumerAcknowledgement(ctx, view, capture, consumer.Candidate, source)
}

func (repository *InputRepository) prepareConsumerAcknowledgement(
	ctx context.Context,
	view workqueue.ResumeView,
	capture blueprintattachinputs.Generation,
	candidate *agentpb.ComposeArtifact,
	acknowledgement *serviceruntimerecord.Acknowledgement,
) (workqueue.Change, error) {
	if capture.ConsumerRuntime == nil || candidate == nil {
		return workqueue.Change{}, checkpointConflict("Attach consumer settlement has no captured runtime")
	}
	if acknowledgement != nil &&
		(acknowledgement.TaskID != view.Attempt.Attempt.TaskID || acknowledgement.WorkItemID != view.Attempt.Attempt.WorkItemID || acknowledgement.AttemptID != view.Attempt.Attempt.ID || acknowledgement.Executor != view.Attempt.Attempt.Executor || acknowledgement.Input != view.Attempt.Attempt.Input) {
		return workqueue.Change{}, checkpointConflict("Attach runtime acknowledgement belongs to another execution")
	}
	keys := []string{ConsumerSettlementKey(view.Work.Item.Item.ID), serviceruntimerecord.Key(capture.ConsumerServiceID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: view.Work.ReadRevision})
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != view.Work.ReadRevision || len(read.Values) != 2 || read.Values[1] == nil {
		return workqueue.Change{}, checkpointConflict("Attach consumer runtime is unavailable")
	}
	guards := make([]keyvalue.Condition, 0, 2)
	for index, value := range read.Values {
		if value != nil &&
			(value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > view.Work.ReadRevision) {
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
		guards = append(guards, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	expected := *capture.ConsumerRuntime
	if read.Values[0] != nil {
		source, err := recordcodec.Decode[serviceruntimerecord.Acknowledgement](
			read.Values[0].Value,
			"attach-consumer-settlement",
		)
		if err != nil || source.TaskID != view.Attempt.Attempt.TaskID ||
			source.WorkItemID != view.Attempt.Attempt.WorkItemID ||
			source.Input != view.Attempt.Attempt.Input ||
			source.Executor.HostID != view.Attempt.Attempt.Executor.HostID {
			return workqueue.Change{}, checkpointConflict("Attach consumer settlement belongs to different execution")
		}
		expected, err = acknowledgedConsumer(capture, candidate, source)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer clear(expected.Runtime.CurrentArtifact)
		if acknowledgement != nil && source.AttemptID == view.Attempt.Attempt.ID {
			if !sameCheckpoint(source.Checkpoint, acknowledgement.Checkpoint) {
				return workqueue.Change{}, checkpointConflict("Attach connection result changed after acknowledgement")
			}
		}
	} else if read.Values[1].ModRevision != capture.ConsumerRuntimeRevision {
		return workqueue.Change{}, checkpointConflict("Service runtime changed after the Attach captured its network transition")
	}
	raw, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", expected)
	if err != nil {
		return workqueue.Change{}, err
	}
	same := bytes.Equal(raw, read.Values[1].Value)
	clear(raw)
	if !same {
		return workqueue.Change{}, checkpointConflict("Attach cannot overwrite a changed Service runtime")
	}
	if acknowledgement == nil || read.Values[0] != nil {
		return workqueue.Change{Conditions: guards}, nil
	}
	next, err := acknowledgedConsumer(capture, candidate, *acknowledgement)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(next.Runtime.CurrentArtifact)
	value, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", next)
	if err != nil {
		return workqueue.Change{}, err
	}
	receipt, err := recordcodec.Encode("attach-consumer-settlement", *acknowledgement)
	if err != nil {
		clear(value)
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{
		{
			Type:  keyvalue.MutationPut,
			Key:   keys[1],
			Value: value,
		}, {Type: keyvalue.MutationPut, Key: keys[0], Value: receipt},
	}}, nil
}

func acknowledgedConsumer(
	capture blueprintattachinputs.Generation,
	candidate *agentpb.ComposeArtifact,
	source serviceruntimerecord.Acknowledgement,
) (serviceruntimerecord.Record, error) {
	previous := *capture.ConsumerRuntime
	updated := previous.Runtime
	artifact, err := (proto.MarshalOptions{Deterministic: true}).Marshal(candidate)
	if err != nil {
		return serviceruntimerecord.Record{}, err
	}
	updated.CurrentArtifact = artifact
	next, err := serviceruntimerecord.AcknowledgeAttach(previous, serviceruntimerecord.AttachPreparation{
		EnvironmentID: previous.EnvironmentID, WorkItemID: source.WorkItemID, Input: source.Input, StageID: source.Checkpoint.StageID,
		RenderGeneration: source.RenderGeneration, Updates: []serviceruntimerecord.AttachUpdate{{PreviousRevision: capture.ConsumerRuntimeRevision, Runtime: updated}},
	}, source)
	if err != nil {
		clear(artifact)
		return serviceruntimerecord.Record{}, err
	}
	return next, nil
}
