package attachremoval

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

type consumerSettlement struct {
	Source          serviceruntimerecord.Acknowledgement `json:"source"`
	CandidateSHA256 string                               `json:"candidate_sha256"`
}

func ConsumerSettlementKey(workID string) string {
	return DetachInputPrefix(workID) + "consumer-settlement"
}

// PrepareDetachCheckpoint saves command evidence and the applied network
// artifact in one commit. An Agent cannot proceed to credential/hook cleanup
// until this combined change is acknowledged. Later cleanup failure therefore
// cannot leave GP advertising the consumer's removed network membership.
func (repository *InputRepository) PrepareDetachCheckpoint(ctx context.Context, view workqueue.ResumeView,
	input attachexecution.RemovalInput, report attachexecution.RemovalReport,
) (workqueue.Change, error) {
	capture, err := repository.LoadDetachCapture(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer capture.Clear()
	if err := validateDetachDeliverySource(ctx, capture, view, input); err != nil {
		return workqueue.Change{}, err
	}
	change, err := PrepareRemovalCheckpoint(ctx, repository.store, view, input, report)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, capture.Conditions...)
	result := report.Progress.LastResult
	if report.Progress.InFlight || result == nil || result.Consumer == nil {
		return change, nil
	}
	settlement, err := repository.prepareConsumerSettlement(ctx, view, capture.Input, input, report)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, settlement.Conditions...)
	change.Mutations = append(change.Mutations, settlement.Mutations...)
	return change, nil
}

func validateDetachDeliverySource(ctx context.Context, capture DetachCapture, view workqueue.ResumeView,
	input attachexecution.RemovalInput,
) error {
	if capture.Revision <= 0 || input.Validate() != nil || view.Work.Item.Item.Input == nil ||
		input.Attempt.Input != *view.Work.Item.Item.Input || capture.Capture.Action != input.Attempt.Input {
		return recordcodec.CorruptRecord()
	}
	raw, err := EncodeWorkCapture(capture.Capture)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(raw)
	clear(raw)
	source, target := input.Removal.CapturedSource, capture.Input.Snapshot.Source
	if source.InputId != ids.DeriveAt(ids.KindPlan, view.Work.Task.Task.CreatedAt, view.Work.Item.Item.ID, "attach-removal-source") ||
		!bytes.Equal(source.Sha256, digest[:]) || input.Removal.AttachId != target.Attach.ID ||
		input.Removal.BackingServiceId != target.Attach.BackingServiceID || input.Removal.TenantId != target.TenantID ||
		input.Removal.ProjectId != target.ProjectID || input.Removal.EnvironmentId != target.Attach.EnvironmentID ||
		input.Removal.ServiceId != target.Attach.ServiceID {
		return recordcodec.CorruptRecord()
	}
	consumer, err := capture.Input.ConsumerTransition(ctx)
	if err != nil {
		return err
	}
	defer clearConsumer(consumer)
	if !proto.Equal(consumer, input.Removal.Consumer) {
		return errs.New(errs.KindStateConflict, "Detach report consumer differs from the admitted runtime transition")
	}
	return nil
}

func (repository *InputRepository) prepareConsumerSettlement(ctx context.Context, view workqueue.ResumeView,
	captured DetachInput, input attachexecution.RemovalInput, report attachexecution.RemovalReport,
) (workqueue.Change, error) {
	checkpoint, err := report.Checkpoint(input)
	if err != nil {
		return workqueue.Change{}, err
	}
	ordinal, exists := input.ConsumerCommandOrdinal()
	result := report.Progress.LastResult
	if !exists || result == nil || result.Consumer == nil || result.Ordinal != ordinal ||
		report.Progress.InFlight || report.Progress.Completed != ordinal+1 || captured.Snapshot.Runtime == nil {
		return workqueue.Change{}, recordcodec.CorruptRecord()
	}
	previous := *captured.Snapshot.Runtime
	updatedRuntime := previous.Runtime
	updatedRuntime.CurrentArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(input.Removal.Consumer.Candidate)
	if err != nil {
		return workqueue.Change{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(updatedRuntime.CurrentArtifact)
	source := serviceruntimerecord.Acknowledgement{TaskID: input.Attempt.TaskID, WorkItemID: input.Attempt.WorkItemID,
		AttemptID: input.Attempt.ID, Executor: input.Attempt.Executor, Input: input.Attempt.Input, Checkpoint: checkpoint,
		RenderGeneration: previous.Source.RenderGeneration, AcknowledgedAt: report.Progress.UpdatedAt}
	next, err := serviceruntimerecord.AcknowledgeAttach(previous, serviceruntimerecord.AttachPreparation{
		EnvironmentID: previous.EnvironmentID, WorkItemID: input.Attempt.WorkItemID, Input: input.Attempt.Input,
		StageID: checkpoint.StageID, RenderGeneration: source.RenderGeneration,
		Updates: []serviceruntimerecord.AttachUpdate{{PreviousRevision: captured.Snapshot.RuntimeRevision, Runtime: updatedRuntime}},
	}, source)
	if err != nil {
		return workqueue.Change{}, err
	}
	key, runtimeKey := ConsumerSettlementKey(input.Attempt.WorkItemID), serviceruntimerecord.Key(previous.Runtime.ServiceID)
	read, err := repository.read(ctx, []string{key, runtimeKey}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	digest := hex.EncodeToString(input.Removal.Consumer.Candidate.YamlSha256)
	if receipt := read.Values[0]; receipt != nil {
		saved, err := recordcodec.Decode[consumerSettlement](receipt.Value, "detach-consumer-settlement")
		if err != nil || saved.CandidateSHA256 != digest || saved.Source.TaskID != input.Attempt.TaskID ||
			saved.Source.WorkItemID != input.Attempt.WorkItemID || saved.Source.Input != input.Attempt.Input ||
			saved.Source.Executor.HostID != input.Attempt.Executor.HostID {
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
		next.Source = saved.Source
		if serviceruntimerecord.Validate(next) != nil {
			return workqueue.Change{}, recordcodec.CorruptRecord()
		}
		change := workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: receipt.ModRevision}}}
		if saved.Source.AttemptID == input.Attempt.ID {
			if !sameCheckpoint(saved.Source.Checkpoint, checkpoint) {
				return workqueue.Change{}, recordcodec.CorruptRecord()
			}
			return change, nil
		}
		// A later explicit Retry may observe the same completed effect. It can
		// reuse that proof, but cannot overwrite a newer independent runtime.
		current := read.Values[1]
		if current == nil {
			return workqueue.Change{}, checkpointConflict("Detach Retry lost its settled consumer runtime")
		}
		raw, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", next)
		if err != nil {
			return workqueue.Change{}, err
		}
		same := bytes.Equal(raw, current.Value)
		clear(raw)
		if !same {
			return workqueue.Change{}, checkpointConflict("Detach Retry cannot replace a changed consumer runtime")
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: runtimeKey, ModRevision: current.ModRevision})
		return change, nil
	}
	current := read.Values[1]
	if current == nil || current.ModRevision != captured.Snapshot.RuntimeRevision {
		return workqueue.Change{}, checkpointConflict("Detach consumer runtime changed before its network result could be saved")
	}
	raw, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", next)
	if err != nil {
		return workqueue.Change{}, err
	}
	receipt, err := recordcodec.Encode("detach-consumer-settlement", consumerSettlement{Source: source, CandidateSHA256: digest})
	if err != nil {
		clear(raw)
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}, {Key: runtimeKey, ModRevision: current.ModRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: runtimeKey, Value: raw},
			{Type: keyvalue.MutationPut, Key: key, Value: receipt}}}, nil
}
