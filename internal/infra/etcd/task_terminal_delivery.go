package etcd

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type terminalDeliveryAuthority struct {
	conditions []etcdstore.Condition
	delivery   taskjournal.TaskTerminalDeliveryRecord
	rebind     bool
	report     *agentpb.TaskAck
}

func (repository *TaskRepository) readTerminalDeliveryAuthority(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	receipt *agentpb.TaskTerminalReceiptAck,
	allowProcessRebind bool,
	report *agentpb.TaskAck,
) (terminalDeliveryAuthority, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return terminalDeliveryAuthority{}, err
	}
	if err := executionplan.ValidateTaskTerminalReceiptAck(receipt); err != nil {
		return terminalDeliveryAuthority{}, err
	}
	keys := []string{
		taskjournal.TaskStorageKey(receipt.TaskId),
		taskjournal.TaskTerminalReceiptKey(receipt.TaskId),
		taskjournal.TaskTerminalDeliveryKey(receipt.TaskId),
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return terminalDeliveryAuthority{}, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) || read.Values[0] == nil ||
		read.Values[1] == nil ||
		read.Values[0].ModRevision != receipt.DurableTaskModRevision ||
		read.Values[1].ModRevision != receipt.DurableTaskModRevision ||
		read.Values[1].Version != 1 {
		return terminalDeliveryAuthority{}, terminalDeliveryConflict()
	}
	defer etcdstore.ClearValues(read.Values)
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return terminalDeliveryAuthority{}, err
	}
	stored, err := taskjournal.DecodeTaskTerminalReceipt(read.Values[1].Value)
	if err != nil {
		return terminalDeliveryAuthority{}, err
	}
	expected, err := taskTerminalDeliveryReceipt(task)
	if err != nil || stored != expected || stored.AgentID != agentID || stored.AgentGeneration != agentGeneration ||
		stored.TaskID != receipt.TaskId || stored.AssignmentID != receipt.AssignmentId || stored.AssignmentGeneration != receipt.AssignmentGeneration ||
		stored.PlanHash != hex.EncodeToString(
			receipt.PlanHash,
		) || taskjournal.TaskTerminalWire(stored.Terminal) != receipt.Terminal {
		return terminalDeliveryAuthority{}, terminalDeliveryConflict()
	}
	digest, err := taskjournal.TaskTerminalReceiptSHA256(stored)
	if err != nil || digest != hex.EncodeToString(receipt.TerminalReceiptSha256) {
		return terminalDeliveryAuthority{}, terminalDeliveryConflict()
	}
	result := terminalDeliveryAuthority{conditions: make([]etcdstore.Condition, len(keys))}
	for index, key := range keys {
		result.conditions[index] = etcdstore.Condition{Key: key}
		if read.Values[index] != nil {
			result.conditions[index].ModRevision = read.Values[index].ModRevision
		}
	}
	if read.Values[2] != nil {
		result.delivery, err = taskjournal.DecodeTaskTerminalDelivery(read.Values[2].Value)
		if err != nil || !executionplan.SameTaskTerminalReceiptAuthority(result.delivery.Receipt, receipt) {
			return terminalDeliveryAuthority{}, terminalDeliveryConflict()
		}
		if !bytes.Equal(result.delivery.Receipt.ProcessGeneration, receipt.ProcessGeneration) {
			if !allowProcessRebind {
				return terminalDeliveryAuthority{}, terminalDeliveryConflict()
			}
			result.rebind = true
		}
		if report != nil && !proto.Equal(report, result.delivery.Report) {
			return terminalDeliveryAuthority{}, terminalDeliveryConflict()
		}
		report = result.delivery.Report
	}
	if err := validateTaskTerminalReport(task, receipt, report); err != nil {
		return terminalDeliveryAuthority{}, err
	}
	result.report = proto.CloneOf(report)
	return result, nil
}

func (repository *TaskRepository) BeginTaskTerminalDelivery(
	ctx context.Context, agentID string, agentGeneration uint64, receipt *agentpb.TaskTerminalReceiptAck,
	report *agentpb.TaskAck,
) error {
	if _, err := executionplan.TaskAcknowledgementSHA256(report); err != nil {
		return err
	}
	return repository.advanceTaskTerminalDelivery(
		ctx,
		agentID,
		agentGeneration,
		receipt,
		taskjournal.TaskTerminalDeliveryCommitted,
		report,
	)
}

func (repository *TaskRepository) ApplyTaskTerminalReceipt(
	ctx context.Context, agentID string, agentGeneration uint64, applied *agentpb.TaskTerminalReceiptApplied,
) error {
	receipt, err := executionplan.ReceiptFromTaskTerminalApplied(applied)
	if err != nil {
		return err
	}
	return repository.advanceTaskTerminalDelivery(
		ctx,
		agentID,
		agentGeneration,
		receipt,
		taskjournal.TaskTerminalDeliveryApplied,
		nil,
	)
}

func (repository *TaskRepository) RetireTaskTerminalAssignment(
	ctx context.Context, agentID string, agentGeneration uint64, retired *agentpb.TaskTerminalAssignmentRetired,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if retired == nil || executionplan.RejectUnknown(retired) != nil {
		return errs.New(errs.KindValidationFailed, "Task assignment retirement is invalid")
	}
	read, err := repository.store.Get(ctx, taskjournal.TaskTerminalDeliveryKey(retired.TaskId))
	if err != nil {
		return err
	}
	if read == nil || read.Entry == nil {
		return terminalDeliveryConflict()
	}
	defer clear(read.Entry.Value)
	delivery, err := taskjournal.DecodeTaskTerminalDelivery(read.Entry.Value)
	if err != nil {
		return err
	}
	receipt := delivery.Receipt
	if receipt.TaskId != retired.TaskId || receipt.AssignmentId != retired.AssignmentId ||
		receipt.AssignmentGeneration != retired.AssignmentGeneration ||
		receipt.Terminal != retired.Terminal ||
		!bytes.Equal(receipt.PlanHash, retired.PlanHash) ||
		!bytes.Equal(receipt.ProcessGeneration, retired.ProcessGeneration) {
		return terminalDeliveryConflict()
	}
	return repository.advanceTaskTerminalDelivery(
		ctx,
		agentID,
		agentGeneration,
		receipt,
		taskjournal.TaskTerminalDeliveryRetired,
		nil,
	)
}

func (repository *TaskRepository) advanceTaskTerminalDelivery(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	receipt *agentpb.TaskTerminalReceiptAck,
	next taskjournal.TaskTerminalDeliveryPhase,
	report *agentpb.TaskAck,
) error {
	conflicts := 0
	for {
		authority, err := repository.readTerminalDeliveryAuthority(
			ctx,
			agentID,
			agentGeneration,
			receipt,
			next == taskjournal.TaskTerminalDeliveryCommitted,
			report,
		)
		if err != nil {
			return err
		}
		current := authority.delivery.Phase
		// A new authenticated process repeats delivery, never Task execution.
		// The immutable Task and receipt still supply every completion field.
		if authority.rebind {
			current = ""
		}
		if current == next || current == taskjournal.TaskTerminalDeliveryRetired ||
			current == taskjournal.TaskTerminalDeliveryApplied && next == taskjournal.TaskTerminalDeliveryCommitted {
			return nil
		}
		if !(current == "" && next == taskjournal.TaskTerminalDeliveryCommitted ||
			current == taskjournal.TaskTerminalDeliveryCommitted && next == taskjournal.TaskTerminalDeliveryApplied ||
			current == taskjournal.TaskTerminalDeliveryApplied && next == taskjournal.TaskTerminalDeliveryRetired) {
			return terminalDeliveryConflict()
		}
		encoded, err := taskjournal.EncodeTaskTerminalDelivery(
			taskjournal.TaskTerminalDeliveryRecord{
				Receipt: proto.CloneOf(receipt),
				Report:  authority.report,
				Phase:   next,
			},
		)
		if err != nil {
			return err
		}
		transaction, err := repository.store.Transact(
			ctx,
			authority.conditions,
			[]etcdstore.Mutation{
				{Type: etcdstore.MutationPut, Key: taskjournal.TaskTerminalDeliveryKey(receipt.TaskId), Value: encoded},
			},
		)
		clear(encoded)
		etcdstore.ClearValues(transaction.FailureReads)
		if err != nil {
			return err
		}
		if transaction.Succeeded {
			return nil
		}
		conflicts++
		if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
			return err
		}
	}
}

func terminalDeliveryConflict() error {
	return errs.New(errs.KindStateConflict, "Task terminal delivery authority or phase changed")
}
