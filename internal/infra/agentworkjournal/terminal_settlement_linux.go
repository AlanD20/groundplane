package agentworkjournal

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workreceipt"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SettleTerminalDelivery applies a saved authenticated ACK through the existing
// operation record validator, then uses that operation's ordinary retirement.
// Failure ACKs do not prove helper termination; existing recovery retention stays
// in force. No command, effect, checkpoint or predicted outcome is generated.
func (journal *Journal) SettleTerminalDelivery(ctx context.Context, attemptID string) error {
	terminal, kind, err := journal.saveTerminalDeliveryReceipt(ctx, attemptID)
	if err != nil {
		return err
	}
	switch kind {
	case "resolver_completion", "resolver_failure", "resolver_recovery":
		return journal.RetireResolver(ctx, terminal)
	case "attach_provision_recovery":
		return journal.RetireProvisionInspection(ctx, terminal)
	case "attach_removal_completion":
		if terminal.Outcome == workexecution.Succeeded {
			return journal.RetireCompletedAttach(ctx, terminal)
		}
	case "release_failure":
		// This receipt settles transport only. The Release cleanup owner still
		// needs its captured forward/helper evidence, including failed cleanup.
		return nil
	}
	return journal.Retire(ctx, terminal)
}

func (journal *Journal) saveTerminalDeliveryReceipt(ctx context.Context, attemptID string) (workexecution.Attempt, string, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, "", err
	}
	defer unlock()
	record, err := journal.readRecord(ctx, attemptID)
	if err != nil {
		return workexecution.Attempt{}, "", err
	}
	if record == nil || record.TerminalDelivery == nil {
		return workexecution.Attempt{}, "", conflict()
	}
	request, err := record.TerminalDelivery.Message()
	if err != nil {
		return workexecution.Attempt{}, "", err
	}
	reply, err := record.TerminalDelivery.Acknowledgement()
	if err != nil {
		return workexecution.Attempt{}, "", err
	}
	if reply == nil {
		return workexecution.Attempt{}, "", conflict()
	}
	terminal, failure, err := workreceipt.Terminal(request, reply, record.Attempt)
	if err != nil {
		return workexecution.Attempt{}, "", err
	}
	if terminal.FinishedAt.Before(record.UpdatedAt) {
		return workexecution.Attempt{}, "", conflict()
	}
	if err := record.applyTerminalReceipt(request, reply, terminal, failure); err != nil {
		return workexecution.Attempt{}, "", err
	}
	if err := journal.writeRecord(ctx, *record); err != nil {
		return workexecution.Attempt{}, "", err
	}
	_, kind, _, err := workreceipt.Describe(request)
	return terminal, kind, err
}

func (record *attemptRecord) applyTerminalReceipt(request *agentpb.AgentMessage, reply *agentpb.ControllerMessage, terminal workexecution.Attempt, failure []byte) error {
	_, kind, _, err := workreceipt.Describe(request)
	if err != nil {
		return err
	}
	switch kind {
	case "resolver_completion", "resolver_failure", "resolver_recovery":
		if record.Resolver == nil {
			return conflict()
		}
		record.Resolver.Receipt = &terminal
		if kind == "resolver_failure" {
			record.Resolver.Failure = slices.Clone(failure)
		}
	case "backing_completion":
		if record.Backing == nil || record.Backing.Failure != nil {
			return conflict()
		}
		record.Backing.Completion = &terminal
	case "backing_failure":
		if record.Backing == nil || record.Backing.Failure == nil {
			return conflict()
		}
		record.Backing.Failure.Receipt, record.Backing.Failure.Evidence = &terminal, slices.Clone(failure)
	case "backing_recovery":
		if record.BackingRecovery == nil {
			return conflict()
		}
		record.BackingRecovery.Receipt = &terminal
	case "environment_provision_result":
		if record.EnvironmentProvision == nil {
			return conflict()
		}
		record.EnvironmentProvision.Submitted, record.EnvironmentProvision.Receipt = true, &terminal
	case "backup_completion", "backup_recovery", "backup_failure":
		if record.Backup == nil {
			return conflict()
		}
		record.Backup.Completion = &terminal
		if kind == "backup_failure" {
			raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(reply.GetBackupWorkFailureAck())
			if err != nil {
				return invalid()
			}
			record.Backup.FailureAck = raw
		}
	case "removal_recovery_completion":
		if record.RemovalRecovery == nil {
			return conflict()
		}
		record.RemovalRecovery.Completion = &terminal
	case "removal_recovery_failure":
		if record.RemovalRecovery == nil || record.RemovalRecovery.Failure == nil {
			return conflict()
		}
		record.RemovalRecovery.Failure.Receipt, record.RemovalRecovery.Failure.Evidence = &terminal, slices.Clone(failure)
	case "lifecycle_completion":
		if record.Lifecycle == nil {
			return conflict()
		}
		if reply.GetServiceLifecycleCompletionAck().GetStopped() != nil {
			proof, err := lifecycleexecution.DecodeFailureStages(terminal, record.Lifecycle.Stages, failure, *terminal.Result)
			if err != nil {
				return err
			}
			record.Lifecycle.Failure = &LifecycleFailureDelivery{Reason: proof.Reason, Receipt: &terminal, Evidence: slices.Clone(failure)}
		} else {
			record.Lifecycle.Completion = &terminal
		}
	case "lifecycle_failure":
		if record.Lifecycle == nil || record.Lifecycle.Failure == nil {
			return conflict()
		}
		record.Lifecycle.Failure.Receipt, record.Lifecycle.Failure.Evidence = &terminal, slices.Clone(failure)
	case "lifecycle_recovery":
		if record.LifecycleRecovery == nil {
			return conflict()
		}
		record.LifecycleRecovery.Receipt = &terminal
	case "attach_provision_completion":
		if record.AttachProvision == nil {
			return conflict()
		}
		record.AttachProvision.Completion = &terminal
	case "attach_provision_failure":
		if record.AttachProvisionFailure == nil {
			return conflict()
		}
		record.AttachProvisionFailure.Receipt, record.AttachProvisionFailure.Evidence = &terminal, slices.Clone(failure)
	case "attach_provision_recovery":
		if record.AttachProvisionRecovery == nil {
			return conflict()
		}
		record.AttachProvisionRecovery.Receipt = &terminal
	case "entry_completion":
		if record.EntryConfiguration == nil {
			return conflict()
		}
		record.EntryConfiguration.Completion = &terminal
	case "entry_failure":
		if record.EntryConfiguration == nil || record.EntryConfiguration.Failure == nil {
			return conflict()
		}
		record.EntryConfiguration.Failure.Receipt, record.EntryConfiguration.Failure.Evidence = &terminal, slices.Clone(failure)
	case "entry_recovery":
		if record.EntryRecovery == nil {
			return conflict()
		}
		record.EntryRecovery.Receipt = &terminal
	case "component_completion":
		if record.Component == nil {
			return conflict()
		}
		record.Component.Completion = &terminal
	case "component_failure":
		if record.Component != nil && record.Component.Failure != nil {
			record.Component.Failure.Receipt, record.Component.Failure.Evidence = &terminal, slices.Clone(failure)
		} else if record.ComponentRecovery != nil && record.ComponentRecovery.Failure != nil {
			record.ComponentRecovery.Failure.Receipt, record.ComponentRecovery.Failure.Evidence = &terminal, slices.Clone(failure)
		} else {
			return conflict()
		}
	case "component_recovery":
		if record.ComponentRecovery == nil {
			return conflict()
		}
		record.ComponentRecovery.Receipt = &terminal
	case "network_completion":
		if record.Network == nil {
			return conflict()
		}
		record.Network.Completion = &terminal
	case "network_failure":
		if record.Network != nil && record.Network.Failure != nil {
			record.Network.Failure.Receipt, record.Network.Failure.Evidence = &terminal, slices.Clone(failure)
		} else if record.NetworkRecovery != nil && record.NetworkRecovery.Failure != nil {
			record.NetworkRecovery.Failure.Receipt, record.NetworkRecovery.Failure.Evidence = &terminal, slices.Clone(failure)
		} else {
			return conflict()
		}
	case "network_recovery":
		if record.NetworkRecovery == nil {
			return conflict()
		}
		record.NetworkRecovery.Receipt = &terminal
	case "volume_completion":
		if record.Volume == nil {
			return conflict()
		}
		record.Volume.Completion = &terminal
	case "volume_failure":
		if record.Volume != nil && record.Volume.Failure != nil {
			record.Volume.Failure.Receipt, record.Volume.Failure.Evidence = &terminal, slices.Clone(failure)
		} else if record.VolumeRecovery != nil && record.VolumeRecovery.Failure != nil {
			record.VolumeRecovery.Failure.Receipt, record.VolumeRecovery.Failure.Evidence = &terminal, slices.Clone(failure)
		} else {
			return conflict()
		}
	case "volume_recovery":
		if record.VolumeRecovery == nil {
			return conflict()
		}
		record.VolumeRecovery.Receipt = &terminal
	case "attach_removal_completion":
		if record.AttachRemoval == nil {
			return conflict()
		}
		record.AttachRemoval.Completion = &terminal
	case "attach_removal_failure":
		if record.AttachFailure == nil {
			return conflict()
		}
		record.AttachFailure.Receipt, record.AttachFailure.Evidence = &terminal, slices.Clone(failure)
	case "attach_recovery_completion":
		if record.AttachRecovery == nil {
			return conflict()
		}
		record.AttachRecovery.Receipt = &terminal
	case "environment_removal_completion":
		if record.EnvironmentRemoval == nil {
			return conflict()
		}
		record.EnvironmentRemoval.Completion = &terminal
	case "environment_removal_failure":
		if record.EnvironmentFailure == nil {
			return conflict()
		}
		record.EnvironmentFailure.Receipt, record.EnvironmentFailure.Evidence = &terminal, slices.Clone(failure)
	case "script_completion":
		record.ScriptCompletion = &terminal
	case "script_recovery_completion":
		if record.ScriptRecovery == nil {
			return conflict()
		}
		record.ScriptRecovery.Receipt = &terminal
	case "script_failure":
		if record.ScriptFailure == nil {
			return conflict()
		}
		record.ScriptFailure.Receipt = &terminal
	case "release_completion":
		receipt := &releaseexecution.CompletionReceipt{Outcome: terminal.Outcome, FinishedAt: *terminal.FinishedAt}
		if record.Recovery != nil {
			record.Recovery.Completion = receipt
		} else {
			record.Completion = receipt
		}
	case "release_failure":
		if record.Failure == nil {
			return conflict()
		}
		receipt, err := releaseexecution.DecodeFailureAck(record.Attempt, record.Failure.Reason, reply.GetReleaseWorkFailureAck())
		if err != nil {
			return err
		}
		record.Failure.Receipt = &receipt
	default:
		return invalid()
	}
	return record.validate()
}
