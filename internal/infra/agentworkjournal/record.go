package agentworkjournal

import (
	"bytes"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// attemptRecord owns the single transport sequence shared by an operation and
// its hooks. Each typed report is retained for replay without retaining history.
type attemptRecord struct {
	Backup                  *BackupRecord                       `json:"backup,omitempty"`
	RemovalRecovery         *RemovalRecoveryRecord              `json:"removal_recovery,omitempty"`
	LifecycleRecovery       *LifecycleRecoveryRecord            `json:"lifecycle_recovery,omitempty"`
	Lifecycle               *LifecycleRecord                    `json:"lifecycle,omitempty"`
	AttachProvisionRecovery *AttachProvisionRecoveryRecord      `json:"attach_provision_recovery,omitempty"`
	AttachProvisionFailure  *AttachProvisionFailureDelivery     `json:"attach_provision_failure,omitempty"`
	AttachProvision         *AttachProvisionRecord              `json:"attach_provision,omitempty"`
	EntryRecovery           *EntryRecoveryRecord                `json:"entry_recovery,omitempty"`
	EntryConfiguration      *EntryConfigurationRecord           `json:"entry_configuration,omitempty"`
	ComponentRecovery       *ComponentRecoveryRecord            `json:"component_recovery,omitempty"`
	Component               *ComponentRecord                    `json:"component,omitempty"`
	NetworkRecovery         *NetworkRecoveryRecord              `json:"network_recovery,omitempty"`
	Network                 *NetworkRecord                      `json:"network,omitempty"`
	VolumeRecovery          *VolumeRecoveryRecord               `json:"volume_recovery,omitempty"`
	Volume                  *VolumeRecord                       `json:"volume,omitempty"`
	AttachRecovery          *AttachRecoveryRecord               `json:"attach_recovery,omitempty"`
	AttachFailure           *AttachFailureDelivery              `json:"attach_failure,omitempty"`
	AttachRemoval           *AttachRemovalRecord                `json:"attach_removal,omitempty"`
	EnvironmentFailure      *EnvironmentFailureDelivery         `json:"environment_failure,omitempty"`
	EnvironmentRemoval      *EnvironmentRemovalRecord           `json:"environment_removal,omitempty"`
	ScriptFailure           *ScriptFailureDelivery              `json:"script_failure,omitempty"`
	ScriptRecovery          *ScriptRecoveryRecord               `json:"script_recovery,omitempty"`
	Attempt                 workexecution.Attempt               `json:"attempt"`
	Sequence                uint64                              `json:"sequence"`
	UpdatedAt               time.Time                           `json:"updated_at"`
	Script                  json.RawMessage                     `json:"script,omitempty"`
	Release                 *releaseexecution.CheckpointReport  `json:"release,omitempty"`
	Effect                  *EffectRecord                       `json:"effect,omitempty"`
	Completion              *releaseexecution.CompletionReceipt `json:"completion,omitempty"`
	Failure                 *ReleaseFailureDelivery             `json:"failure,omitempty"`
	Recovery                *RecoveryRecord                     `json:"recovery,omitempty"`
	ScriptCompletion        *workexecution.Attempt              `json:"script_completion,omitempty"`
}

func (record attemptRecord) script() (*ScriptRecord, error) {
	if len(record.Script) == 0 {
		return nil, nil
	}
	value, err := decodeScript(record.Script)
	return &value, err
}

func (record attemptRecord) pending() (bool, error) {
	if record.Backup != nil {
		return record.Backup.Completion == nil, nil
	}
	if record.RemovalRecovery != nil {
		if record.RemovalRecovery.Failure != nil {
			return record.RemovalRecovery.Failure.Receipt == nil, nil
		}
		return record.RemovalRecovery.Completion == nil, nil
	}
	if record.LifecycleRecovery != nil {
		return record.LifecycleRecovery.Receipt == nil, nil
	}
	if record.Lifecycle != nil {
		if record.Lifecycle.Failure != nil {
			return record.Lifecycle.Failure.Receipt == nil, nil
		}
		return record.Lifecycle.Completion == nil, nil
	}
	if record.AttachProvisionRecovery != nil {
		return record.AttachProvisionRecovery.Receipt == nil, nil
	}
	if record.AttachProvisionFailure != nil {
		return record.AttachProvisionFailure.Receipt == nil, nil
	}
	if record.AttachProvision != nil {
		return record.AttachProvision.Completion == nil, nil
	}
	if record.EntryRecovery != nil {
		return record.EntryRecovery.Receipt == nil, nil
	}
	if record.EntryConfiguration != nil {
		if record.EntryConfiguration.Failure != nil {
			return record.EntryConfiguration.Failure.Receipt == nil, nil
		}
		return !record.EntryConfiguration.Acknowledged, nil
	}
	if record.ComponentRecovery != nil {
		if record.ComponentRecovery.Failure != nil {
			return record.ComponentRecovery.Failure.Receipt == nil, nil
		}
		return record.ComponentRecovery.Receipt == nil, nil
	}
	if record.Component != nil {
		if record.Component.Failure != nil {
			return record.Component.Failure.Receipt == nil, nil
		}
		return !record.Component.Acknowledged, nil
	}
	if record.NetworkRecovery != nil {
		if record.NetworkRecovery.Failure != nil {
			return record.NetworkRecovery.Failure.Receipt == nil, nil
		}
		return record.NetworkRecovery.Receipt == nil, nil
	}
	if record.Network != nil {
		if record.Network.Failure != nil {
			return record.Network.Failure.Receipt == nil, nil
		}
		return !record.Network.Acknowledged, nil
	}
	if record.VolumeRecovery != nil {
		if record.VolumeRecovery.Failure != nil {
			return record.VolumeRecovery.Failure.Receipt == nil, nil
		}
		return record.VolumeRecovery.Receipt == nil, nil
	}
	if record.Volume != nil {
		if record.Volume.Failure != nil {
			return record.Volume.Failure.Receipt == nil, nil
		}
		return !record.Volume.Acknowledged, nil
	}
	if record.AttachRecovery != nil {
		return record.AttachRecovery.Receipt == nil, nil
	}
	if record.AttachFailure != nil {
		return record.AttachFailure.Receipt == nil, nil
	}
	if record.AttachRemoval != nil {
		return !record.AttachRemoval.Acknowledged, nil
	}
	if record.EnvironmentFailure != nil {
		return record.EnvironmentFailure.Receipt == nil, nil
	}
	if record.EnvironmentRemoval != nil {
		return !record.EnvironmentRemoval.Acknowledged, nil
	}
	if record.ScriptFailure != nil {
		return record.ScriptFailure.Receipt == nil, nil
	}
	if record.ScriptRecovery != nil {
		return record.ScriptRecovery.Receipt == nil, nil
	}
	script, err := record.script()
	if err != nil {
		return false, err
	}
	return record.Recovery != nil && (record.Recovery.Report.Ack == nil || record.Recovery.Effect != nil && !record.Recovery.Effect.Acknowledged) || record.Failure != nil || script != nil && script.Ack == nil || record.Release != nil && record.Release.Ack == nil || record.Effect != nil && !record.Effect.Acknowledged, nil
}

func (record attemptRecord) validate() error {
	if record.Backup != nil {
		return record.validateBackup()
	}
	if record.RemovalRecovery != nil {
		return record.validateRemovalRecovery()
	}
	if record.LifecycleRecovery != nil {
		return record.validateLifecycleRecovery()
	}
	if record.Lifecycle != nil {
		return record.validateLifecycle()
	}
	if record.AttachProvisionRecovery != nil {
		return record.validateAttachProvisionRecovery()
	}
	if record.AttachProvisionFailure != nil {
		return record.validateAttachProvisionFailure()
	}
	if record.AttachProvision != nil {
		return record.validateAttachProvision()
	}
	if record.EntryRecovery != nil {
		return record.validateEntryRecovery()
	}
	if record.EntryConfiguration != nil {
		return record.validateEntryConfiguration()
	}
	if record.ComponentRecovery != nil {
		return record.validateComponentRecovery()
	}
	if record.Component != nil {
		return record.validateComponent()
	}
	if record.ScriptCompletion != nil {
		if err := record.validateScriptCompletion(); err != nil {
			return err
		}
	}
	if record.Attempt.Validate() != nil || record.Attempt.Executor.Executor != workexecution.Agent || record.Attempt.FinishedAt != nil ||
		record.UpdatedAt.IsZero() || record.UpdatedAt.Before(record.Attempt.AssignedAt) {
		return invalid()
	}
	if record.Network != nil {
		return record.validateNetwork()
	}
	if record.NetworkRecovery != nil {
		return record.validateNetworkRecovery()
	}
	if record.Volume != nil {
		return record.validateVolume()
	}
	if record.VolumeRecovery != nil {
		return record.validateVolumeRecovery()
	}
	if record.AttachRecovery != nil {
		return record.validateAttachRecovery()
	}
	if record.AttachFailure != nil {
		return record.validateAttachFailure()
	}
	if record.AttachRemoval != nil {
		return record.validateAttachRemoval()
	}
	if record.EnvironmentFailure != nil {
		return record.validateEnvironmentFailure()
	}
	if record.EnvironmentRemoval != nil {
		return record.validateEnvironmentRemoval()
	}
	if record.ScriptFailure != nil {
		return record.validateScriptFailure()
	}
	if record.ScriptRecovery != nil {
		return record.validateScriptRecovery()
	}
	if record.Recovery != nil {
		if record.Release != nil || record.Completion != nil || record.Effect != nil || len(record.Script) != 0 ||
			record.Recovery.Report.Progress.Sequence != record.Sequence || !record.Recovery.Report.Progress.UpdatedAt.Equal(record.UpdatedAt) {
			return invalid()
		}
		input := record.Recovery.input(record.Attempt)
		if err := record.Recovery.Report.ValidateEnvelope(input); err != nil {
			return err
		}
		if failure := record.Failure; failure != nil {
			if record.Recovery.Completion != nil || record.Recovery.Report.Progress.Phase == releaseexecution.RecoveryComplete ||
				(failure.Reason != releaseexecution.ExecutionFailed && failure.Reason != releaseexecution.DeadlineExpired && failure.Reason != releaseexecution.ExecutionInterrupted) ||
				failure.Receipt != nil && failure.Receipt.Validate(record.Attempt, failure.Reason) != nil {
				return invalid()
			}
		}
		if effect := record.Recovery.Effect; effect != nil {
			if _, err := releaseexecution.DecodeRecoveryEffectEnvelope(input, effect.Boundary, effect.Raw, effect.Reference); err != nil {
				return err
			}
			if effect.Boundary.Progress.Sequence > record.Sequence || !effect.Acknowledged &&
				(effect.Boundary.Progress.Sequence != record.Sequence || record.Recovery.Report.Ack == nil) {
				return invalid()
			}
		}
		if receipt := record.Recovery.Completion; receipt != nil {
			if record.Recovery.Report.Ack == nil || record.Recovery.Report.Progress.Phase != releaseexecution.RecoveryComplete ||
				record.Recovery.Report.Progress.InFlight || record.Recovery.Effect == nil || !record.Recovery.Effect.Acknowledged ||
				receipt.Outcome != workexecution.Succeeded || receipt.FinishedAt.IsZero() || receipt.FinishedAt.Location() != time.UTC ||
				receipt.FinishedAt.Before(record.UpdatedAt) {
				return invalid()
			}
		}
		return nil
	}
	if record.Failure != nil {
		if receipt := record.Failure.Receipt; receipt != nil && receipt.Validate(record.Attempt, record.Failure.Reason) != nil {
			return invalid()
		}
		if record.Completion != nil || record.Attempt.Direction == workexecution.Recovery && record.Sequence != 0 ||
			(record.Failure.Reason != releaseexecution.ExecutionFailed && record.Failure.Reason != releaseexecution.DeadlineExpired && record.Failure.Reason != releaseexecution.ExecutionInterrupted) ||
			record.Release != nil && record.Release.Progress.Phase == releaseexecution.Complete {
			return invalid()
		}
	}
	if record.Sequence == 0 {
		if record.Failure == nil || record.Release != nil || record.Effect != nil || len(record.Script) != 0 || !record.UpdatedAt.Equal(record.Attempt.AssignedAt) {
			return invalid()
		}
		return nil
	}
	script, err := record.script()
	if err != nil {
		return err
	}
	maximum, pending := uint64(0), 0
	if script != nil {
		if !workexecution.SameAssignment(script.Attempt, record.Attempt) || script.Report.Sequence > record.Sequence ||
			script.Report.RecordedAt.AsTime().After(record.UpdatedAt) {
			return invalid()
		}
		maximum = script.Report.Sequence
		if maximum == record.Sequence && !script.Report.RecordedAt.AsTime().Equal(record.UpdatedAt) {
			return invalid()
		}
		if script.Ack == nil {
			pending++
			if maximum != record.Sequence {
				return invalid()
			}
		}
	}
	if record.Release != nil {
		if record.Release.ValidateEnvelope(record.Attempt) != nil || record.Release.Sequence > record.Sequence || record.Release.Progress.UpdatedAt.After(record.UpdatedAt) {
			return invalid()
		}
		if script != nil && script.Report.Sequence == record.Release.Sequence {
			return invalid()
		}
		if record.Release.Sequence > maximum {
			maximum = record.Release.Sequence
		}
		if record.Release.Sequence == record.Sequence && !record.Release.Progress.UpdatedAt.Equal(record.UpdatedAt) {
			return invalid()
		}
		if record.Release.Ack == nil {
			pending++
			if record.Release.Sequence != record.Sequence {
				return invalid()
			}
		}
	}
	if maximum != record.Sequence || pending > 1 {
		return invalid()
	}
	if record.Effect != nil {
		if record.Effect.validate(record.Attempt) != nil || record.Effect.Boundary.Sequence > record.Sequence ||
			!record.Effect.Acknowledged && (pending != 0 || record.Effect.Boundary.Sequence != record.Sequence) {
			return invalid()
		}
	}
	if receipt := record.Completion; receipt != nil {
		if record.Release == nil || record.Release.Ack == nil || record.Release.Progress.Phase != releaseexecution.Complete ||
			record.Release.Progress.InFlight || record.Release.Sequence != record.Sequence || pending != 0 ||
			record.Effect != nil && !record.Effect.Acknowledged || receipt.FinishedAt.IsZero() ||
			receipt.FinishedAt.Location() != time.UTC || receipt.FinishedAt.Before(record.UpdatedAt) ||
			(receipt.Outcome != workexecution.Succeeded && receipt.Outcome != workexecution.Cancelled && receipt.Outcome != workexecution.Superseded) {
			return invalid()
		}
	}
	return nil
}

func encodeRecord(record attemptRecord) ([]byte, error) {
	if err := record.validate(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(record)
	if err != nil {
		return nil, invalid()
	}
	if len(raw) > MaximumRecordBytes {
		return nil, invalid()
	}
	return jcs.Canonicalize(raw)
}

func decodeRecord(raw []byte) (attemptRecord, error) {
	if len(raw) == 0 || len(raw) > MaximumRecordBytes {
		return attemptRecord{}, invalid()
	}
	record, err := jcs.Decode[attemptRecord](raw)
	if err != nil {
		return attemptRecord{}, invalid()
	}
	encoded, err := encodeRecord(record)
	if err != nil || !bytes.Equal(encoded, raw) {
		return attemptRecord{}, invalid()
	}
	return record, nil
}
