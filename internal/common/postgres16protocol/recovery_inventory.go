package postgres16protocol

import (
	"bytes"
	"encoding/json"
	"io"
)

const (
	MaximumRecoveryInventoryRecords       = 1024
	MaximumRecoveryInventoryEncodedBytes  = 8 * 1024 * 1024
	MaximumRecoveryRetirementEncodedBytes = 16 * 1024
)

// RecoveryInventory is the bounded, canonical view of every retained helper
// execution in one managed PostgreSQL container. An empty, non-nil Records
// slice proves that the namespace has no retained execution; each non-empty
// record independently states whether its execution completed successfully.
type RecoveryInventory struct {
	Records []RecoveryRecord `json:"records"`
}

// RecoveryRecord is a compact classification of one validated state journal.
// StateSHA256 binds the complete current ConfinementStateShape without exposing
// its larger representation. Clean is true only for a successful, fully reaped
// client execution with complete streams and validated state/process/exec links.
// Every pre-exec, unavailable, failed, unreaped, or otherwise uncertain record
// remains present with Clean false.
type RecoveryRecord struct {
	Nonce                  Nonce                   `json:"nonce"`
	HasProcess             bool                    `json:"has_process"`
	HasExecIntent          bool                    `json:"has_exec_intent"`
	Operation              Operation               `json:"operation"`
	RequestSHA256          Digest                  `json:"request_sha256"`
	StateSHA256            Digest                  `json:"state_sha256"`
	Sequence               uint64                  `json:"sequence"`
	Phase                  ConfinementStatePhase   `json:"phase"`
	ProgressPhase          ConfinementStatePhase   `json:"progress_phase"`
	TerminalKind           ConfinementTerminalKind `json:"terminal_kind"`
	TerminalExitCode       uint32                  `json:"terminal_exit_code"`
	TerminalSignal         uint32                  `json:"terminal_signal"`
	TerminalCoreDumped     bool                    `json:"terminal_core_dumped"`
	TerminalRawWaitStatus  uint32                  `json:"terminal_raw_wait_status"`
	TerminalWait4Reaped    bool                    `json:"terminal_wait4_reaped"`
	TerminalEvidenceSHA256 Digest                  `json:"terminal_evidence_sha256"`
	StdinEOF               bool                    `json:"stdin_eof"`
	StdoutEOF              bool                    `json:"stdout_eof"`
	StderrEOF              bool                    `json:"stderr_eof"`
	Clean                  bool                    `json:"clean"`
}

// NewRecoveryRecord compacts a state whose journal and retained links were
// already descriptor-confined and validated by the helper.
func NewRecoveryRecord(
	nonce Nonce,
	hasProcess bool,
	hasExecIntent bool,
	operation Operation,
	requestSHA256 Digest,
	state ConfinementStateShape,
) (RecoveryRecord, error) {
	if nonce == (Nonce{}) || state.Validate() != nil ||
		hasExecIntent != (operation.ValidRun() && requestSHA256 != (Digest{})) ||
		hasExecIntent && !hasProcess {
		return RecoveryRecord{}, invalid("postgres recovery record authority is invalid")
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return RecoveryRecord{}, invalid("postgres recovery state encoding is invalid")
	}
	record := RecoveryRecord{
		Nonce: nonce, HasProcess: hasProcess, HasExecIntent: hasExecIntent,
		Operation: operation, RequestSHA256: requestSHA256,
		StateSHA256: domainSeparatedDigest("groundplane.postgres16.recovery-state.v1", encoded),
		Sequence:    state.Sequence, Phase: state.Phase, ProgressPhase: state.ProgressPhase,
	}
	if state.IOEvidence != nil {
		record.StdinEOF = state.IOEvidence.Stdin.EOF
		record.StdoutEOF = state.IOEvidence.Stdout.EOF
		record.StderrEOF = state.IOEvidence.Stderr.EOF
	}
	if state.Terminal != nil {
		record.TerminalKind = state.Terminal.Kind
		record.TerminalExitCode = state.Terminal.ExitCode
		record.TerminalSignal = state.Terminal.Signal
		record.TerminalCoreDumped = state.Terminal.CoreDumped
		record.TerminalRawWaitStatus = state.Terminal.RawWaitStatus
		record.TerminalWait4Reaped = state.Terminal.Wait4Reaped
		record.TerminalEvidenceSHA256 = state.Terminal.EvidenceSHA256
	}
	if hasExecIntent {
		record.Clean = (ExecutionEvidence{
			Nonce: nonce, RequestSHA256: requestSHA256, State: state,
		}).Validate() == nil
	}
	if err := record.Validate(); err != nil {
		return RecoveryRecord{}, err
	}
	return record, nil
}

func (record RecoveryRecord) Validate() error {
	if record.Nonce == (Nonce{}) || record.StateSHA256 == (Digest{}) || record.Sequence == 0 ||
		record.Phase < ConfinementPhaseCreated || record.Phase > ConfinementPhaseRecoveryRetired ||
		record.ProgressPhase < ConfinementPhaseCreated || record.ProgressPhase > ConfinementPhaseIOComplete ||
		record.HasExecIntent != (record.Operation.ValidRun() && record.RequestSHA256 != (Digest{})) ||
		record.HasExecIntent && !record.HasProcess {
		return invalid("postgres recovery record is invalid")
	}
	if record.Phase < ConfinementPhaseTerminal && record.Phase != record.ProgressPhase ||
		!record.HasProcess && record.ProgressPhase != ConfinementPhaseCreated ||
		!record.HasExecIntent && record.ProgressPhase >= ConfinementPhaseChildDurable ||
		record.HasExecIntent && record.ProgressPhase < ConfinementPhaseProfileApplied {
		return invalid("postgres recovery record progress is invalid")
	}
	terminal := record.Phase >= ConfinementPhaseTerminal
	if terminal != (record.TerminalKind != 0) || terminal != (record.TerminalEvidenceSHA256 != (Digest{})) {
		return invalid("postgres recovery record terminal classification is invalid")
	}
	if !terminal && (record.TerminalExitCode != 0 || record.TerminalSignal != 0 ||
		record.TerminalCoreDumped || record.TerminalRawWaitStatus != 0 || record.TerminalWait4Reaped) {
		return invalid("postgres recovery record terminal evidence is inapplicable")
	}
	if terminal {
		classification := ConfinementTerminal{
			Kind: record.TerminalKind, ExitCode: record.TerminalExitCode,
			Signal: record.TerminalSignal, CoreDumped: record.TerminalCoreDumped,
			RawWaitStatus: record.TerminalRawWaitStatus, Wait4Reaped: record.TerminalWait4Reaped,
			EvidenceSHA256: record.TerminalEvidenceSHA256,
		}
		if classification.Validate(record.Phase) != nil {
			return invalid("postgres recovery record terminal state is invalid")
		}
	}
	if record.ProgressPhase < ConfinementPhaseIOComplete &&
		(record.StdinEOF || record.StdoutEOF || record.StderrEOF) {
		return invalid("postgres recovery record stream evidence is inapplicable")
	}
	clean := record.HasProcess && record.HasExecIntent && record.Phase == ConfinementPhaseReaped &&
		record.ProgressPhase == ConfinementPhaseIOComplete &&
		record.TerminalKind == ConfinementTerminalExited && record.TerminalExitCode == 0 &&
		record.TerminalRawWaitStatus == 0 && record.TerminalWait4Reaped &&
		record.StdinEOF && record.StdoutEOF && record.StderrEOF
	if record.Clean != clean {
		return invalid("postgres recovery record clean classification is invalid")
	}
	return nil
}

// Completed reports the only helper state that is clean for startup: the
// original client exited successfully, every stream reached EOF, and its
// original parent reaped the exact child. It never treats RecoveryRetired as
// completion.
func (record RecoveryRecord) Completed() bool {
	return record.Clean && record.Validate() == nil
}

// SettledWithoutRestoreApply reports an exact client outcome that is safe to
// discard without claiming success: the original parent reaped the client,
// every stream reached EOF, and the fixed operation was not RestoreApply.
func (record RecoveryRecord) SettledWithoutRestoreApply() bool {
	if record.Validate() != nil || !record.HasProcess || !record.HasExecIntent ||
		record.Operation == OperationRestoreApply || record.Phase != ConfinementPhaseReaped ||
		record.ProgressPhase != ConfinementPhaseIOComplete || !record.TerminalWait4Reaped ||
		!record.StdinEOF || !record.StdoutEOF || !record.StderrEOF {
		return false
	}
	return record.TerminalKind == ConfinementTerminalExited ||
		record.TerminalKind == ConfinementTerminalSignaled
}

// Retirable reports records whose retained helper files may be deleted. A
// RestoreApply remains retirement-eligible only after successful completion;
// fixed non-apply operations may also retire a fully settled failed outcome.
func (record RecoveryRecord) Retirable() bool {
	return record.Completed() || record.SettledWithoutRestoreApply()
}

// RecoveryRetirement is the single-record durable deletion marker. It retains
// exact operation and request authority while the linked files are removed.
type RecoveryRetirement struct {
	Record RecoveryRecord `json:"record"`
}

func (retirement RecoveryRetirement) Validate() error {
	if !retirement.Record.Retirable() {
		return invalid("postgres recovery retirement record is not retirable")
	}
	return nil
}

func MarshalRecoveryRetirement(retirement RecoveryRetirement) ([]byte, error) {
	if err := retirement.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(retirement)
	if err != nil || len(encoded) > MaximumRecoveryRetirementEncodedBytes {
		return nil, invalid("postgres recovery retirement encoding is invalid")
	}
	return encoded, nil
}

func ParseRecoveryRetirement(encoded []byte) (RecoveryRetirement, error) {
	if len(encoded) == 0 || len(encoded) > MaximumRecoveryRetirementEncodedBytes {
		return RecoveryRetirement{}, invalid("postgres recovery retirement size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var retirement RecoveryRetirement
	if err := decoder.Decode(&retirement); err != nil || retirement.Validate() != nil {
		return RecoveryRetirement{}, invalid("postgres recovery retirement is invalid")
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return RecoveryRetirement{}, invalid("postgres recovery retirement has trailing data")
	}
	canonical, err := MarshalRecoveryRetirement(retirement)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return RecoveryRetirement{}, invalid("postgres recovery retirement is not canonical")
	}
	return retirement, nil
}

func (inventory RecoveryInventory) Validate() error {
	if inventory.Records == nil || len(inventory.Records) > MaximumRecoveryInventoryRecords {
		return invalid("postgres recovery inventory record count is invalid")
	}
	for index, record := range inventory.Records {
		if err := record.Validate(); err != nil {
			return err
		}
		if index > 0 && bytes.Compare(inventory.Records[index-1].Nonce[:], record.Nonce[:]) >= 0 {
			return invalid("postgres recovery inventory order is invalid")
		}
	}
	return nil
}

func MarshalRecoveryInventory(inventory RecoveryInventory) ([]byte, error) {
	if err := inventory.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(inventory)
	if err != nil || len(encoded) > MaximumRecoveryInventoryEncodedBytes {
		return nil, invalid("postgres recovery inventory encoding is invalid")
	}
	return encoded, nil
}

func ParseRecoveryInventory(encoded []byte) (RecoveryInventory, error) {
	if len(encoded) == 0 || len(encoded) > MaximumRecoveryInventoryEncodedBytes {
		return RecoveryInventory{}, invalid("postgres recovery inventory size is invalid")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var inventory RecoveryInventory
	if err := decoder.Decode(&inventory); err != nil || inventory.Validate() != nil {
		return RecoveryInventory{}, invalid("postgres recovery inventory is invalid")
	}
	var trailing json.RawMessage
	if decoder.Decode(&trailing) != io.EOF {
		return RecoveryInventory{}, invalid("postgres recovery inventory has trailing data")
	}
	canonical, err := MarshalRecoveryInventory(inventory)
	if err != nil || !bytes.Equal(canonical, encoded) {
		return RecoveryInventory{}, invalid("postgres recovery inventory is not canonical")
	}
	return inventory, nil
}
