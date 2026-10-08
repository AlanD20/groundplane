package agent

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentstagingjournal"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type backupStagingState struct {
	mu        sync.Mutex
	active    map[backupstage.IDs]*backupstage.Stage
	cleaned   map[backupstage.IDs]bool
	journal   *agentstagingjournal.Journal
	recovery  *backupstage.RecoverySession
	inventory *agentpb.BackupStagingInventory
	physical  map[string]backupstage.Recovered
	volumes   map[string]recoveredVolumeRestore
	stager    *backupstage.Stager
	prepared  []*backupstage.PreparedStage
	held      map[backupstage.IDs]*backupstage.Stage
	holdIDs   map[backupstage.IDs]bool
	plan      *agentpb.BackupStagingRecoveryPlan
	ready     bool
	reinspect atomic.Bool
}

func openBackupStaging(ctx context.Context, journal *agentstagingjournal.Journal) (*backupStagingState, error) {
	retained, found, err := journal.Read(ctx)
	if err != nil {
		return nil, err
	}
	if found && retained.Phase == agentstagingjournal.PhaseReceiptAccepted {
		if err := journal.AcceptReceipt(ctx, retained.Receipt); err != nil {
			return nil, err
		}
		found = false
	}
	if err := backupstage.PrepareAgentRoot(ctx); err != nil {
		return nil, err
	}
	recovery, err := backupstage.OpenRecovery(ctx, backupstage.Config{Root: backupstage.AgentRoot})
	if err != nil {
		return nil, err
	}
	state := &backupStagingState{
		journal:   journal,
		recovery:  recovery,
		inventory: &agentpb.BackupStagingInventory{},
		physical:  make(map[string]backupstage.Recovered),
		volumes:   make(map[string]recoveredVolumeRestore),
		active:    make(map[backupstage.IDs]*backupstage.Stage),
		cleaned:   make(map[backupstage.IDs]bool),
		held:      make(map[backupstage.IDs]*backupstage.Stage),
		holdIDs:   make(map[backupstage.IDs]bool),
	}
	for _, entry := range recovery.Inventory() {
		key, err := executionplan.BackupStagingRecoveryKey(entry.IDs.Task, entry.IDs.Step, entry.IDs.Point)
		if err != nil {
			_ = recovery.Close(ctx)
			return nil, err
		}
		wire := &agentpb.BackupRecoveredStage{RecoveryKeySha256: key}
		for _, file := range entry.Files {
			role := backupRecoveredFileRole(file.Name)
			if role == agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_UNSPECIFIED {
				_ = recovery.Close(ctx)
				return nil, invalidAgentStaging()
			}
			wire.Files = append(
				wire.Files,
				&agentpb.BackupRecoveredFile{
					Role:      role,
					SizeBytes: file.Size,
					Sha256:    append([]byte(nil), file.SHA256[:]...),
				},
			)
		}
		sort.Slice(wire.Files, func(i, j int) bool { return wire.Files[i].Role < wire.Files[j].Role })
		state.inventory.Entries = append(state.inventory.Entries, wire)
		state.physical[hex.EncodeToString(key)] = entry
	}
	if err := inventoryDatabaseExecutions(ctx, state.inventory); err != nil {
		_ = recovery.Close(ctx)
		return nil, err
	}
	sort.Slice(state.inventory.Entries, func(i, j int) bool {
		return bytes.Compare(
			state.inventory.Entries[i].RecoveryKeySha256,
			state.inventory.Entries[j].RecoveryKeySha256,
		) < 0
	})
	state.inventory.VolumeRestores, state.volumes, err = inventoryBackupVolumeRestores(ctx)
	if err != nil {
		_ = recovery.Close(ctx)
		return nil, err
	}
	if err := executionplan.ValidateBackupStagingInventory(state.inventory); err != nil {
		_ = recovery.Close(ctx)
		return nil, err
	}
	if found {
		// A crash may have completed some recorded discards. Only exact remaining
		// files from the recorded inventory are admissible; new bytes are not.
		if err := validateRetainedStagingInventory(state.inventory, retained.Inventory, retained.Plan); err != nil {
			_ = recovery.Close(ctx)
			return nil, err
		}
		state.inventory = retained.Inventory
	}
	return state, nil
}

func validateRetainedStagingInventory(physical, retained *agentpb.BackupStagingInventory,
	plan *agentpb.BackupStagingRecoveryPlan,
) error {
	known := make(map[string]*agentpb.BackupRecoveredStage, len(retained.Entries))
	for _, entry := range retained.Entries {
		known[hex.EncodeToString(entry.RecoveryKeySha256)] = entry
	}
	for _, actual := range physical.Entries {
		expected := known[hex.EncodeToString(actual.RecoveryKeySha256)]
		if expected == nil {
			return invalidAgentStaging()
		}
		if actual.DatabaseExecution && !expected.DatabaseExecution {
			return invalidAgentStaging()
		}
		for _, file := range actual.Files {
			matched := false
			for _, original := range expected.Files {
				if proto.Equal(file, original) {
					matched = true
					break
				}
			}
			if !matched {
				return invalidAgentStaging()
			}
		}
	}
	knownVolumes := make(map[string]*agentpb.BackupRecoveredVolumeRestore, len(retained.VolumeRestores))
	for _, entry := range retained.VolumeRestores {
		knownVolumes[hex.EncodeToString(entry.RecoveryKeySha256)] = entry
	}
	for _, actual := range physical.VolumeRestores {
		expected := knownVolumes[hex.EncodeToString(actual.RecoveryKeySha256)]
		if expected == nil {
			return invalidAgentStaging()
		}
		if proto.Equal(actual, expected) {
			continue
		}
		cleanup := false
		if plan != nil {
			for _, disposition := range plan.VolumeDispositions {
				if bytes.Equal(disposition.RecoveryKeySha256, actual.RecoveryKeySha256) &&
					disposition.GetCleanup() != nil {
					cleanup = true
					break
				}
			}
		}
		if !cleanup || !volumeInventoryCleanupSubset(actual, expected) {
			return invalidAgentStaging()
		}
	}
	return nil
}

func (state *backupStagingState) applyPlan(
	ctx context.Context,
	process [16]byte,
	plan *agentpb.BackupStagingRecoveryPlan,
) (*agentpb.BackupStagingRecoveryAck, error) {
	if state.ready {
		return nil, invalidAgentStaging()
	}
	if err := executionplan.ValidateBackupStagingRecoveryPlan(state.inventory, plan); err != nil {
		return nil, err
	}
	if err := state.journal.RecordPlan(ctx, process, state.inventory, plan); err != nil {
		return nil, err
	}
	for _, disposition := range plan.Dispositions {
		if err := inspectDatabaseStaging(ctx, disposition); err != nil {
			return nil, err
		}
	}
	planSHA, err := executionplan.BackupStagingRecoveryPlanSHA256(plan)
	if err != nil {
		return nil, err
	}
	for _, disposition := range plan.Dispositions {
		key := hex.EncodeToString(disposition.RecoveryKeySha256)
		entry, exists := state.physical[key]
		dispositionID := hex.EncodeToString(planSHA) + "/" + key
		if disposition.GetDiscardRecovered() != nil {
			if exists {
				if err := state.recovery.DiscardRecovered(ctx, entry.RecoveryID, dispositionID); err != nil {
					return nil, err
				}
			}
			// The marker is last: source cleanup cannot hide a helper whose
			// retirement was interrupted. Exact replay also permits absence.
			if err := retireDatabaseExecutionMarker(ctx, disposition); err != nil {
				return nil, err
			}
			continue
		}
		if !exists && disposition.DatabaseGuard != nil && disposition.GetRecoveryRequired() != nil &&
			len(disposition.GetRecoveryRequired().ExpectedFiles) == 0 {
			continue // Native completed source cleanup; keep the execution marker.
		}
		if !exists {
			return nil, invalidAgentStaging()
		}
		if required := disposition.GetRecoveryRequired(); required != nil {
			files := append([]backupstage.ArtifactEvidence(nil), entry.Files...)
			// The shared storage owner independently hashes the retained bytes.
			// NoGrowth retains their lease, without admitting a worker or delete.
			if err := state.recovery.ResumePrepared(ctx, backupstage.ResumeDisposition{
				RecoveryID: entry.RecoveryID, DispositionID: dispositionID,
				ExpectedFiles: files, RemainingGrowth: backupstage.NoGrowth(),
			}); err != nil {
				return nil, err
			}
			state.holdIDs[entry.IDs] = true
			continue
		}
		resume := disposition.GetResumePrepared()
		if resume == nil {
			return nil, invalidAgentStaging()
		}
		growth := backupstage.NoGrowth()
		switch resume.RemainingGrowth {
		case agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_BOUNDED:
			growth = backupstage.Bounded(resume.RequiredGrowthBytes)
		case agentpb.BackupRemainingGrowth_BACKUP_REMAINING_GROWTH_EXCLUSIVE_UNKNOWN:
			growth = backupstage.ExclusiveUnknown()
		}
		files := make([]backupstage.ArtifactEvidence, 0, len(entry.Files))
		for _, file := range entry.Files {
			role := backupRecoveredFileRole(file.Name)
			matched := false
			for _, expected := range resume.ExpectedFiles {
				if expected.Role == role && expected.SizeBytes == file.Size &&
					bytes.Equal(expected.Sha256, file.SHA256[:]) {
					matched = true
					break
				}
			}
			if !matched {
				return nil, invalidAgentStaging()
			}
			files = append(files, file)
		}
		restartEncryption := resume.RestartConfigEncryption != nil || resume.RestartDatabaseEncryption != nil
		if len(files) != len(resume.ExpectedFiles) && !(restartEncryption && len(files) == 1 &&
			backupRecoveredFileRole(files[0].Name) == agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_SOURCE_PLAINTEXT) {
			return nil, invalidAgentStaging()
		}
		if err := state.recovery.ResumePrepared(ctx, backupstage.ResumeDisposition{RecoveryID: entry.RecoveryID, DispositionID: dispositionID,
			ExpectedFiles: files, RemainingGrowth: growth}); err != nil {
			return nil, err
		}
		if restartEncryption {
			for _, file := range files {
				if backupRecoveredFileRole(
					file.Name,
				) == agentpb.BackupRecoveredFileRole_BACKUP_RECOVERED_FILE_ROLE_STORED_OBJECT {
					if err := state.recovery.DiscardResumedFile(ctx, entry.RecoveryID, file); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	if err := state.applyVolumeRecoveryPlan(ctx, plan); err != nil {
		return nil, err
	}
	ack := &agentpb.BackupStagingRecoveryAck{
		InventorySha256:         append([]byte(nil), plan.InventorySha256...),
		AppliedPlanSha256:       planSHA,
		AppliedDispositionCount: uint32(len(plan.Dispositions) + len(plan.VolumeDispositions)),
	}
	if err := state.journal.MarkApplied(ctx, ack); err != nil {
		return nil, err
	}
	state.plan = proto.CloneOf(plan)
	return ack, nil
}

func (state *backupStagingState) acceptReceipt(
	ctx context.Context,
	receipt *agentpb.BackupStagingRecoveryAckReceipt,
) error {
	if err := state.journal.AcceptReceipt(ctx, receipt); err != nil {
		return err
	}
	if state.ready {
		return nil
	}
	stager, prepared, err := state.recovery.Complete(ctx)
	if err != nil {
		return err
	}
	state.stager, state.ready = stager, true
	for _, entry := range prepared {
		if state.holdIDs[entry.IDs] {
			state.held[entry.IDs] = entry.Stage
		} else {
			state.prepared = append(state.prepared, entry)
		}
	}
	return nil
}

func (state *backupStagingState) close(ctx context.Context) error {
	if state == nil {
		return nil
	}
	if !state.ready {
		return state.recovery.Close(ctx)
	}
	var result error
	for _, prepared := range state.prepared {
		result = errors.Join(result, prepared.Stage.Suspend(ctx))
	}
	for _, stage := range state.active {
		result = errors.Join(result, stage.Suspend(ctx))
	}
	for _, stage := range state.held {
		result = errors.Join(result, stage.Suspend(ctx))
	}
	result = errors.Join(result, state.stager.Close(ctx))
	if result == nil {
		return nil
	}
	return errs.Wrap(errs.KindStorageUnavailable, result)
}

func invalidAgentStaging() error {
	return errs.New(errs.KindStateConflict, "Agent staging differs from the exact retained recovery authority")
}
