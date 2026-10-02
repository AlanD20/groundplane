package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The complete helper namespace must be classified before staging cleanup or
// Ready. A known apply nonce alone would miss interrupted probes and list passes.
func inspectPostgresStaging(ctx context.Context, disposition *agentpb.BackupStagingDisposition) (resultErr error) {
	guard := disposition.PostgresGuard
	if guard == nil {
		return nil
	}
	if guard.CompletedTask {
		// Completion follows helper retirement. Inspecting the old container
		// here could touch a newer operation, or a legitimately replaced DB.
		return nil
	}
	authority, err := postgresSourceAuthority(guard.Step,
		[]*agentpb.BackupServiceFact{guard.DatabaseService}, []*agentpb.ComposeArtifact{guard.DatabaseArtifact})
	if err != nil {
		return err
	}
	executor, err := postgres16execution.New(authority.index, nativePostgresArchitecture(), nil)
	if err != nil {
		return err
	}
	defer func() { resultErr = errs.WrapJoined(errs.KindInternal, resultErr, executor.Close()) }()
	container, err := executor.ResolveContainer(ctx, authority.selection)
	if err != nil {
		return err
	}
	var heldNonce postgres16protocol.Nonce
	var heldDigest postgres16protocol.Digest
	if guard.RecoveryApply != nil {
		request, containerID, _, err := postgresOriginalRequest(guard.Step, nil, guard.RecoveryApply)
		if err != nil || container.ID != containerID || disposition.GetRecoveryRequired() == nil {
			return invalidAgentStaging()
		}
		heldNonce = request.Nonce
		heldDigest, err = postgres16protocol.ExecutionRequestSHA256(request)
		if err != nil {
			return err
		}
	}
	inventory, err := executor.InspectRecoveryInventory(ctx, container)
	if err != nil {
		return err
	}
	heldFound := heldNonce == (postgres16protocol.Nonce{})
	for _, record := range inventory.Records {
		if record.Nonce == heldNonce {
			if record.RequestSHA256 != heldDigest || record.Operation != postgres16protocol.OperationRestoreApply {
				return invalidAgentStaging()
			}
			heldFound = true
		}
		if record.Retirable() {
			continue
		}
		// An explicit native hold permits unrelated work, never another apply.
		// It cannot classify an extra nonce or a process not proved quiescent.
		if heldNonce == (postgres16protocol.Nonce{}) || record.Nonce != heldNonce ||
			record.RequestSHA256 != heldDigest ||
			record.Operation != postgres16protocol.OperationRestoreApply ||
			(record.Phase != postgres16protocol.ConfinementPhaseRecoveryRetired &&
				(record.Phase != postgres16protocol.ConfinementPhaseReaped || !record.TerminalWait4Reaped)) {
			return errs.New(errs.KindStateConflict, "PostgreSQL helper has an unclassified retained execution")
		}
	}
	if !heldFound {
		return errs.New(errs.KindStateConflict, "PostgreSQL held execution evidence is missing")
	}
	if guard.TerminalCleanup {
		// Classify the entire namespace before deleting any evidence. Resume
		// and held stages retain their proof for the original worker/incident.
		for _, record := range inventory.Records {
			if err := executor.RetireInventoriedExecution(ctx, container, record); err != nil {
				return err
			}
		}
	}
	return nil
}
