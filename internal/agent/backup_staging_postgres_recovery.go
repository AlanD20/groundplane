package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The complete helper namespace must be classified before staging cleanup or
// Ready. A known apply nonce alone would miss interrupted probes and list passes.
func inspectDatabaseStaging(ctx context.Context, disposition *agentpb.BackupStagingDisposition) error {
	guard := disposition.DatabaseGuard
	if guard == nil {
		return nil
	}
	if guard.Step.GetCapture().GetPostgres() != nil || guard.Step.GetRestore().GetPostgres() != nil {
		return inspectPostgresStaging(ctx, disposition, guard)
	}
	if guard.Step.GetCapture().GetMysql() != nil || guard.Step.GetRestore().GetMysql() != nil {
		return inspectMySQLStaging(ctx, disposition, guard)
	}
	return invalidAgentStaging()
}

func inspectMySQLStaging(ctx context.Context, disposition *agentpb.BackupStagingDisposition,
	guard *agentpb.BackupDatabaseStagingGuard,
) (resultErr error) {
	if guard.CompletedTask {
		return nil
	}
	authority, err := mysqlSourceAuthority(guard.Step,
		[]*agentpb.BackupServiceFact{guard.DatabaseService}, []*agentpb.ComposeArtifact{guard.DatabaseArtifact})
	if err != nil {
		return err
	}
	executor, err := mysql84execution.New(authority.selection, nil)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := executor.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
		}
	}()
	container, err := executor.ResolveContainer(ctx)
	if err != nil {
		return err
	}
	var heldNonce mysql84protocol.Nonce
	if guard.GetMysqlRecoveryApply() != nil {
		request, containerID, _, err := mysqlOriginalRequest(guard.Step, nil, guard.GetMysqlRecoveryApply())
		if err != nil || container.ID != containerID || disposition.GetRecoveryRequired() == nil {
			return invalidAgentStaging()
		}
		heldNonce = request.Nonce
	}
	inventory, err := executor.InspectRecoveryInventory(ctx, container)
	if err != nil {
		return err
	}
	heldFound := heldNonce == (mysql84protocol.Nonce{})
	for _, record := range inventory {
		if record.Nonce == heldNonce {
			if record.Operation != mysql84protocol.OperationRestoreApply {
				return invalidAgentStaging()
			}
			heldFound = true
			continue
		}
		if record.Active {
			return errs.New(errs.KindStateConflict, "MySQL helper has an unclassified retained execution")
		}
	}
	if !heldFound {
		return errs.New(errs.KindStateConflict, "MySQL held execution evidence is missing")
	}
	if guard.TerminalCleanup {
		for _, record := range inventory {
			if record.Nonce == heldNonce {
				continue
			}
			if err := executor.RetireInventoriedExecution(ctx, container, record); err != nil {
				return err
			}
		}
	}
	return nil
}

func inspectPostgresStaging(ctx context.Context, disposition *agentpb.BackupStagingDisposition,
	guard *agentpb.BackupDatabaseStagingGuard,
) (resultErr error) {
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
	defer func() {
		if closeErr := executor.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindInternal, resultErr, closeErr)
		}
	}()
	container, err := executor.ResolveContainer(ctx, authority.selection)
	if err != nil {
		return err
	}
	var heldNonce postgres16protocol.Nonce
	var heldDigest postgres16protocol.Digest
	if guard.GetPostgresRecoveryApply() != nil {
		request, containerID, _, err := postgresOriginalRequest(guard.Step, nil, guard.GetPostgresRecoveryApply())
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
