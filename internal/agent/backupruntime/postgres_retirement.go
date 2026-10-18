package backupruntime

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func postgresOriginalRequest(step *agentpb.BackupStepAuthority,
	dump *agentpb.BackupPostgresDumpStart, apply *agentpb.BackupPostgresRestoreApplyStartCheckpoint,
) (postgres16protocol.Request, string, string, error) {
	request := postgres16protocol.Request{DeadlineUnixNano: step.StepDeadlineUnixNano}
	var containerID, execID string
	if capture := step.GetCapture().GetPostgres(); capture != nil && dump != nil && apply == nil {
		if len(dump.ExecutionNonce) != len(request.Nonce) || dump.PointId != step.GetCapture().PointId ||
			dump.DatabaseName != capture.DatabaseName || dump.RoleName != capture.RoleName {
			return request, "", "", invalidDatabaseStaging()
		}
		request.Operation, request.Database, request.Role = postgres16protocol.OperationDump, capture.DatabaseName, capture.RoleName
		copy(request.Nonce[:], dump.ExecutionNonce)
		containerID, execID = dump.ContainerId, dump.ExecId
	} else if restore := step.GetRestore().GetPostgres(); restore != nil && apply != nil && dump == nil {
		if len(apply.ExecutionNonce) != len(request.Nonce) || apply.PointId != step.GetRestore().PointId ||
			len(apply.SourceSha256) != len(request.SourceSHA256) {
			return request, "", "", invalidDatabaseStaging()
		}
		request.Operation, request.Database, request.Role = postgres16protocol.OperationRestoreApply, restore.DatabaseName, restore.RoleName
		request.SourceSize = apply.SourceSizeBytes
		copy(request.Nonce[:], apply.ExecutionNonce)
		copy(request.SourceSHA256[:], apply.SourceSha256)
		containerID, execID = apply.ContainerId, apply.ExecId
	} else {
		return request, "", "", invalidDatabaseStaging()
	}
	return request, containerID, execID, request.Validate()
}

// This is called only after the source-cleanup checkpoint is acknowledged.
// The retained start keeps retirement bound across all later resume phases.
func retirePostgresExecution(ctx context.Context, assignment *agentpb.BackupWorkAssignment,
	identity workexecution.Identity, ordinal uint32, publisher *checkpointdelivery.BackupSource, dump *agentpb.BackupPostgresDumpStart,
	apply *agentpb.BackupPostgresRestoreApplyStartCheckpoint,
) (resultErr error) {
	cursor, err := publisher.Current(ctx)
	if err != nil {
		return err
	}
	if cursor.GetCapture().GetSourceCleanupCompleted() == nil && cursor.GetRestore().GetSourceCleanupCompleted() == nil {
		return errs.New(errs.KindStateConflict, "Database helper retirement requires acknowledged source cleanup")
	}
	if uint64(ordinal) >= uint64(len(assignment.Input.Steps)) {
		return invalidDatabaseStaging()
	}
	step := assignment.Input.Steps[ordinal]
	request, containerID, _, err := postgresOriginalRequest(step, dump, apply)
	if err != nil {
		return err
	}
	authority, err := postgresAuthority(assignment, identity, ordinal)
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
	if err != nil || container.ID != containerID {
		return errs.New(errs.KindStateConflict, "PostgreSQL helper retirement container changed")
	}
	return executor.RetireExecution(ctx, container, request)
}
