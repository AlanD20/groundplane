package agent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func newMySQLRequest(operation mysql84protocol.Operation, step *agentpb.BackupStepAuthority,
	database, role string, maximum, size uint64, digest mysql84protocol.Digest,
) (mysql84protocol.Request, error) {
	nonce, err := newMySQLNonce()
	if err != nil {
		return mysql84protocol.Request{}, err
	}
	request := mysql84protocol.Request{Operation: operation, Nonce: nonce,
		DeadlineUnixNano: step.StepDeadlineUnixNano, Database: database, Role: role,
		MaximumBytes: maximum, SourceSize: size, SourceSHA256: digest}
	return request, request.Validate()
}

func mysqlOriginalRequest(step *agentpb.BackupStepAuthority, dump *agentpb.BackupMySQLDumpStart,
	apply *agentpb.BackupMySQLRestoreApplyStartCheckpoint,
) (mysql84protocol.Request, string, string, error) {
	request := mysql84protocol.Request{DeadlineUnixNano: step.StepDeadlineUnixNano}
	var containerID, execID string
	if capture := step.GetCapture().GetMysql(); capture != nil && dump != nil && apply == nil {
		if len(dump.ExecutionNonce) != len(request.Nonce) || dump.PointId != step.GetCapture().PointId ||
			dump.DatabaseName != capture.DatabaseName || dump.RoleName != capture.RoleName {
			return request, "", "", invalidAgentStaging()
		}
		request.Operation, request.Database, request.Role = mysql84protocol.OperationDump,
			capture.DatabaseName, capture.RoleName
		request.MaximumBytes = capture.MaxPlaintextBytes
		copy(request.Nonce[:], dump.ExecutionNonce)
		containerID, execID = dump.ContainerId, dump.ExecId
	} else if restore := step.GetRestore().GetMysql(); restore != nil && apply != nil && dump == nil {
		if len(apply.ExecutionNonce) != len(request.Nonce) || apply.PointId != step.GetRestore().PointId ||
			len(apply.SourceSha256) != len(request.SourceSHA256) {
			return request, "", "", invalidAgentStaging()
		}
		request.Operation, request.Database, request.Role = mysql84protocol.OperationRestoreApply,
			restore.DatabaseName, restore.RoleName
		request.SourceSize = apply.SourceSizeBytes
		copy(request.Nonce[:], apply.ExecutionNonce)
		copy(request.SourceSHA256[:], apply.SourceSha256)
		containerID, execID = apply.ContainerId, apply.ExecId
	} else {
		return request, "", "", invalidAgentStaging()
	}
	return request, containerID, execID, request.Validate()
}

func (pool *WorkerPool) retireMySQLExecution(ctx context.Context, assignment taskassignment.Assignment,
	step *agentpb.BackupStepAuthority, dump *agentpb.BackupMySQLDumpStart,
	apply *agentpb.BackupMySQLRestoreApplyStartCheckpoint,
) (resultErr error) {
	request, containerID, _, err := mysqlOriginalRequest(step, dump, apply)
	if err != nil {
		return err
	}
	authority, err := mysqlAuthority(assignment, step)
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
	if err != nil || container.ID != containerID {
		return errs.New(errs.KindStateConflict, "MySQL helper retirement container changed")
	}
	return executor.RetireExecution(ctx, container, request)
}
