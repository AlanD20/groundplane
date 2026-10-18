package backupruntime

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/agent/checkpointdelivery"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Inspect the spent execution before a read-only database verification. Neither
// executor has a start recorder, so this path cannot authorize another apply.
func provePostgresRecovery(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	resume *agentpb.BackupRestoreResume, publisher *checkpointdelivery.BackupSource,
) (resultErr error) {
	step := assignment.Input.Steps[0]
	authority, err := postgresSourceAuthority(step, assignment.Input.Services, assignment.Input.Artifacts)
	if err != nil {
		return err
	}
	original, containerID, execID, err := postgresOriginalRequest(step, nil, resume.ApplyStart)
	if err != nil {
		return err
	}
	executor, err := postgres16execution.New(authority.index, nativePostgresArchitecture(), nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := executor.Close(); err != nil {
			resultErr = errs.WrapJoined(errs.KindInternal, resultErr, err)
		}
	}()
	container, err := executor.ResolveContainer(ctx, authority.selection)
	if err != nil {
		return err
	}
	if container.ID != containerID {
		return invalidDatabaseStaging()
	}
	if _, err := executor.RecoverExecution(ctx, container, original, execID); err != nil {
		return err
	}
	nonce, err := newPostgresNonce()
	if err != nil {
		return err
	}
	result, err := executor.Execute(ctx, container, postgres16protocol.Request{
		Operation: postgres16protocol.OperationPostRestoreVerify, Nonce: nonce,
		DeadlineUnixNano: uint64(assignment.Attempt.Deadline.AsTime().UnixNano()), Database: authority.database,
	}, nil, nil)
	if err != nil || !bytes.Equal(bytes.TrimSpace(result.Proof), []byte(authority.database)) {
		return errs.New(errs.KindStateConflict, "PostgreSQL recovery could not verify the restored database")
	}
	digest := sha256.Sum256(result.Proof)
	return publisher.PostgresRestoreVerified(ctx, &agentpb.BackupPostgresRestoreVerified{
		PointId: step.GetRestore().PointId, ContainerId: container.ID,
		Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence), VerificationSha256: digest[:],
	})
}

func proveMySQLRecovery(ctx context.Context, assignment *agentpb.BackupRecoveryAssignment,
	resume *agentpb.BackupRestoreResume, publisher *checkpointdelivery.BackupSource,
) (resultErr error) {
	step := assignment.Input.Steps[0]
	authority, err := mysqlSourceAuthority(step, assignment.Input.Services, assignment.Input.Artifacts)
	if err != nil {
		return err
	}
	original, containerID, execID, err := mysqlOriginalRequest(step, nil, resume.MysqlApplyStart)
	if err != nil {
		return err
	}
	executor, err := mysql84execution.New(authority.selection, nil)
	if err != nil {
		return err
	}
	defer func() {
		if err := executor.Close(); err != nil {
			resultErr = errs.WrapJoined(errs.KindInternal, resultErr, err)
		}
	}()
	container, err := executor.ResolveContainer(ctx)
	if err != nil {
		return err
	}
	if container.ID != containerID {
		return invalidDatabaseStaging()
	}
	if _, err := executor.RecoverExecution(ctx, container, original, execID, nil); err != nil {
		return err
	}
	nonce, err := newMySQLNonce()
	if err != nil {
		return err
	}
	result, err := executor.Execute(ctx, container, mysql84protocol.Request{
		Operation: mysql84protocol.OperationPostRestoreVerify, Nonce: nonce,
		DeadlineUnixNano: uint64(assignment.Attempt.Deadline.AsTime().UnixNano()), Database: authority.database,
	}, nil, nil)
	proof := bytes.TrimSpace(result.Proof)
	if err != nil || !bytes.Equal(proof, []byte(authority.database)) {
		return errs.New(errs.KindStateConflict, "MySQL recovery could not verify the restored database")
	}
	digest := sha256.Sum256(proof)
	return publisher.MysqlRestoreVerified(ctx, &agentpb.BackupMySQLRestoreVerified{
		PointId: step.GetRestore().PointId, ContainerId: container.ID,
		Evidence: proto.CloneOf(step.GetRestore().ExpectedEvidence), VerificationSha256: digest[:],
	})
}
