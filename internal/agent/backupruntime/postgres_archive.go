package backupruntime

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type postgresArtifactWriter struct {
	ctx      context.Context
	artifact *backupstage.Artifact
	limit    uint64
	written  uint64
}

func (writer *postgresArtifactWriter) Write(content []byte) (int, error) {
	if uint64(len(content)) > writer.limit-writer.written {
		return 0, invalidDatabaseStaging()
	}
	n, err := writer.artifact.Write(writer.ctx, content)
	writer.written += uint64(n)
	return n, err
}

func (*postgresArtifactWriter) Close() error { return nil }

func newPostgresRequest(operation postgres16protocol.Operation, step *agentpb.BackupStepAuthority,
	database, role string, sourceSize uint64, sourceSHA postgres16protocol.Digest,
) (postgres16protocol.Request, error) {
	nonce, err := newPostgresNonce()
	if err != nil {
		return postgres16protocol.Request{}, err
	}
	request := postgres16protocol.Request{Operation: operation, Nonce: nonce,
		DeadlineUnixNano: step.StepDeadlineUnixNano, Database: database, Role: role,
		SourceSize: sourceSize, SourceSHA256: sourceSHA}
	if err := request.Validate(); err != nil {
		return postgres16protocol.Request{}, err
	}
	return request, nil
}

func postgresListArchive(ctx context.Context, executor *postgres16execution.Executor,
	container postgres16execution.Container, step *agentpb.BackupStepAuthority,
	source *backupstage.Artifact, evidence backupstage.ArtifactEvidence,
) error {
	reader, err := source.OpenPrefix(ctx)
	if err != nil {
		return err
	}
	defer reader.Close()
	request, err := newPostgresRequest(postgres16protocol.OperationRestoreList, step,
		"", "", evidence.Size, postgres16protocol.Digest(evidence.SHA256))
	if err != nil {
		return err
	}
	result, err := executor.Execute(ctx, container, request, reader, nil)
	if err != nil || !result.Stdin.EOF || result.Stdin.Bytes != evidence.Size ||
		result.Stdin.SHA256 != evidence.SHA256 {
		return errs.New(errs.KindStateConflict, "PostgreSQL archive list proof is unavailable")
	}
	return nil
}
