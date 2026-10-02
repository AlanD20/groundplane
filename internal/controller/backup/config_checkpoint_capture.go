package backup

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// validateConfigCaptureCompletion proves the Agent's completion against both
// the complete native credit history and a fresh replay of the Controller's
// pinned source. The returned cursor is compared atomically with Run progress.
func (service *BackupCheckpointService) validateConfigCaptureCompletion(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	claim etcdstore.Versioned[taskassignments.TaskAssignmentRecord],
	run etcdstore.Versioned[backupruntime.BackupRunRecord],
	ordinal int,
	request *agentpb.BackupCheckpointRequest,
) (etcdstore.Versioned[backupconfiguration.ConfigTransferCursor], error) {
	var zero etcdstore.Versioned[backupconfiguration.ConfigTransferCursor]
	completed := request.GetConfig().GetTransferCompleted()
	assignment := claim.Record
	if completed == nil || completed.RestoreGenerationId != "" || completed.RenderGeneration != 0 ||
		ordinal < 0 || ordinal >= len(run.Record.Sources) || assignment.BackupAuthorityFence == nil {
		return zero, invalidConfigCaptureCompletion()
	}
	plan, err := service.repository.GetBackupExecutionPlan(ctx, request.TaskId)
	if err != nil {
		return zero, err
	}
	authority, authorityDigest, err := executionplan.BindBackupTaskAuthority(
		plan.Record,
		executionplan.BackupAssignmentIdentity{
			TaskID: request.TaskId, OperationID: run.Record.OperationID, AssignmentID: request.AssignmentId,
			Generation:       assignment.BackupAuthorityFence.AssignmentGeneration,
			DeadlineUnixNano: uint64(assignment.Deadline.UnixNano()),
		},
	)
	if err != nil {
		return zero, err
	}
	if hex.EncodeToString(authorityDigest) != assignment.BackupAuthorityFence.AuthoritySHA256 ||
		ordinal >= len(authority.Steps) {
		return zero, invalidConfigCaptureCompletion()
	}
	step := authority.Steps[ordinal]
	capture := step.GetCapture().GetConfig()
	source := run.Record.Sources[ordinal]
	if step.StepId != request.StepId || step.ExecutionId != request.ExecutionId || capture == nil ||
		source.Kind != backupruntime.BackupRuntimeSourceConfig || source.Snapshot.Config == nil ||
		!proto.Equal(completed.Content, capture.Content) {
		return zero, invalidConfigCaptureCompletion()
	}
	binding := backupconfigtransfer.Binding{
		TaskID: authority.TaskId, AssignmentID: authority.AssignmentId, StepID: step.StepId,
		ExecutionID: step.ExecutionId, TransferID: step.ExecutionId,
		Direction: agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE,
	}
	cursor, found, err := service.repository.ReadConfigTransferCursor(
		ctx,
		authority.TaskId,
		authority.AssignmentId,
		step.StepId,
		step.ExecutionId,
		0,
	)
	if err != nil {
		return zero, err
	}
	owner := cursor.Record.Owner
	if !found || owner.Binding != binding || owner.AgentID != agentID ||
		owner.AgentGeneration != agentGeneration ||
		owner.AssignmentGeneration != authority.AssignmentGeneration ||
		owner.AuthoritySHA256 != hex.EncodeToString(step.StepDigest) ||
		cursor.Record.MetadataAcceptedCreditSequence == 0 {
		return zero, invalidConfigCaptureCompletion()
	}
	var credits []*agentpb.BackupConfigCredit
	err = service.repository.VisitConfigTransferCredits(ctx, cursor, func(credit *agentpb.BackupConfigCredit) error {
		credits = append(credits, credit)
		return nil
	})
	if err != nil {
		return zero, err
	}
	if len(credits) == 0 {
		return zero, invalidConfigCaptureCompletion()
	}
	final := credits[len(credits)-1]
	if final.GetValueCredit() == nil ||
		final.CreditSequence != cursor.Record.LastCreditSequence ||
		final.CommittedRecordSequence != completed.CommittedRecordCount ||
		final.NextOrdinal != capture.Content.EntryCount+1 ||
		!bytes.Equal(final.CumulativeChainSha256, completed.ValueChainSha256) {
		return zero, invalidConfigCaptureCompletion()
	}
	snapshot, err := service.OpenBackupConfigCapture(ctx, agentID, agentGeneration, authority, step.StepId)
	if err != nil {
		return zero, err
	}
	transcript, err := snapshot.StreamCapture(
		ctx,
		binding,
		configCaptureProofTransport{},
		credits,
		final,
	)
	if err != nil {
		return zero, err
	}
	if !bytes.Equal(completed.TransferTranscriptSha256, transcript[:]) {
		return zero, invalidConfigCaptureCompletion()
	}
	return cursor, nil
}

// configCaptureProofTransport makes canonical source replay observational.
// A complete native history must consume no new channel I/O or credit writes.
type configCaptureProofTransport struct{}

func (configCaptureProofTransport) SendFrame(context.Context, *agentpb.BackupConfigTransfer) error {
	return invalidConfigCaptureCompletion()
}

func (configCaptureProofTransport) ReadCredit(context.Context) (*agentpb.BackupConfigCredit, error) {
	return nil, invalidConfigCaptureCompletion()
}

func (configCaptureProofTransport) CommitCredit(context.Context, *agentpb.BackupConfigCredit) error {
	return invalidConfigCaptureCompletion()
}

func invalidConfigCaptureCompletion() error {
	return errs.New(errs.KindStateConflict, "Config capture completion differs from its sealed source or native cursor")
}
