package backupconfiguration

import (
	"context"
	"io"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/internal/infra/backupstage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CaptureTransport delivers bounded records and durably confirms receiver
// credits. CommitCredit returns only after the Controller echoes the exact
// stored credit, not merely after the Agent writes it to a disconnected stream.
type CaptureTransport interface {
	ReadFrame(context.Context) (*agentpb.BackupConfigTransfer, error)
	CommitCredit(context.Context, *agentpb.BackupConfigCredit) error
}

// Capture writes only the sealed Config source. The caller owns encryption,
// object upload, checkpoints and eventual cleanup; this is not point publication.
func Capture(
	ctx context.Context,
	binding backupconfigtransfer.Binding,
	expected *agentpb.BackupConfigContentAuthority,
	source *backupstage.Artifact,
	retained backupstage.ArtifactEvidence,
	journal *agentconfigjournal.Journal,
	transport CaptureTransport,
) (_ backupconfigtransfer.ReceiverProgress, resultErr error) {
	if ctx == nil || source == nil || journal == nil || transport == nil ||
		binding.Direction != agentpb.BackupConfigDirection_BACKUP_CONFIG_DIRECTION_CAPTURE {
		return backupconfigtransfer.ReceiverProgress{}, invalidCapture()
	}
	credits, err := journal.ReadCredits(ctx)
	if err != nil {
		return backupconfigtransfer.ReceiverProgress{}, err
	}
	if len(credits) == 0 {
		initial, err := backupconfigtransfer.InitialCredit(binding)
		if err != nil {
			return backupconfigtransfer.ReceiverProgress{}, err
		}
		if err := journal.StoreCredit(ctx, initial); err != nil {
			return backupconfigtransfer.ReceiverProgress{}, err
		}
		credits = []*agentpb.BackupConfigCredit{initial}
	}
	prefix, err := source.OpenPrefix(ctx)
	if err != nil {
		return backupconfigtransfer.ReceiverProgress{}, err
	}
	defer func() {
		if closeErr := prefix.Close(); closeErr != nil {
			resultErr = errs.WrapJoined(errs.KindStorageUnavailable, resultErr, closeErr)
		}
	}()
	writer := &stageSourceWriter{ctx: ctx, artifact: source}
	receiver, err := backupconfigtransfer.RecoverReceiver(ctx, binding, expected, credits, journal, prefix,
		backupconfig.ArtifactEvidence{SizeBytes: retained.Size, SHA256: retained.SHA256}, writer)
	if err != nil {
		return backupconfigtransfer.ReceiverProgress{}, err
	}
	defer receiver.Abort() // Releases in-memory hash state; never removes staging.
	if err := transport.CommitCredit(ctx, credits[len(credits)-1]); err != nil {
		return backupconfigtransfer.ReceiverProgress{}, err
	}
	pending := make([]*backupconfigtransfer.RecordDescriptor, 0, backupconfigtransfer.MetadataCreditRecords)
	for !receiver.Progress().Complete {
		frame, err := transport.ReadFrame(ctx)
		if err != nil {
			return backupconfigtransfer.ReceiverProgress{}, err
		}
		if frame.GetResume() != nil {
			backupconfigtransfer.ClearFrame(frame)
			return backupconfigtransfer.ReceiverProgress{}, invalidCapture()
		}
		err = receiver.AcceptFrame(ctx, frame)
		var descriptor *backupconfigtransfer.RecordDescriptor
		if err == nil {
			descriptor, err = backupconfigtransfer.DescribeRecord(binding, frame)
		}
		backupconfigtransfer.ClearFrame(frame)
		if err != nil {
			return backupconfigtransfer.ReceiverProgress{}, err
		}
		pending = append(pending, descriptor)
		credit, err := receiver.NextCredit()
		if err != nil {
			return backupconfigtransfer.ReceiverProgress{}, err
		}
		if credit == nil {
			continue
		}
		if retained.Name != executionplan.BackupSourceStagingFinal {
			if _, err := source.SyncPartial(ctx); err != nil {
				return backupconfigtransfer.ReceiverProgress{}, err
			}
		}
		for _, descriptor := range pending {
			if err := journal.AppendRecord(ctx, descriptor); err != nil {
				return backupconfigtransfer.ReceiverProgress{}, err
			}
		}
		if err := journal.StoreCredit(ctx, credit); err != nil {
			return backupconfigtransfer.ReceiverProgress{}, err
		}
		if err := transport.CommitCredit(ctx, credit); err != nil {
			return backupconfigtransfer.ReceiverProgress{}, err
		}
		if err := receiver.AcceptPersistedCredit(credit); err != nil {
			return backupconfigtransfer.ReceiverProgress{}, err
		}
		clear(pending)
		pending = pending[:0]
	}
	progress := receiver.Progress()
	physical, err := source.Publish(ctx)
	if err != nil {
		return backupconfigtransfer.ReceiverProgress{}, err
	}
	if physical.Size != progress.Source.SizeBytes || physical.SHA256 != progress.Source.SHA256 {
		return backupconfigtransfer.ReceiverProgress{}, invalidCapture()
	}
	return progress, nil
}

type stageSourceWriter struct {
	ctx      context.Context
	artifact *backupstage.Artifact
}

func (writer *stageSourceWriter) Write(content []byte) (int, error) {
	count, err := writer.artifact.Write(writer.ctx, content)
	if err == nil && count != len(content) {
		err = io.ErrShortWrite
	}
	return count, err
}

func invalidCapture() error {
	return errs.New(errs.KindStateConflict, "Config capture differs from its sealed source or durable cursor")
}
