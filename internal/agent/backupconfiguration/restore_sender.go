package backupconfiguration

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/internal/infra/agentconfigjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SendRestore joins the authenticated artifact sender to the assignment inbox
// and private journal. Persist every descriptor before sending and every native
// grant before using it; reconnect reconstructs bytes rather than recapturing.
func (artifact *RestoreArtifact) SendRestore(ctx context.Context, stream *RestoreStream,
	journal *agentconfigjournal.Journal,
) (*agentpb.BackupConfigTransferCompleted, error) {
	if stream == nil || journal == nil {
		return nil, invalidRestoreArtifact()
	}
	credits, err := journal.ReadCredits(ctx)
	if err != nil {
		return nil, err
	}
	if len(credits) == 0 {
		if stream.initial.CreditSequence != 1 || stream.initial.CommittedRecordSequence != 0 {
			return nil, invalidRestoreCredit()
		}
		if err := journal.StoreCredit(ctx, stream.initial); err != nil {
			return nil, err
		}
		credits = []*agentpb.BackupConfigCredit{stream.initial}
	}
	transport := &restoreJournalTransport{stream: stream, journal: journal}
	return artifact.StreamRestore(ctx, stream.binding, transport, credits, stream.initial)
}

type restoreJournalTransport struct {
	stream  *RestoreStream
	journal *agentconfigjournal.Journal
}

func (transport *restoreJournalTransport) SendFrame(ctx context.Context, frame *agentpb.BackupConfigTransfer) error {
	descriptor, err := backupconfigtransfer.DescribeRecord(transport.stream.binding, frame)
	if err != nil {
		return err
	}
	if err := transport.journal.AppendRecord(ctx, descriptor); err != nil {
		return err
	}
	return transport.stream.publish(ctx, frame)
}

func (transport *restoreJournalTransport) ReadCredit(ctx context.Context) (*agentpb.BackupConfigCredit, error) {
	return transport.stream.ReadCredit(ctx)
}

func (transport *restoreJournalTransport) CommitCredit(ctx context.Context, credit *agentpb.BackupConfigCredit) error {
	return transport.journal.StoreCredit(ctx, credit)
}
