package backupconfigtransfer

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func streamReplayCredits(
	binding Binding,
	native []*agentpb.BackupConfigCredit,
	receiver *agentpb.BackupConfigCredit,
) ([]*agentpb.BackupConfigCredit, error) {
	if len(native) == 0 {
		return nil, invalidStream()
	}
	owned := make([]*agentpb.BackupConfigCredit, 0, len(native)+1)
	for index, credit := range native {
		validated, err := ValidateCredit(binding, credit)
		if err != nil {
			return nil, err
		}
		if validated.CreditSequence != uint64(index+1) ||
			(index == 0 && (validated.CommittedRecordSequence != 0 || validated.GetMetadataCredit() == nil)) ||
			(index > 0 && validated.CommittedRecordSequence <= owned[index-1].CommittedRecordSequence) {
			return nil, invalidStream()
		}
		owned = append(owned, validated)
	}
	if receiver == nil {
		return nil, invalidStream()
	}
	validated, err := ValidateCredit(binding, receiver)
	if err != nil {
		return nil, err
	}
	last := owned[len(owned)-1]
	if proto.Equal(last, validated) {
		return owned, nil
	}
	// Stop-and-wait delivery can lose only the next receiver grant: the sender
	// persists every grant before using it to emit subsequent records.
	if validated.CreditSequence != last.CreditSequence+1 ||
		validated.CommittedRecordSequence <= last.CommittedRecordSequence {
		return nil, invalidStream()
	}
	return append(owned, validated), nil
}

func (sender *streamSender) sendFrame(ctx context.Context, frame *agentpb.BackupConfigTransfer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := sender.window.Send(frame); err != nil {
		return err
	}
	if frame.RecordSequence > sender.replayThrough {
		if sender.replayIndex != len(sender.replayCredits) {
			return invalidStream()
		}
		return sender.transport.SendFrame(ctx, frame)
	}
	if sender.replayIndex < len(sender.replayCredits) {
		credit := sender.replayCredits[sender.replayIndex]
		if credit.CommittedRecordSequence < frame.RecordSequence {
			return invalidStream()
		}
		if credit.CommittedRecordSequence == frame.RecordSequence {
			if err := sender.applyCredit(ctx, credit, sender.replayIndex >= sender.nativeCredits); err != nil {
				return err
			}
			sender.replayIndex++
		}
	}
	return nil
}
