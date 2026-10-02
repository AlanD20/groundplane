package agentconfigjournal

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupconfig"
	"github.com/AlanD20/groundplane/internal/common/backupconfigtransfer"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	creditSuffix       = ".credit"
	creditNextSuffix   = ".credit-next"
	maximumCreditBytes = backupconfig.MaxEntryHeaderEnvelopeBytes
)

// StoreCredit durably records a receiver-issued sealed credit before it is
// exposed to the peer or used to send further records. Only exact same-sequence
// replay is accepted.
func (journal *Journal) StoreCredit(ctx context.Context, credit *agentpb.BackupConfigCredit) error {
	if journal == nil {
		return invalidRecord()
	}
	owned, err := backupconfigtransfer.ValidateCredit(journal.binding, credit)
	if err != nil {
		return err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(owned)
	if err != nil || len(raw) == 0 || len(raw) > maximumCreditBytes {
		return invalidRecord()
	}
	sequence := owned.CreditSequence
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return err
	}
	if sequence == 0 || sequence > journal.maxRecords+1 {
		return invalidRecord()
	}
	if journal.retired {
		return conflict("Config journal has already reached native cleanup")
	}
	if sequence <= journal.creditCount {
		current, err := journal.readRawLimit(ctx, creditName(sequence), maximumCreditBytes)
		if err != nil {
			return err
		}
		if !bytes.Equal(current, raw) {
			return conflict("Agent Config journal credit sequence conflicts")
		}
		return fileError(journal.directory.Sync())
	}
	if sequence != journal.creditCount+1 || owned.CommittedRecordSequence > journal.recordCount {
		return conflict("Agent Config journal credit is not contiguous with durable records")
	}
	if err := validateNextCredit(
		owned,
		journal.creditCount,
		journal.lastCreditCommitted,
		journal.metadataAccepted,
		journal.recordCount,
	); err != nil {
		return err
	}
	if err := journal.writeImmutable(ctx, creditName(sequence), creditNextName(sequence), raw, maximumCreditBytes); err != nil {
		return err
	}
	journal.creditCount = sequence
	journal.lastCreditCommitted = owned.CommittedRecordSequence
	if owned.GetMetadataAccepted() != nil {
		journal.metadataAccepted = true
	}
	return nil
}

// ReadCredits returns owned receiver-issued history. Native commitment is
// independently proved by the Controller before it uses each grant.
func (journal *Journal) ReadCredits(ctx context.Context) ([]*agentpb.BackupConfigCredit, error) {
	if journal == nil {
		return nil, invalidRecord()
	}
	journal.mu.Lock()
	defer journal.mu.Unlock()
	if err := journal.available(ctx); err != nil {
		return nil, err
	}
	if journal.retired {
		return nil, conflict("Config journal has already reached native cleanup")
	}
	return journal.readCredits(ctx, journal.creditCount)
}

func (journal *Journal) readCredits(
	ctx context.Context,
	count uint64,
) ([]*agentpb.BackupConfigCredit, error) {
	credits := make([]*agentpb.BackupConfigCredit, 0, count)
	for sequence := uint64(1); sequence <= count; sequence++ {
		credit, err := journal.readCredit(ctx, creditName(sequence))
		if err != nil {
			return nil, err
		}
		credits = append(credits, credit)
	}
	return credits, nil
}

func (journal *Journal) readCredit(ctx context.Context, name string) (*agentpb.BackupConfigCredit, error) {
	raw, err := journal.readRawLimit(ctx, name, maximumCreditBytes)
	if err != nil {
		return nil, err
	}
	credit := &agentpb.BackupConfigCredit{}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(raw, credit); err != nil {
		return nil, invalidRecord()
	}
	owned, err := backupconfigtransfer.ValidateCredit(journal.binding, credit)
	if err != nil {
		return nil, err
	}
	canonical, err := proto.MarshalOptions{Deterministic: true}.Marshal(owned)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, invalidRecord()
	}
	return owned, nil
}

func validateCreditHistory(credits []*agentpb.BackupConfigCredit, recordCount uint64) error {
	metadataAccepted := false
	var committed uint64
	for index, credit := range credits {
		if err := validateNextCredit(credit, uint64(index), committed, metadataAccepted, recordCount); err != nil {
			return err
		}
		committed = credit.CommittedRecordSequence
		metadataAccepted = metadataAccepted || credit.GetMetadataAccepted() != nil
	}
	return nil
}

func validateNextCredit(
	credit *agentpb.BackupConfigCredit,
	creditCount uint64,
	committed uint64,
	metadataAccepted bool,
	recordCount uint64,
) error {
	if credit.CreditSequence != creditCount+1 || credit.CommittedRecordSequence > recordCount {
		return invalidRecord()
	}
	if creditCount > 0 && credit.CommittedRecordSequence <= committed {
		return conflict("Agent Config journal credit cursor is not strictly advancing")
	}
	switch credit.Credit.(type) {
	case *agentpb.BackupConfigCredit_MetadataCredit:
		if metadataAccepted {
			return conflict("Agent Config journal metadata credit follows acceptance")
		}
	case *agentpb.BackupConfigCredit_MetadataAccepted:
		if metadataAccepted {
			return conflict("Agent Config journal repeats metadata acceptance")
		}
	case *agentpb.BackupConfigCredit_ValueCredit:
		if !metadataAccepted {
			return conflict("Agent Config journal value credit precedes metadata acceptance")
		}
	default:
		return invalidRecord()
	}
	return nil
}

func creditName(sequence uint64) string {
	return fmt.Sprintf("%0*d%s", sequenceDigits, sequence, creditSuffix)
}

func creditNextName(sequence uint64) string {
	return fmt.Sprintf("%0*d%s", sequenceDigits, sequence, creditNextSuffix)
}

func parseCreditName(name string) (uint64, bool, bool) {
	committed := strings.HasSuffix(name, creditSuffix)
	suffix := creditSuffix
	if !committed {
		suffix = creditNextSuffix
		if !strings.HasSuffix(name, suffix) {
			return 0, false, false
		}
	}
	digits := strings.TrimSuffix(name, suffix)
	if len(digits) != sequenceDigits {
		return 0, false, false
	}
	sequence, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || creditName(sequence) != digits+creditSuffix {
		return 0, false, false
	}
	return sequence, committed, true
}
