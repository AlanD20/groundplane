package backupconfigtransfer

import (
	"crypto/sha256"
	"crypto/subtle"

	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type creditLane uint8

const (
	metadataLane creditLane = 1
	valueLane    creditLane = 2
)

type pendingRecord struct {
	sequence    uint64
	nextOrdinal uint32
	chain       [sha256.Size]byte
	lane        creditLane
	bytes       uint64
}

// Cursor is the exact receiver-committed transfer position.
type Cursor struct {
	RecordSequence uint64
	NextOrdinal    uint32
	ChainSHA256    [sha256.Size]byte
}

// SenderStatus exposes only bounded counters and digests. SenderWindow never
// retains a transfer frame or selected-value plaintext.
type SenderStatus struct {
	LastCreditSequence        uint64
	LastCreditSHA256          [sha256.Size]byte
	Committed                 Cursor
	LastSentRecordSequence    uint64
	SentNextOrdinal           uint32
	SentChainSHA256           [sha256.Size]byte
	MetadataAccepted          bool
	MetadataRecordCredit      uint32
	MetadataByteCredit        uint64
	ValueRecordCredit         uint32
	ValueByteCredit           uint64
	UnacknowledgedRecordCount uint32
}

// SenderWindow enforces credit and acknowledgement cursors for one transfer.
// The caller must durably persist a sealed credit before constructing or
// applying it; this in-memory owner supplies no persistence authority.
type SenderWindow struct {
	binding Binding
	status  SenderStatus
	pending []pendingRecord
}

// NewSenderWindow starts at the cursor and grant in one already-persisted
// sealed credit. Reconnect therefore restarts from only receiver-committed
// state and cannot retain an unacknowledged plaintext frame.
func NewSenderWindow(binding Binding, persistedCredit *agentpb.BackupConfigCredit) (*SenderWindow, error) {
	credit, err := ValidateCredit(binding, persistedCredit)
	if err != nil {
		return nil, err
	}
	window := &SenderWindow{binding: binding}
	window.setCommitted(credit)
	window.status.LastSentRecordSequence = credit.CommittedRecordSequence
	window.status.SentNextOrdinal = credit.NextOrdinal
	copy(window.status.SentChainSHA256[:], credit.CumulativeChainSha256)
	if _, ok := credit.Credit.(*agentpb.BackupConfigCredit_ValueCredit); ok {
		window.status.MetadataAccepted = true
	}
	if err := window.installGrant(credit, true); err != nil {
		return nil, err
	}
	window.status.LastCreditSequence = credit.CreditSequence
	copy(window.status.LastCreditSHA256[:], credit.AckSha256)
	return window, nil
}

// ApplyCredit advances to the next sealed receiver credit. Exact replay,
// sequence gaps, conflicting committed cursors, and window over-grants fail.
func (window *SenderWindow) ApplyCredit(credit *agentpb.BackupConfigCredit) error {
	if window == nil {
		return invalid("config sender credit window is required")
	}
	owned, err := ValidateCredit(window.binding, credit)
	if err != nil {
		return err
	}
	if owned.CreditSequence == window.status.LastCreditSequence {
		if subtle.ConstantTimeCompare(owned.AckSha256, window.status.LastCreditSHA256[:]) == 1 {
			return nil
		}
		return invalid("config credit sequence conflicts with the latest credit")
	}
	if owned.CreditSequence != window.status.LastCreditSequence+1 {
		return invalid("config credit sequence is replayed, conflicting, or discontinuous")
	}
	candidate := *window
	candidate.pending = append([]pendingRecord(nil), window.pending...)
	committedIndex, err := candidate.matchCommittedCursor(owned)
	if err != nil {
		return err
	}
	if committedIndex >= 0 {
		candidate.pending = append([]pendingRecord(nil), candidate.pending[committedIndex+1:]...)
	}
	candidate.setCommitted(owned)
	if err := candidate.installGrant(owned, false); err != nil {
		return err
	}
	candidate.status.LastCreditSequence = owned.CreditSequence
	copy(candidate.status.LastCreditSHA256[:], owned.AckSha256)
	candidate.status.UnacknowledgedRecordCount = uint32(len(candidate.pending))
	*window = candidate
	return nil
}

// Send consumes credit for one validated next record and returns its resulting
// chain digest. Only scalar accounting and the record digest/cursor are kept.
func (window *SenderWindow) Send(frame *agentpb.BackupConfigTransfer) ([]byte, error) {
	if window == nil {
		return nil, invalid("config sender credit window is required")
	}
	owned, err := ValidateFrame(window.binding, frame)
	if err != nil {
		return nil, err
	}
	defer ClearFrame(owned)
	wantSequence := window.status.LastSentRecordSequence + 1
	if owned.RecordSequence != wantSequence {
		return nil, invalid("config transfer record sequence is not the exact next record")
	}
	lane, byteCharge, nextOrdinal, err := window.recordAccounting(owned)
	if err != nil {
		return nil, err
	}
	if err := window.consume(lane, byteCharge); err != nil {
		return nil, err
	}
	chain, err := FrameChainSHA256(window.binding, window.status.SentChainSHA256[:], owned)
	if err != nil {
		window.refund(lane, byteCharge)
		return nil, err
	}
	record := pendingRecord{
		sequence: owned.RecordSequence, nextOrdinal: nextOrdinal, lane: lane, bytes: byteCharge,
	}
	copy(record.chain[:], chain)
	window.pending = append(window.pending, record)
	window.status.LastSentRecordSequence = owned.RecordSequence
	window.status.SentNextOrdinal = nextOrdinal
	copy(window.status.SentChainSHA256[:], chain)
	window.status.UnacknowledgedRecordCount = uint32(len(window.pending))
	return append([]byte(nil), chain...), nil
}

func (window *SenderWindow) Status() SenderStatus {
	if window == nil {
		return SenderStatus{}
	}
	return window.status
}

func (window *SenderWindow) matchCommittedCursor(credit *agentpb.BackupConfigCredit) (int, error) {
	if credit.CommittedRecordSequence < window.status.Committed.RecordSequence {
		return -1, invalid("config credit committed record moves backwards")
	}
	if credit.CommittedRecordSequence == window.status.Committed.RecordSequence {
		if credit.NextOrdinal != window.status.Committed.NextOrdinal ||
			!sameDigest(credit.CumulativeChainSha256, window.status.Committed.ChainSHA256[:]) {
			return -1, invalid("config credit conflicts with the committed cursor")
		}
		return -1, nil
	}
	for index := range window.pending {
		record := window.pending[index]
		if record.sequence == credit.CommittedRecordSequence {
			if record.nextOrdinal != credit.NextOrdinal ||
				!sameDigest(credit.CumulativeChainSha256, record.chain[:]) {
				return -1, invalid("config credit conflicts with a sent record cursor")
			}
			return index, nil
		}
	}
	return -1, invalid("config credit acknowledges an unsent record")
}

func (window *SenderWindow) installGrant(credit *agentpb.BackupConfigCredit, initial bool) error {
	switch grant := credit.Credit.(type) {
	case *agentpb.BackupConfigCredit_MetadataCredit:
		if window.status.MetadataAccepted {
			return invalid("config metadata credit follows metadata acceptance")
		}
		if !initial && window.status.MetadataRecordCredit != 0 {
			return invalid("config metadata credit replaces an unconsumed grant")
		}
		if window.pendingUsage(metadataLane, grant.MetadataCredit.RecordCredit, grant.MetadataCredit.ByteCredit) != nil {
			return invalid("config metadata credit exceeds the bounded window")
		}
		window.status.MetadataRecordCredit = grant.MetadataCredit.RecordCredit
		window.status.MetadataByteCredit = grant.MetadataCredit.ByteCredit
	case *agentpb.BackupConfigCredit_MetadataAccepted:
		if window.status.MetadataAccepted || window.hasPending(metadataLane) {
			return invalid("config metadata acceptance precedes complete metadata acknowledgement")
		}
		window.status.MetadataAccepted = true
		window.status.MetadataRecordCredit = 0
		window.status.MetadataByteCredit = 0
		window.status.ValueRecordCredit = grant.MetadataAccepted.InitialValueRecordCredit
		window.status.ValueByteCredit = grant.MetadataAccepted.InitialValueCreditBytes
	case *agentpb.BackupConfigCredit_ValueCredit:
		if !window.status.MetadataAccepted {
			return invalid("config value credit precedes metadata acceptance")
		}
		if grant.ValueCredit.RecordCredit > ^uint32(0)-window.status.ValueRecordCredit ||
			grant.ValueCredit.ValueCreditBytes > ^uint64(0)-window.status.ValueByteCredit ||
			window.pendingUsage(
				valueLane,
				window.status.ValueRecordCredit+grant.ValueCredit.RecordCredit,
				window.status.ValueByteCredit+grant.ValueCredit.ValueCreditBytes,
			) != nil {
			return invalid("config value credit exceeds the bounded window")
		}
		window.status.ValueRecordCredit += grant.ValueCredit.RecordCredit
		window.status.ValueByteCredit += grant.ValueCredit.ValueCreditBytes
	default:
		return invalid("config credit grant kind is invalid")
	}
	return nil
}

func (window *SenderWindow) pendingUsage(lane creditLane, records uint32, bytes uint64) error {
	for _, record := range window.pending {
		if record.lane != lane {
			continue
		}
		if records == ^uint32(0) || bytes > ^uint64(0)-record.bytes {
			return invalid("config credit accounting overflows")
		}
		records++
		bytes += record.bytes
	}
	if lane == metadataLane && (records > MetadataCreditRecords || bytes > MetadataCreditBytes) {
		return invalid("config metadata window exceeds its limit")
	}
	if lane == valueLane && (records > ValueCreditRecords || bytes > ValueCreditBytes) {
		return invalid("config value window exceeds its limit")
	}
	return nil
}

func (window *SenderWindow) recordAccounting(frame *agentpb.BackupConfigTransfer) (creditLane, uint64, uint32, error) {
	nextOrdinal := window.status.SentNextOrdinal
	switch record := frame.Record.(type) {
	case *agentpb.BackupConfigTransfer_Start:
		if window.status.MetadataAccepted || frame.RecordSequence != 1 {
			return 0, 0, 0, invalid("config transfer start record or phase is invalid")
		}
		return metadataLane, uint64(proto.Size(frame)), nextOrdinal, nil
	case *agentpb.BackupConfigTransfer_EntryHeader:
		if window.status.MetadataAccepted {
			return 0, 0, 0, invalid("config metadata record follows metadata acceptance")
		}
		return metadataLane, uint64(proto.Size(frame)), nextOrdinal, nil
	case *agentpb.BackupConfigTransfer_ValueChunk:
		if !window.status.MetadataAccepted || record.ValueChunk.Ordinal != nextOrdinal {
			return 0, 0, 0, invalid("config value chunk ordinal or phase is invalid")
		}
		return valueLane, uint64(len(record.ValueChunk.Content)), nextOrdinal, nil
	case *agentpb.BackupConfigTransfer_EntryEnd:
		if !window.status.MetadataAccepted || record.EntryEnd.Ordinal != nextOrdinal {
			return 0, 0, 0, invalid("config Entry end ordinal or phase is invalid")
		}
		return valueLane, 0, nextOrdinal + 1, nil
	case *agentpb.BackupConfigTransfer_End:
		if !window.status.MetadataAccepted {
			return 0, 0, 0, invalid("config transfer end precedes metadata acceptance")
		}
		return valueLane, 0, nextOrdinal, nil
	case *agentpb.BackupConfigTransfer_Resume:
		if !window.status.MetadataAccepted || record.Resume.NextOrdinal != nextOrdinal ||
			!sameDigest(record.Resume.ValueChainSha256, window.status.SentChainSHA256[:]) {
			return 0, 0, 0, invalid("config transfer resume does not match the sent cursor")
		}
		return valueLane, 0, nextOrdinal, nil
	default:
		return 0, 0, 0, invalid("config transfer record kind is invalid")
	}
}

func (window *SenderWindow) consume(lane creditLane, bytes uint64) error {
	switch lane {
	case metadataLane:
		if window.status.MetadataRecordCredit == 0 || window.status.MetadataByteCredit < bytes {
			return invalid("config metadata record exceeds granted credit")
		}
		window.status.MetadataRecordCredit--
		window.status.MetadataByteCredit -= bytes
	case valueLane:
		if window.status.ValueRecordCredit == 0 || window.status.ValueByteCredit < bytes {
			return invalid("config value record exceeds granted credit")
		}
		window.status.ValueRecordCredit--
		window.status.ValueByteCredit -= bytes
	default:
		return invalid("config credit lane is invalid")
	}
	return nil
}

func (window *SenderWindow) refund(lane creditLane, bytes uint64) {
	if lane == metadataLane {
		window.status.MetadataRecordCredit++
		window.status.MetadataByteCredit += bytes
		return
	}
	window.status.ValueRecordCredit++
	window.status.ValueByteCredit += bytes
}

func (window *SenderWindow) hasPending(lane creditLane) bool {
	for _, record := range window.pending {
		if record.lane == lane {
			return true
		}
	}
	return false
}

func (window *SenderWindow) setCommitted(credit *agentpb.BackupConfigCredit) {
	window.status.Committed.RecordSequence = credit.CommittedRecordSequence
	window.status.Committed.NextOrdinal = credit.NextOrdinal
	copy(window.status.Committed.ChainSHA256[:], credit.CumulativeChainSha256)
}
