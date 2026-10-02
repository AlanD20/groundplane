package backupvolumemanifest

import (
	"bytes"
	"context"
	"crypto/sha256"

	"github.com/AlanD20/groundplane/internal/common/backupvolumetransfer"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(
		context.Context,
		[]etcdstore.Condition,
		[]etcdstore.Mutation,
	) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// Guard returns exact Task, claim, assignment, deadline and Environment-lock
// compares at the supplied fixed MVCC read. Both initial and incremental
// publication require it; no credit is granted from channel identity alone.
type Guard func(context.Context, Owner, int64) ([]etcdstore.Condition, error)

type Repository struct {
	store store
	guard Guard
}

func NewRepository(store store, guard Guard) *Repository {
	return &Repository{store: store, guard: guard}
}

func (repository *Repository) Read(
	ctx context.Context,
	owner Owner,
	revision int64,
) (etcdstore.Versioned[Cursor], bool, error) {
	var zero etcdstore.Versioned[Cursor]
	if repository == nil || repository.store == nil || owner.Validate() != nil || revision < 0 {
		return zero, false, invalidLedger()
	}
	key := CursorKey(owner.Binding)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return zero, false, err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != 1 ||
		(revision > 0 && read.ReadRevision != revision) {
		return zero, false, invalidLedger()
	}
	zero.ReadRevision = read.ReadRevision
	defer etcdstore.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return zero, false, nil
	}
	cursor, err := decodeCursor(value.Value)
	if err != nil || value.Key != key || value.ModRevision <= 0 || cursor.Owner != owner {
		return zero, false, invalidLedger()
	}
	return etcdstore.Versioned[Cursor]{Record: cursor, Revision: value.ModRevision,
		ReadRevision: read.ReadRevision}, true, nil
}

// Open durably publishes the first grant. Reopen returns the actual latest
// native credit so the sender can reconstruct and verify its immutable prefix.
func (repository *Repository) Open(ctx context.Context, owner Owner) (*agentpb.BackupVolumeManifestAckCredit, error) {
	if repository == nil || repository.guard == nil || owner.Validate() != nil {
		return nil, invalidLedger()
	}
	current, found, err := repository.Read(ctx, owner, 0)
	if err != nil {
		return nil, err
	}
	conditions, err := repository.guard(ctx, owner, current.ReadRevision)
	if err != nil {
		return nil, err
	}
	if found {
		_ = conditions
		return current.Record.LastCredit()
	}
	credit, err := backupvolumetransfer.InitialCredit(owner.Binding)
	if err != nil {
		return nil, err
	}
	raw, err := proto.MarshalOptions{Deterministic: true}.Marshal(credit)
	if err != nil {
		return nil, invalidLedger()
	}
	cursor, err := encodeCursor(Cursor{Owner: owner, Credit: raw})
	if err != nil {
		return nil, err
	}
	conditions = append(conditions, etcdstore.Condition{Key: CursorKey(owner.Binding)},
		etcdstore.Condition{Key: CreditKey(owner.Binding, 1)})
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: CursorKey(owner.Binding), Value: cursor},
		{Type: etcdstore.MutationPut, Key: CreditKey(owner.Binding, 1), Value: raw}}
	defer etcdstore.ClearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, invalidLedger()
	}
	return credit, nil
}

// Accept commits one validated frame, its next grant and the native cursor in
// one bounded transaction. On ambiguous commit, exact replay returns the
// immutable original credit; a conflicting replacement never advances.
func (repository *Repository) Accept(ctx context.Context, owner Owner,
	frame *agentpb.BackupVolumeManifestTransfer,
) (*agentpb.BackupVolumeManifestAckCredit, error) {
	if repository == nil || repository.guard == nil || owner.Validate() != nil {
		return nil, invalidLedger()
	}
	validated, err := backupvolumetransfer.ValidateFrame(owner.Binding, frame)
	if err != nil || validated.GetResume() != nil {
		return nil, invalidLedger()
	}
	current, found, err := repository.Read(ctx, owner, 0)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, invalidLedger()
	}
	previous, err := current.Record.LastCredit()
	if err != nil {
		return nil, err
	}
	frameKey := FrameKey(owner.Binding, validated.RecordSequence)
	creditKey := CreditKey(owner.Binding, validated.RecordSequence+1)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{frameKey, creditKey}, Revision: current.ReadRevision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != current.ReadRevision || len(read.Values) != 2 {
		return nil, invalidLedger()
	}
	defer etcdstore.ClearValues(read.Values)
	if read.Values[0] != nil || read.Values[1] != nil {
		if read.Values[0] == nil || read.Values[1] == nil {
			return nil, invalidLedger()
		}
		storedOwner, storedFrame, err := decodeFrame(read.Values[0].Value)
		if err != nil || storedOwner != owner || !proto.Equal(storedFrame, validated) {
			return nil, invalidLedger()
		}
		var credit agentpb.BackupVolumeManifestAckCredit
		if proto.Unmarshal(read.Values[1].Value, &credit) != nil ||
			backupvolumetransfer.ValidateCredit(owner.Binding, &credit) != nil ||
			credit.CommittedRecordSequence != validated.RecordSequence {
			return nil, invalidLedger()
		}
		return &credit, nil
	}
	if current.Record.Complete || validated.RecordSequence != previous.CommittedRecordSequence+1 {
		return nil, invalidLedger()
	}
	chain, err := backupvolumetransfer.NextChain(owner.Binding,
		digest(previous.TransferChainSha256), validated)
	if err != nil {
		return nil, err
	}
	nextCursor := current.Record
	next := previous.NextOrdinal
	if start := validated.GetStart(); start != nil {
		if validated.RecordSequence != 1 || nextCursor.EntryCount != 0 ||
			start.PointId == "" || start.EntryCount == 0 {
			return nil, invalidLedger()
		}
		nextCursor.EntryCount, nextCursor.PointID, nextCursor.Generation = start.EntryCount, start.PointId, start.RestoreGenerationId
		nextCursor.ContentSHA = append([]byte(nil), start.ContentManifestSha256...)
		nextCursor.FullTreeSHA = append([]byte(nil), start.FullTreeSha256...)
	} else if batch := validated.GetBatch(); batch != nil {
		if nextCursor.EntryCount == 0 || batch.FirstOrdinal != next ||
			batch.FirstOrdinal+uint64(len(batch.Entries))-1 > nextCursor.EntryCount {
			return nil, invalidLedger()
		}
		next += uint64(len(batch.Entries))
	} else if end := validated.GetEnd(); end != nil {
		if nextCursor.EntryCount == 0 || next != nextCursor.EntryCount+1 ||
			end.EntryCount != nextCursor.EntryCount ||
			!bytes.Equal(end.ContentManifestSha256, nextCursor.ContentSHA) ||
			!bytes.Equal(end.FullTreeSha256, nextCursor.FullTreeSHA) {
			return nil, invalidLedger()
		}
		nextCursor.Complete = true
	} else {
		return nil, invalidLedger()
	}
	credit, err := backupvolumetransfer.NextCredit(owner.Binding, previous, validated, chain, next)
	if err != nil {
		return nil, err
	}
	creditRaw, err := proto.MarshalOptions{Deterministic: true}.Marshal(credit)
	if err != nil {
		return nil, invalidLedger()
	}
	nextCursor.Credit = creditRaw
	cursorRaw, err := encodeCursor(nextCursor)
	if err != nil {
		return nil, err
	}
	frameRaw, err := encodeFrame(owner, validated)
	if err != nil {
		return nil, err
	}
	conditions, err := repository.guard(ctx, owner, current.ReadRevision)
	if err != nil {
		return nil, err
	}
	conditions = append(conditions, etcdstore.Condition{Key: CursorKey(owner.Binding), ModRevision: current.Revision},
		etcdstore.Condition{Key: frameKey}, etcdstore.Condition{Key: creditKey})
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: CursorKey(owner.Binding), Value: cursorRaw},
		{Type: etcdstore.MutationPut, Key: frameKey, Value: frameRaw},
		{Type: etcdstore.MutationPut, Key: creditKey, Value: creditRaw}}
	defer etcdstore.ClearMutationValues(mutations)
	budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
	if err != nil || !budget.Fits() {
		return nil, invalidLedger()
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	etcdstore.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, invalidLedger()
	}
	return credit, nil
}

func digest(value []byte) [sha256.Size]byte {
	var result [sha256.Size]byte
	copy(result[:], value)
	return result
}
