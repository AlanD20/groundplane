package entries

import (
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ClassifyEntryWriteConflict(
	values []*etcdstore.KeyValue,
	record Record,
	expectedEntryRevision int64,
	expectedOwnerRevision int64,
	fence environmentfence.Evidence,
) error {
	expected := 4 + fence.ConditionCount()
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Entry write compare evidence is incomplete")
	}
	if expectedEntryRevision == 0 {
		if values[0] != nil || values[1] != nil {
			return errs.New(errs.KindStateConflict, "Entry stable identity is already in use")
		}
	} else {
		if values[0] == nil {
			return errs.New(errs.KindEntryNotFound, "Entry was not found")
		}
		if values[0].ModRevision != expectedEntryRevision {
			return recordcodec.StateConflict("entry", record.Entry.ID)
		}
		if values[1] == nil || string(values[1].Value) != record.Entry.ID {
			return errs.New(errs.KindInternal, "Entry owner index changed or is corrupt")
		}
		if values[1].ModRevision != expectedOwnerRevision {
			return recordcodec.StateConflict("Entry", record.Entry.ID)
		}
	}
	if values[2] != nil {
		return errs.New(errs.KindStateConflict, "Entry value generation id is already occupied")
	}
	if values[3] != nil {
		return errs.New(errs.KindResourceInUse, "Entry deletion is in progress")
	}
	if conflict := fence.ClassifyConflict(values[4:]); conflict != nil {
		return conflict
	}
	return recordcodec.StateConflict("entry", record.Entry.ID)
}

func classifyEntryDeleteConflict(
	values []*etcdstore.KeyValue,
	current etcdstore.Versioned[Record],
	expectedOwnerRevision int64,
	fence environmentfence.Evidence,
) error {
	expected := 3 + fence.ConditionCount()
	if len(values) != expected {
		return errs.New(errs.KindInternal, "Entry delete compare evidence is incomplete")
	}
	if values[0] == nil {
		return errs.New(errs.KindEntryNotFound, "Entry was not found")
	}
	if values[0].ModRevision != current.Revision {
		return recordcodec.StateConflict("entry", current.Record.Entry.ID)
	}
	if values[1] == nil || string(values[1].Value) != current.Record.Entry.ID {
		return errs.New(errs.KindInternal, "Entry owner index changed or is corrupt")
	}
	if values[1].ModRevision != expectedOwnerRevision {
		return recordcodec.StateConflict("Entry", current.Record.Entry.ID)
	}
	if values[2] != nil {
		return errs.New(errs.KindResourceInUse, "Entry deletion is in progress")
	}
	if conflict := fence.ClassifyConflict(values[3:]); conflict != nil {
		return conflict
	}
	return recordcodec.StateConflict("entry", current.Record.Entry.ID)
}

func EntryOwnerCollectionPrefix(environmentID string) string {
	return entryOwnerPrefix + environmentID + "/"
}

func EntryOwnerKey(environmentID string, entryID string) string {
	return EntryOwnerCollectionPrefix(environmentID) + entryID
}
