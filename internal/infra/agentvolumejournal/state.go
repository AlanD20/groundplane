package agentvolumejournal

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/backupformat"
	"github.com/AlanD20/groundplane/internal/common/backupvolume"
	"github.com/AlanD20/groundplane/internal/infra/backupvolumefs"
)

func parseLog(raw []byte, config Config, seed [sha256.Size]byte, collect bool) (State, []byte, error) {
	if len(raw) > MaximumJournalBytes {
		return State{}, nil, invalid("Volume journal exceeds its reservation")
	}
	state := State{ChainSHA256: seed}
	position := 0
	for position < len(raw) {
		if len(raw)-position < 4 {
			break
		}
		length := binary.BigEndian.Uint32(raw[position : position+4])
		if length == 0 || length > maximumFrameBytes-4 {
			return State{}, nil, invalid("Volume journal frame length is invalid")
		}
		if int(length)+4 > len(raw)-position {
			break
		}
		value, err := decodeRecord(raw[position : position+int(length)+4])
		if err != nil {
			return State{}, nil, err
		}
		if value.sequence != state.RecordCount+1 || value.prior != state.ChainSHA256 ||
			value.sequence > 2*(2*config.NewEntryCount+config.OldEntryCount+1) {
			return State{}, nil, invalid("Volume journal sequence or chain is invalid")
		}
		if err := applyRecord(&state, config, value, collect); err != nil {
			return State{}, nil, err
		}
		state.RecordCount, state.ChainSHA256 = value.sequence, value.hash
		position += int(length) + 4
	}
	tail := append([]byte(nil), raw[position:]...)
	if len(tail) > maximumFrameBytes {
		return State{}, nil, invalid("Volume journal partial frame exceeds its bound")
	}
	state.UncommittedPrefix = len(tail) != 0
	return state, tail, nil
}

func applyRecord(state *State, config Config, value record, collect bool) error {
	mutation := value.mutation
	if value.phase == phaseIntent {
		if state.Pending != nil || state.RootDeleted || nextMutationInvalid(*state, config, mutation) {
			return invalid("Volume journal mutation intent is out of order")
		}
		owned := cloneMutation(mutation)
		state.Pending = &owned
		if mutation.Kind == backupvolumefs.MutationExchange {
			state.ExchangeOldInode, state.ExchangeNewInode = mutation.OldInode, mutation.NewInode
		}
		return nil
	}
	if value.phase != phaseCompleted || state.Pending == nil || !sameCompletion(*state.Pending, mutation) {
		return invalid("Volume journal completion lacks its exact intent")
	}
	switch mutation.Kind {
	case backupvolumefs.MutationConstruction:
		state.ConstructionCursor = mutation.Ordinal
		if mutation.Ordinal == 1 {
			state.RootNewInode = mutation.NewInode
		}
	case backupvolumefs.MutationFinalization:
		state.FinalizationCursor = mutation.Ordinal
	case backupvolumefs.MutationExchange:
		state.Exchanged = true
	case backupvolumefs.MutationDelete:
		state.DeletionCursor = mutation.Ordinal
		if mutation.Ordinal == config.OldEntryCount {
			state.RootDeleted = true
		}
	default:
		return invalid("Volume journal mutation kind is invalid")
	}
	state.LastCompletedOrdinal = mutation.Ordinal
	state.Pending = nil
	if collect {
		state.Completed = append(state.Completed, cloneMutation(mutation))
	}
	return nil
}

func nextMutationInvalid(state State, config Config, mutation backupvolumefs.Mutation) bool {
	if !validMutationEntry(mutation) {
		return true
	}
	zero := [sha256.Size]byte{}
	switch mutation.Kind {
	case backupvolumefs.MutationConstruction:
		return state.FinalizationCursor != 0 || state.Exchanged || state.ConstructionCursor >= config.NewEntryCount ||
			mutation.Ordinal != state.ConstructionCursor+1 ||
			(mutation.Ordinal == 1) != bytes.Equal(mutation.Entry.Path, []byte(".")) ||
			mutation.OldTreeSHA != zero || mutation.NewTreeSHA != zero ||
			mutation.OldInode != 0 || mutation.NewInode != 0
	case backupvolumefs.MutationFinalization:
		return state.ConstructionCursor != config.NewEntryCount || state.Exchanged ||
			state.FinalizationCursor >= config.NewEntryCount ||
			mutation.Ordinal != state.FinalizationCursor+1 ||
			(mutation.Ordinal == config.NewEntryCount) != bytes.Equal(mutation.Entry.Path, []byte(".")) ||
			mutation.OldTreeSHA != zero || mutation.NewTreeSHA != zero ||
			mutation.OldInode != 0 || mutation.NewInode != 0
	case backupvolumefs.MutationExchange:
		return state.FinalizationCursor != config.NewEntryCount || state.Exchanged ||
			mutation.Ordinal != 0 || len(mutation.Entry.Path) != 0 || mutation.Entry.Kind != 0 ||
			mutation.Entry.Mode != 0 || mutation.Entry.UID != 0 || mutation.Entry.GID != 0 ||
			mutation.Entry.SizeBytes != 0 || mutation.Entry.ContentSHA256 != zero ||
			mutation.OldTreeSHA != config.OldFullTreeSHA256 ||
			mutation.NewTreeSHA != config.NewFullTreeSHA256 ||
			mutation.OldInode == 0 || mutation.NewInode != state.RootNewInode ||
			mutation.OldInode == mutation.NewInode
	case backupvolumefs.MutationDelete:
		return !state.Exchanged || state.DeletionCursor >= config.OldEntryCount ||
			mutation.Ordinal != state.DeletionCursor+1 ||
			(mutation.Ordinal == config.OldEntryCount) != bytes.Equal(mutation.Entry.Path, []byte(".")) ||
			mutation.OldTreeSHA != config.OldFullTreeSHA256 ||
			mutation.NewTreeSHA != config.NewFullTreeSHA256 ||
			mutation.OldInode != state.ExchangeOldInode || mutation.NewInode != state.ExchangeNewInode
	default:
		return true
	}
}

func validMutationEntry(mutation backupvolumefs.Mutation) bool {
	if mutation.Kind == backupvolumefs.MutationExchange {
		return true
	}
	entry := mutation.Entry
	if len(entry.Path) == 0 || len(entry.Path) > backupvolume.MaxPathBytes ||
		entry.Path[0] == '/' || bytes.IndexByte(entry.Path, 0) >= 0 ||
		entry.Mode > 0o7777 || entry.Kind != backupvolume.EntryRegular && entry.Kind != backupvolume.EntryDirectory {
		return false
	}
	if !bytes.Equal(entry.Path, []byte(".")) {
		for _, component := range strings.Split(string(entry.Path), "/") {
			if component == "" || component == "." || component == ".." || len(component) > 255 {
				return false
			}
		}
	} else if entry.Kind != backupvolume.EntryDirectory {
		return false
	}
	if entry.Kind == backupvolume.EntryDirectory {
		return entry.SizeBytes == 0 && entry.ContentSHA256 == ([sha256.Size]byte{})
	}
	return entry.SizeBytes <= backupformat.MaxStoredBytes && entry.SizeBytes <= math.MaxInt64
}

func sameCompletion(pending, completion backupvolumefs.Mutation) bool {
	if pending.Kind != completion.Kind || pending.Ordinal != completion.Ordinal ||
		!bytes.Equal(pending.Entry.Path, completion.Entry.Path) ||
		pending.Entry.Kind != completion.Entry.Kind || pending.Entry.Mode != completion.Entry.Mode ||
		pending.Entry.UID != completion.Entry.UID || pending.Entry.GID != completion.Entry.GID ||
		pending.Entry.SizeBytes != completion.Entry.SizeBytes ||
		pending.Entry.ContentSHA256 != completion.Entry.ContentSHA256 ||
		pending.OldTreeSHA != completion.OldTreeSHA || pending.NewTreeSHA != completion.NewTreeSHA ||
		pending.OldInode != completion.OldInode {
		return false
	}
	if pending.Kind == backupvolumefs.MutationConstruction && pending.Ordinal == 1 {
		return pending.NewInode == 0 && completion.NewInode != 0
	}
	return pending.NewInode == completion.NewInode
}

func cloneMutation(value backupvolumefs.Mutation) backupvolumefs.Mutation {
	value.Entry.Path = append([]byte(nil), value.Entry.Path...)
	return value
}

func cloneState(state State) State {
	copyState := state
	copyState.Completed = make([]backupvolumefs.Mutation, len(state.Completed))
	for index := range state.Completed {
		copyState.Completed[index] = cloneMutation(state.Completed[index])
	}
	if state.Pending != nil {
		pending := cloneMutation(*state.Pending)
		copyState.Pending = &pending
	}
	return copyState
}
