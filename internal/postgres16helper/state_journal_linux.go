package postgres16helper

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	grounderrs "github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

const privateStateRecordsLimit = 16

// StateJournal retains each phase transition as a chained, fsynced record.
// A torn or contradictory tail is retained and blocks another launch with the
// same nonce; no reader truncates it to a convenient previous phase.
type StateJournal struct {
	file     *os.File
	last     postgres16protocol.ConfinementStateShape
	hash     postgres16protocol.Digest
	count    int
	offset   int64
	poisoned bool
}

func OpenStateJournal(
	stateDir *os.File, nonce postgres16protocol.Nonce,
) (*StateJournal, error) {
	return openStateJournal(stateDir, nonce, true)
}

func OpenExistingStateJournal(
	stateDir *os.File, nonce postgres16protocol.Nonce,
) (*StateJournal, error) {
	return openStateJournal(stateDir, nonce, false)
}

func openStateJournal(
	stateDir *os.File, nonce postgres16protocol.Nonce, create bool,
) (*StateJournal, error) {
	if stateDir == nil || nonce == (postgres16protocol.Nonce{}) {
		return nil, stateJournalError()
	}
	name := nonce.String() + ".state"
	var fd int
	var err error
	created := false
	if create {
		fd, err = unix.Openat2(int(stateDir.Fd()), name, &unix.OpenHow{
			Flags:   unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW | unix.O_CREAT | unix.O_EXCL,
			Mode:    uint64(postgres16protocol.StateFileMode),
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
		})
		created = err == nil
	}
	if !create || errors.Is(err, unix.EEXIST) {
		fd, err = unix.Openat2(int(stateDir.Fd()), name, &unix.OpenHow{
			Flags:   unix.O_RDWR | unix.O_CLOEXEC | unix.O_NOFOLLOW,
			Resolve: unix.RESOLVE_BENEATH | unix.RESOLVE_NO_SYMLINKS | unix.RESOLVE_NO_MAGICLINKS,
		})
	}
	if err != nil {
		return nil, stateJournalError()
	}
	file := os.NewFile(uintptr(fd), name)
	closeOnError := func() (*StateJournal, error) {
		_ = file.Close()
		return nil, stateJournalError()
	}
	var metadata unix.Stat_t
	if err := unix.Fstat(fd, &metadata); err != nil || metadata.Mode&unix.S_IFMT != unix.S_IFREG ||
		metadata.Uid != postgres16protocol.StateFileUID ||
		metadata.Gid != postgres16protocol.StateFileGID ||
		metadata.Mode&0o7777 != postgres16protocol.StateFileMode || metadata.Nlink != 1 {
		return closeOnError()
	}
	if err := unix.Flock(fd, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return closeOnError()
	}
	if created && unix.Fsync(int(stateDir.Fd())) != nil {
		return closeOnError()
	}
	journal := &StateJournal{file: file}
	if err := journal.load(); err != nil {
		return closeOnError()
	}
	return journal, nil
}

func (journal *StateJournal) Current() (postgres16protocol.ConfinementStateShape, bool) {
	return journal.last, journal.count != 0
}

func (journal *StateJournal) Append(next postgres16protocol.ConfinementStateShape) error {
	if journal == nil || journal.file == nil || journal.poisoned || journal.count >= privateStateRecordsLimit {
		return stateJournalError()
	}
	encoded, sized, err := encodeSizedState(next)
	if err != nil {
		return stateJournalError()
	}
	next = sized
	if journal.count == 0 {
		if next.Sequence != 1 || next.Phase != postgres16protocol.ConfinementPhaseCreated {
			return stateJournalError()
		}
	} else if !postgres16protocol.ValidConfinementTransition(journal.last, next) {
		return stateJournalError()
	}
	var metadata unix.Stat_t
	if err := unix.Fstat(int(journal.file.Fd()), &metadata); err != nil || metadata.Size != journal.offset {
		journal.poisoned = true
		return stateJournalError()
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(encoded)))
	chain := stateChain(journal.hash, encoded)
	if _, err := journal.file.Seek(journal.offset, io.SeekStart); err != nil {
		journal.poisoned = true
		return stateJournalError()
	}
	if writeAll(journal.file, header[:]) != nil || writeAll(journal.file, encoded) != nil ||
		writeAll(journal.file, chain[:]) != nil || journal.file.Sync() != nil {
		journal.poisoned = true
		return stateJournalError()
	}
	journal.last = next
	journal.hash = chain
	journal.count++
	journal.offset += int64(len(header) + len(encoded) + len(chain))
	return nil
}

func (journal *StateJournal) Close() error {
	if journal == nil || journal.file == nil {
		return nil
	}
	err := journal.file.Close()
	journal.file = nil
	return err
}

func (journal *StateJournal) load() error {
	for {
		var header [4]byte
		_, err := io.ReadFull(journal.file, header[:])
		if err == io.EOF {
			return nil
		}
		if err != nil || journal.count >= privateStateRecordsLimit {
			return stateJournalError()
		}
		size := binary.BigEndian.Uint32(header[:])
		if size == 0 || size > privateStateLimit {
			return stateJournalError()
		}
		encoded := make([]byte, int(size))
		var claimed postgres16protocol.Digest
		if _, err := io.ReadFull(journal.file, encoded); err != nil {
			return stateJournalError()
		}
		if _, err := io.ReadFull(journal.file, claimed[:]); err != nil ||
			claimed != stateChain(journal.hash, encoded) {
			return stateJournalError()
		}
		state, err := unmarshalState(encoded)
		if err != nil || state.EncodedSizeBytes != uint64(len(encoded)) {
			return stateJournalError()
		}
		if journal.count == 0 {
			if state.Sequence != 1 || state.Phase != postgres16protocol.ConfinementPhaseCreated {
				return stateJournalError()
			}
		} else if !postgres16protocol.ValidConfinementTransition(journal.last, state) {
			return stateJournalError()
		}
		journal.last = state
		journal.hash = claimed
		journal.count++
		journal.offset += int64(len(header) + len(encoded) + len(claimed))
	}
}

func encodeSizedState(state postgres16protocol.ConfinementStateShape) (
	[]byte, postgres16protocol.ConfinementStateShape, error,
) {
	if state.EncodedSizeBytes == 0 {
		state.EncodedSizeBytes = 1
	}
	for attempt := 0; attempt < 8; attempt++ {
		encoded, err := marshalState(state)
		if err != nil {
			return nil, postgres16protocol.ConfinementStateShape{}, err
		}
		if state.EncodedSizeBytes == uint64(len(encoded)) {
			return encoded, state, nil
		}
		state.EncodedSizeBytes = uint64(len(encoded))
	}
	return nil, postgres16protocol.ConfinementStateShape{}, stateJournalError()
}

func stateChain(previous postgres16protocol.Digest, encoded []byte) postgres16protocol.Digest {
	hash := sha256.New()
	_, _ = hash.Write([]byte("groundplane.postgres16.state-chain.v1\x00"))
	_, _ = hash.Write(previous[:])
	_, _ = hash.Write(encoded)
	var digest postgres16protocol.Digest
	copy(digest[:], hash.Sum(nil))
	return digest
}

func stateJournalError() error {
	return grounderrs.New(grounderrs.KindInternal, "postgres helper state journal is invalid")
}
