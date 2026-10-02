//go:build linux

package agentvolumemanifest

import (
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/oklog/ulid/v2"
	"golang.org/x/sys/unix"
	"google.golang.org/protobuf/proto"
)

// Inventory is read-only. Every row or error must block Ready until exact
// Controller-directed recovery or retirement; no row is an implicit grant.
func Inventory(ctx context.Context, root string) ([]Recovered, error) {
	if ctx == nil || root != AgentRoot {
		return nil, invalid("Volume manifest receiver inventory root is invalid")
	}
	rootFD, found, err := openRoot(ctx, false)
	if err != nil || !found {
		return nil, err
	}
	defer unix.Close(rootFD)
	dup, err := unix.Dup(rootFD)
	if err != nil {
		return nil, storage(err)
	}
	directory := os.NewFile(uintptr(dup), "volume-manifest-root")
	names, readErr := directory.Readdirnames(MaximumJournals + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
		return nil, storage(errors.Join(readErr, closeErr))
	}
	if len(names) > MaximumJournals {
		return nil, invalid("Volume manifest receiver inventory exceeds its bound")
	}
	sort.Strings(names)
	rows := make([]Recovered, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return rows, err
		}
		retiring := strings.HasSuffix(name, ".retiring")
		transfer := name
		if retiring {
			transfer = strings.TrimSuffix(name, ".retiring")
		}
		if len(transfer) != 26 || transfer != strings.ToUpper(transfer) {
			return rows, invalid("Volume manifest receiver inventory has an unexpected transfer")
		}
		if _, err := ulid.ParseStrict(transfer); err != nil {
			return rows, invalid("Volume manifest receiver inventory has an invalid transfer")
		}
		fd, err := openDirectory(rootFD, name, false)
		if err != nil {
			return rows, err
		}
		if err := unix.Flock(fd, unix.LOCK_SH|unix.LOCK_NB); err != nil {
			_ = unix.Close(fd)
			return rows, conflict("Volume manifest receiver inventory overlaps an active owner")
		}
		row, err := inventoryRow(ctx, fd, name, transfer, retiring)
		closeErr := unix.Close(fd)
		if err != nil || closeErr != nil {
			return rows, errors.Join(err, storage(closeErr))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func inventoryRow(ctx context.Context, fd int, name, transfer string, retiring bool) (Recovered, error) {
	row := Recovered{DirectoryName: name, Retiring: retiring}
	names, err := directoryNames(fd)
	if err != nil {
		return row, err
	}
	var hasAuthority, hasLog, hasMarker bool
	for _, leaf := range names {
		switch leaf {
		case authorityName:
			hasAuthority = true
		case logName:
			hasLog = true
		case markerName:
			if hasMarker {
				return row, invalid("Volume manifest receiver has multiple retirement markers")
			}
			hasMarker = true
		case discardName:
			if hasMarker {
				return row, invalid("Volume manifest receiver has multiple retirement markers")
			}
			hasMarker = true
			row.Discarding = true
		default:
			return row, invalid("Volume manifest receiver inventory has an unexpected leaf")
		}
	}
	if hasMarker && !retiring || hasLog && !hasAuthority {
		return row, invalid("Volume manifest receiver inventory lacks a valid authority")
	}
	markerFile := markerName
	if row.Discarding {
		markerFile = discardName
	}
	if !hasAuthority {
		if !hasMarker {
			row.Unbound = true
			return row, nil
		}
		marker, err := readLeaf(fd, markerFile, maximumAuthority+3+64)
		if err != nil {
			return row, err
		}
		config, err := markerAuthority(marker)
		if err != nil || config.Binding.TransferID != transfer {
			return row, invalid("Volume manifest receiver marker authority differs from directory")
		}
		row.Config = config
		row.Complete = !row.Discarding
		return row, nil
	}
	if err := ctx.Err(); err != nil {
		return row, err
	}
	authority, err := readLeaf(fd, authorityName, maximumAuthority)
	if err != nil {
		return row, err
	}
	config, err := decodeAuthority(authority)
	if err != nil {
		return row, err
	}
	if config.Binding.TransferID != transfer {
		return row, invalid("Volume manifest receiver directory differs from authority")
	}
	row.Config = config
	if hasMarker {
		marker, err := readLeaf(fd, markerFile, maximumAuthority+3+64)
		if err != nil {
			return row, err
		}
		marked, err := markerAuthority(marker)
		if err != nil {
			row.Partial = true
		} else if marked != config {
			return row, invalid("Volume manifest receiver retirement marker authority differs")
		}
	}
	if !hasLog {
		row.Unbound = true
		return row, nil
	}
	raw, err := readLeaf(fd, logName, MaximumJournalBytes)
	if err != nil {
		return row, err
	}
	state, _, err := parseLog(raw, config, authority)
	if err != nil {
		return row, err
	}
	row.Complete, row.Partial = state.complete, row.Partial || state.partial
	if len(state.credits) == 0 {
		row.Unbound = true
	} else {
		row.CurrentCredit = proto.CloneOf(state.credits[len(state.credits)-1])
	}
	return row, nil
}
