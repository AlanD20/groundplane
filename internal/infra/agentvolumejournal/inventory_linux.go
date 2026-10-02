//go:build linux

package agentvolumejournal

import (
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/oklog/ulid/v2"
	"golang.org/x/sys/unix"
)

// Inventory reads bounded journal summaries without changing their bytes. Any
// returned row blocks Agent Ready until the Controller assigns exact recovery
// or cleanup. An error also blocks Ready; callers must not discard it.
func Inventory(ctx context.Context, root string) ([]Recovered, error) {
	if ctx == nil || root != AgentRoot {
		return nil, invalid("Volume journal inventory root is invalid")
	}
	rootFD, found, err := openAgentRoot(ctx, false)
	if err != nil || !found {
		return nil, err
	}
	defer unix.Close(rootFD)
	dup, err := unix.Dup(rootFD)
	if err != nil {
		return nil, storage(err)
	}
	directory := os.NewFile(uintptr(dup), "volume-journal-root")
	names, readErr := directory.Readdirnames(MaximumJournals + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) || closeErr != nil {
		return nil, storage(errors.Join(readErr, closeErr))
	}
	if len(names) > MaximumJournals {
		return nil, invalid("Volume journal inventory exceeds its bound")
	}
	sort.Strings(names)
	rows := make([]Recovered, 0, len(names))
	for _, name := range names {
		if err := ctx.Err(); err != nil {
			return rows, err
		}
		retiring := strings.HasSuffix(name, ".retiring")
		generation := name
		if retiring {
			generation = strings.TrimSuffix(name, ".retiring")
		}
		if len(generation) != 26 || generation != strings.ToUpper(generation) {
			return rows, invalid("Volume journal inventory has an unexpected generation")
		}
		if _, err := ulid.ParseStrict(generation); err != nil {
			return rows, invalid("Volume journal inventory has an invalid generation")
		}
		fd, err := openExistingDirectory(rootFD, name)
		if err != nil {
			return rows, err
		}
		if err := unix.Flock(fd, unix.LOCK_SH|unix.LOCK_NB); err != nil {
			_ = unix.Close(fd)
			return rows, conflict("Volume journal inventory overlaps an active owner")
		}
		row, err := inventoryRow(ctx, fd, name, generation, retiring)
		closeErr := unix.Close(fd)
		if err != nil || closeErr != nil {
			return rows, errors.Join(err, storage(closeErr))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// Inspect reads only one sealed restore generation. Other active generation
// locks do not impede concurrent independent Volume restores.
func Inspect(ctx context.Context, root, generation string) (Recovered, bool, error) {
	if ctx == nil || root != AgentRoot || len(generation) != 26 ||
		generation != strings.ToUpper(generation) {
		return Recovered{}, false, invalid("Volume journal generation inspection authority is invalid")
	}
	if _, err := ulid.ParseStrict(generation); err != nil {
		return Recovered{}, false, invalid("Volume journal generation inspection ID is invalid")
	}
	rootFD, found, err := openAgentRoot(ctx, false)
	if err != nil {
		return Recovered{}, false, err
	}
	if !found {
		return Recovered{}, false, nil
	}
	defer unix.Close(rootFD)
	name := generation
	fd, err := openExistingDirectory(rootFD, name)
	if errors.Is(err, unix.ENOENT) {
		name = generation + ".retiring"
		fd, err = openExistingDirectory(rootFD, name)
	}
	if err != nil {
		if errors.Is(err, unix.ENOENT) {
			return Recovered{}, false, nil
		}
		return Recovered{}, false, err
	}
	defer unix.Close(fd)
	if err := unix.Flock(fd, unix.LOCK_SH|unix.LOCK_NB); err != nil {
		if errors.Is(err, unix.EWOULDBLOCK) {
			return Recovered{}, false, conflict("Volume journal generation is owned by another process")
		}
		return Recovered{}, false, storage(err)
	}
	row, err := inventoryRow(ctx, fd, name, generation, name != generation)
	if err != nil {
		return Recovered{}, false, err
	}
	return row, true, nil
}

func inventoryRow(ctx context.Context, fd int, name, generation string, retiring bool) (Recovered, error) {
	row := Recovered{DirectoryName: name, Retiring: retiring}
	names, err := directoryNames(fd)
	if err != nil {
		return row, err
	}
	var authorityExists, logExists, markerExists bool
	for _, leaf := range names {
		switch leaf {
		case authorityName:
			authorityExists = true
		case logName:
			logExists = true
		case retireName:
			if markerExists {
				return row, invalid("Volume journal has multiple retirement markers")
			}
			markerExists = true
		case discardName:
			if markerExists {
				return row, invalid("Volume journal has multiple retirement markers")
			}
			markerExists = true
			row.Discarding = true
		default:
			return row, invalid("Volume journal inventory has an unexpected leaf")
		}
	}
	if markerExists && !retiring {
		return row, invalid("Active Volume journal has a retirement marker")
	}
	markerFile := retireName
	if row.Discarding {
		markerFile = discardName
	}
	if !authorityExists {
		if logExists || markerExists && !retiring {
			return row, invalid("Volume journal lacks its sealed authority")
		}
		if !markerExists {
			row.Unbound = true
			return row, nil
		}
		marker, err := readRetirementMarker(fd, markerFile)
		if err != nil {
			return row, err
		}
		config, err := markerAuthority(marker)
		if err != nil || config.RestoreGenerationID != generation {
			return row, invalid("Volume journal marker authority differs from directory")
		}
		row.Config = config
		row.State.RootDeleted = !row.Discarding
		return row, nil
	}
	config, err := readAuthority(ctx, fd)
	if err != nil {
		return row, err
	}
	if config.RestoreGenerationID != generation {
		return row, invalid("Volume journal generation differs from sealed authority")
	}
	row.Config = config
	if markerExists {
		marker, err := readRetirementMarker(fd, markerFile)
		if err != nil {
			return row, err
		}
		marked, err := markerAuthority(marker)
		if err != nil {
			row.State.UncommittedPrefix = true
		} else if marked != config {
			return row, invalid("Volume journal retirement marker authority differs")
		}
	}
	if !logExists {
		row.Unbound = true
		return row, nil
	}
	file, err := openLeaf(fd, logName, unix.O_RDONLY, false, MaximumJournalBytes)
	if err != nil {
		return row, err
	}
	raw, readErr := readLogBytes(file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		return row, errors.Join(readErr, storage(closeErr))
	}
	authority, err := encodeAuthority(config)
	if err != nil {
		return row, err
	}
	state, _, err := parseLog(raw, config, initialChain(authority), false)
	if err != nil {
		return row, err
	}
	state.UncommittedPrefix = state.UncommittedPrefix || row.State.UncommittedPrefix
	row.State = state
	return row, nil
}

func openExistingDirectory(rootFD int, name string) (int, error) {
	var named unix.Stat_t
	if err := unix.Fstatat(rootFD, name, &named, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return -1, storage(err)
	}
	if named.Mode&unix.S_IFMT != unix.S_IFDIR || named.Mode&0o7777 != 0o700 || named.Uid != 0 {
		return -1, invalid("Volume journal generation directory is unsafe")
	}
	fd, err := unix.Openat2(rootFD, name, &unix.OpenHow{
		Flags: uint64(unix.O_RDONLY | unix.O_DIRECTORY | unix.O_CLOEXEC | unix.O_NOFOLLOW), Resolve: resolveLocal,
	})
	if err != nil {
		return -1, storage(err)
	}
	var opened unix.Stat_t
	if err := unix.Fstat(fd, &opened); err != nil || opened.Dev != named.Dev ||
		opened.Ino != named.Ino || opened.Mode&unix.S_IFMT != unix.S_IFDIR ||
		opened.Mode&0o7777 != 0o700 || opened.Uid != 0 {
		_ = unix.Close(fd)
		if err != nil {
			return -1, storage(err)
		}
		return -1, invalid("Volume journal generation directory changed")
	}
	return fd, nil
}
