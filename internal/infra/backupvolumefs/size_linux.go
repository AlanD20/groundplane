//go:build linux

package backupvolumefs

import (
	"context"
	"errors"
	"io"
	"math"
	"os"

	"golang.org/x/sys/unix"
)

const (
	maximumSizeEntries = 65536
	maximumSizeDepth   = 128
)

// ApparentSize measures regular files without reading contents or following
// symlinks. A hard-linked regular file inside the volume counts only once.
func (volume *Volume) ApparentSize(ctx context.Context) (uint64, error) {
	if err := volume.liveIdentity(); err != nil {
		return 0, err
	}
	size := volumeSize{links: make(map[uint64]struct{})}
	if err := size.walk(ctx, volume.liveFD, 0); err != nil {
		return 0, err
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	if err := volume.liveIdentity(); err != nil {
		return 0, err
	}
	return size.bytes, nil
}

type volumeSize struct {
	bytes   uint64
	entries int
	links   map[uint64]struct{}
}

func (size *volumeSize) walk(ctx context.Context, fd, depth int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if depth >= maximumSizeDepth {
		return invalid("managed Volume size scan exceeds its depth limit")
	}
	// Open a separate directory stream without moving the pinned descriptor.
	stream, err := openDirectory(fd, ".")
	if err != nil {
		return system("reopen managed Volume directory for measurement", err)
	}
	dir := os.NewFile(uintptr(stream), "managed-volume-size")
	defer dir.Close()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		names, err := dir.Readdirnames(128)
		if err != nil && !errors.Is(err, io.EOF) {
			return system("read managed Volume directory for measurement", err)
		}
		for _, name := range names {
			if err := ctx.Err(); err != nil {
				return err
			}
			size.entries++
			if size.entries > maximumSizeEntries {
				return invalid("managed Volume size scan exceeds its entry limit")
			}
			if err := size.measure(ctx, fd, name, depth); err != nil {
				return err
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
	}
}

func (size *volumeSize) measure(ctx context.Context, parent int, name string, depth int) error {
	child, err := unix.Openat2(parent, name, &unix.OpenHow{
		Flags: unix.O_PATH | unix.O_NOFOLLOW | unix.O_CLOEXEC, Resolve: confined,
	})
	if err != nil {
		return system("open managed Volume entry for measurement", err)
	}
	defer unix.Close(child)
	var stat unix.Stat_t
	if err := unix.Fstat(child, &stat); err != nil {
		return system("inspect managed Volume entry size", err)
	}
	switch stat.Mode & unix.S_IFMT {
	case unix.S_IFDIR:
		return size.walk(ctx, child, depth+1)
	case unix.S_IFREG:
		if stat.Nlink > 1 {
			if _, seen := size.links[stat.Ino]; seen {
				return nil
			}
			size.links[stat.Ino] = struct{}{}
		}
		if stat.Size < 0 || uint64(stat.Size) > math.MaxUint64-size.bytes {
			return invalid("managed Volume size is outside the supported range")
		}
		size.bytes += uint64(stat.Size)
	}
	return nil
}
