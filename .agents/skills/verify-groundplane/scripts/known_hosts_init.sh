#!/usr/bin/env bash

copy_known_hosts() {
	python3 -c '
import errno
import os
import stat
import sys

limit = 2097152
source_path = sys.argv[1]
destination = sys.argv[2]
source_fd = -1
temporary_fd = -1
temporary_path = destination + ".tmp"
try:
    source_fd = os.open(source_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    if not stat.S_ISREG(os.fstat(source_fd).st_mode):
        raise OSError(errno.EINVAL, "known-hosts input is not a regular file")
    contents = bytearray()
    while len(contents) < limit + 1:
        chunk = os.read(source_fd, limit + 1 - len(contents))
        if not chunk:
            break
        contents.extend(chunk)
    if len(contents) > limit:
        raise OSError(errno.EFBIG, "known-hosts input exceeds the 2 MiB evidence bound")
    temporary_fd = os.open(
        temporary_path,
        os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
        0o600,
    )
    offset = 0
    while offset < len(contents):
        written = os.write(temporary_fd, contents[offset:])
        if written <= 0:
            raise OSError(errno.EIO, "known-hosts evidence write made no progress")
        offset += written
    os.fsync(temporary_fd)
    os.close(temporary_fd)
    temporary_fd = -1
    os.replace(temporary_path, destination)
except (OSError, ValueError) as error:
    try:
        os.unlink(temporary_path)
    except FileNotFoundError:
        pass
    print(str(error), file=sys.stderr)
    raise SystemExit(1)
finally:
    if temporary_fd >= 0:
        os.close(temporary_fd)
    if source_fd >= 0:
        os.close(source_fd)
' "$1" "$2"
}
