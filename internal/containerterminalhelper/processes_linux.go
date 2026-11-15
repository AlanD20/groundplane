package containerterminalhelper

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

type processIdentity struct {
	pid     int
	parent  int
	started string
	zombie  bool
}

func readIdentity(pid int) (processIdentity, error) {
	data, err := os.ReadFile(filepath.Join("/proc", strconv.Itoa(pid), "stat"))
	if err != nil {
		return processIdentity{}, errs.Wrap(errs.KindRequestUnavailable, err)
	}
	end := strings.LastIndexByte(string(data), ')')
	if end < 0 {
		return processIdentity{}, errs.New(errs.KindInternal, "process identity is unavailable")
	}
	fields := strings.Fields(string(data[end+1:]))
	if len(fields) < 20 {
		return processIdentity{}, errs.New(errs.KindInternal, "process identity is incomplete")
	}
	parent, err := strconv.Atoi(fields[1])
	if err != nil {
		return processIdentity{}, errs.Wrap(errs.KindInternal, err)
	}
	return processIdentity{pid: pid, parent: parent, started: fields[19], zombie: fields[0] == "Z"}, nil
}

// A subreaper retains orphaned grandchildren, including jobs that made a new
// process group. pidfds plus start-time checks prevent PID reuse from selecting
// a different container process. Nothing outside this helper's descendant tree
// is signalled; the Agent rejects host/shared PID namespaces before launch.
func terminateDescendants() error {
	deadline := time.Now().Add(3 * time.Second)
	for {
		entries, err := os.ReadDir("/proc")
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		var processes []processIdentity
		for _, entry := range entries {
			if pid, err := strconv.Atoi(entry.Name()); err == nil && pid > 1 && pid != os.Getpid() {
				if identity, err := readIdentity(pid); err == nil && !identity.zombie {
					processes = append(processes, identity)
				}
			}
		}
		owned := map[int]bool{os.Getpid(): true}
		for changed := true; changed; {
			changed = false
			for _, process := range processes {
				if !owned[process.pid] && owned[process.parent] {
					owned[process.pid], changed = true, true
				}
			}
		}
		if len(owned) == 1 {
			return nil
		}
		for _, process := range processes {
			if !owned[process.pid] {
				continue
			}
			fd, err := unix.PidfdOpen(process.pid, 0)
			if err != nil {
				continue
			}
			current, readErr := readIdentity(process.pid)
			if readErr == nil && current.started == process.started {
				_ = unix.PidfdSendSignal(fd, unix.SIGKILL, nil, 0)
			}
			_ = unix.Close(fd)
		}
		if time.Now().After(deadline) {
			return errs.New(errs.KindRequestUnavailable, "container terminal process cleanup did not finish")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
