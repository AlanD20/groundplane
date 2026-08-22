//go:build linux

package environmentroot

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"golang.org/x/sys/unix"
)

func TestValidateTraversesWithExactPolicyAndReturnsIdentity(t *testing.T) {
	t.Parallel()
	system := newFakeDescriptorSystem()
	identity, err := validate(context.Background(), "/var/lib/groundplane/vol", system)
	if err != nil {
		t.Fatalf("validate() error = %v", err)
	}
	if identity != (Identity{Device: 7, Inode: 11}) {
		t.Fatalf("identity = %#v", identity)
	}
	if system.rootFlags != rootOpenFlags ||
		!reflect.DeepEqual(system.components, []string{"var", "lib", "groundplane", "vol"}) {
		t.Fatalf("root flags/components = %d/%v", system.rootFlags, system.components)
	}
	if len(system.hows) != 4 {
		t.Fatalf("openat2 policies = %d", len(system.hows))
	}
	for _, how := range system.hows {
		if how.Flags != rootOpenFlags || how.Resolve != rootResolve {
			t.Fatalf("openat2 policy = %#v", how)
		}
	}
	if !reflect.DeepEqual(system.closed, []int{10, 11, 12, 13, 14}) {
		t.Fatalf("closed descriptors = %v", system.closed)
	}
}

func TestValidateRejectsUnsafeMetadataResolutionAndCancellation(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name   string
		mutate func(*fakeDescriptorSystem)
		cancel bool
	}{
		{name: "wrong owner", mutate: func(system *fakeDescriptorSystem) { system.stat.Uid = 1000 }},
		{name: "wrong group", mutate: func(system *fakeDescriptorSystem) { system.stat.Gid = 1000 }},
		{name: "wrong mode", mutate: func(system *fakeDescriptorSystem) { system.stat.Mode = unix.S_IFDIR | 0o750 }},
		{name: "wrong type", mutate: func(system *fakeDescriptorSystem) { system.stat.Mode = unix.S_IFREG | 0o700 }},
		{name: "missing component", mutate: func(system *fakeDescriptorSystem) { system.openComponentErr = unix.ENOENT }},
		{name: "unsupported openat2", mutate: func(system *fakeDescriptorSystem) { system.openComponentErr = unix.ENOSYS }},
		{name: "stat failure", mutate: func(system *fakeDescriptorSystem) { system.statErr = unix.EIO }},
		{name: "close failure", mutate: func(system *fakeDescriptorSystem) { system.closeErrAt = 10 }},
		{name: "cancelled", cancel: true},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			system := newFakeDescriptorSystem()
			if test.mutate != nil {
				test.mutate(system)
			}
			ctx := context.Background()
			if test.cancel {
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				ctx = cancelled
			}
			if _, err := validate(ctx, "/var/lib/groundplane/vol", system); err == nil {
				t.Fatal("validate() accepted unsafe root")
			}
		})
	}
}

type fakeDescriptorSystem struct {
	next             int
	rootFlags        int
	components       []string
	hows             []unix.OpenHow
	closed           []int
	stat             unix.Stat_t
	openComponentErr error
	statErr          error
	closeErrAt       int
}

func newFakeDescriptorSystem() *fakeDescriptorSystem {
	return &fakeDescriptorSystem{
		next: 10,
		stat: unix.Stat_t{Dev: 7, Ino: 11, Mode: unix.S_IFDIR | 0o700},
	}
}

func (system *fakeDescriptorSystem) OpenRoot(flags int) (int, error) {
	system.rootFlags = flags
	fd := system.next
	system.next++
	return fd, nil
}

func (system *fakeDescriptorSystem) OpenComponent(
	_ int,
	name string,
	how *unix.OpenHow,
) (int, error) {
	if system.openComponentErr != nil {
		return 0, system.openComponentErr
	}
	system.components = append(system.components, name)
	system.hows = append(system.hows, *how)
	fd := system.next
	system.next++
	return fd, nil
}

func (system *fakeDescriptorSystem) Stat(_ int, stat *unix.Stat_t) error {
	if system.statErr != nil {
		return system.statErr
	}
	*stat = system.stat
	return nil
}

func (system *fakeDescriptorSystem) Close(fd int) error {
	system.closed = append(system.closed, fd)
	if fd == system.closeErrAt {
		return errors.New("close failed")
	}
	return nil
}
