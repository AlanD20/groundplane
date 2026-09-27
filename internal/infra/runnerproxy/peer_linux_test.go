package runnerproxy

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RUN-04: all real/effective/saved/fs identities and confinement properties
// must agree; a matching effective UID alone is not process authorization.
func TestProcessConfinementRejectsPartialOrContradictoryIdentity(t *testing.T) {
	valid := "Uid:\t200000\t200000\t200000\t200000\nGid:\t200000\t200000\t200000\t200000\n" +
		"NoNewPrivs:\t1\nSeccomp:\t2\nCapEff:\t0000000000000000\n" +
		"CapPrm:\t0000000000000000\nCapBnd:\t0000000000000000\n"
	if !confinedProcessStatus(valid, 200000, 200000) {
		t.Fatal("confined process was rejected")
	}
	for _, changed := range []string{
		strings.Replace(valid, "Uid:\t200000", "Uid:\t0", 1),
		strings.Replace(valid, "Gid:\t200000", "Gid:\t0", 1),
		strings.Replace(valid, "NoNewPrivs:\t1", "NoNewPrivs:\t0", 1),
		strings.Replace(valid, "Seccomp:\t2", "Seccomp:\t0", 1),
		strings.Replace(valid, "CapEff:\t0000000000000000", "CapEff:\t0000000000000001", 1),
		strings.Replace(valid, "CapPrm:\t0000000000000000", "CapPrm:\t0000000000000001", 1),
		strings.Replace(valid, "CapBnd:\t0000000000000000\n", "", 1),
		valid + "NoNewPrivs:\t0\n",
	} {
		if confinedProcessStatus(changed, 200000, 200000) {
			t.Fatal("unconfined or contradictory process was accepted")
		}
	}
}

// RUN-04: observing an arbitrary local process cannot mint a Runner receipt.
// This is a real proc/pidfd rejection check, not positive container qualification.
func TestPeerAuthorityCannotAdoptAnUnrelatedLocalProcess(t *testing.T) {
	_, err := CapturePeerAuthority(
		context.Background(), ids.New(ids.KindRunner), 1, strings.Repeat("a", 64),
		int32(os.Getpid()), 200000, 200000,
	)
	if !errors.Is(err, errs.New(errs.KindScopeUnauthorized, "")) {
		t.Fatalf("unrelated process authority: %v", err)
	}
	root := "/proc/" + strconv.Itoa(os.Getpid()) + "/"
	before, err := processStart(root)
	if err != nil {
		t.Fatal(err)
	}
	after, err := processStart(root)
	if err != nil || before != after || before == 0 {
		t.Fatal("same live process has no stable start identity")
	}
}
