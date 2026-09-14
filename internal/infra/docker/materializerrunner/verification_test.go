package materializerrunner

import (
	"bytes"
	"errors"
	"io"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// SVC-15/JOURNEY-02: a postcondition probe cannot mutate the mounted
// configuration, and drift must remain distinct from a broken helper.
func TestVerificationHelperUsesReadOnlyAuthorityAndDistinctDrift(t *testing.T) {
	t.Parallel()
	for _, status := range []int64{0, 1, 2} {
		fake := &fakeEngine{attachConn: &recordingConn{}, waitStatus: status}
		err := newTestRunner(fake).Run(t.Context(), Request{
			VolumeDir: testVolumeDir, Stream: io.NopCloser(bytes.NewReader([]byte("frame"))), VerifyOnly: true,
		})
		switch status {
		case 0:
			if err != nil {
				t.Fatal(err)
			}
		case 1:
			if !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("helper failure: %v", err)
			}
		case 2:
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("file drift: %v", err)
			}
		}
		options := fake.createOptions
		if !slices.Equal(options.Config.Cmd, []string{"verify-materialization"}) ||
			!slices.Equal(options.HostConfig.CapAdd, []string{"DAC_READ_SEARCH"}) ||
			!slices.Equal(options.HostConfig.CapDrop, []string{"ALL"}) ||
			len(options.HostConfig.Mounts) != 1 || !options.HostConfig.Mounts[0].ReadOnly ||
			options.HostConfig.Mounts[0].Source != testVolumeDir || !options.HostConfig.ReadonlyRootfs {
			t.Fatal("verification helper received mutation authority")
		}
	}
}
