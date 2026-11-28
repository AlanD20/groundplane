package runner

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"golang.org/x/sys/unix"
)

// VerifyLegacyGitHubRegistration corroborates a provider-read identity with
// the registration on the exact host slot. The local file alone never grants
// a GitHub identity and its content is neither returned nor logged.
func (control *HostControl) VerifyLegacyGitHubRegistration(
	ctx context.Context,
	plan corerunner.Plan,
	githubID int64,
) error {
	if err := validateCall(ctx, plan); err != nil {
		return err
	}
	if githubID <= 0 || plan.Egress.ScopedEndpoint.IsValid() {
		return errs.New(errs.KindValidationFailed, "Runner capture has no legacy GitHub identity")
	}
	if control.local == nil {
		return errs.New(errs.KindStateConflict, "Runner registration proof requires the owning host")
	}
	if err := observeSlotClaim(plan); err != nil {
		return err
	}
	if err := registrationPresent(plan); err != nil {
		return err
	}
	file, err := os.OpenFile(
		filepath.Join(plan.Paths.RunnerHome, ".runner"),
		os.O_RDONLY|unix.O_NOFOLLOW|unix.O_NONBLOCK,
		0,
	)
	if err != nil {
		return errs.New(errs.KindStateConflict, "Runner registration identity cannot be verified")
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !ownedLegacyRegistryPath(info, plan, false) {
		return errs.New(errs.KindStateConflict, "Runner registration ownership changed")
	}
	const maximumRegistration = 64 << 10
	value, err := io.ReadAll(io.LimitReader(file, maximumRegistration+1))
	defer clear(value)
	var registration struct {
		ID   int64  `json:"agentId"`
		Name string `json:"agentName"`
	}
	if err != nil || len(value) > maximumRegistration || json.Unmarshal(value, &registration) != nil ||
		registration.ID != githubID || registration.Name != plan.Container.RunnerName {
		return errs.New(
			errs.KindStateConflict,
			"Installed Runner registration differs from the verified GitHub identity",
		)
	}
	return nil
}

func (control *HostControl) WaitLegacyIdle(
	ctx context.Context,
	plan corerunner.Plan,
	expected runnerallocation.RunnerRuntimeEvidence,
) error {
	if err := validateCall(ctx, plan); err != nil {
		return err
	}
	if control.local == nil || plan.Egress.ScopedEndpoint.IsValid() || !expected.Valid() {
		return errs.New(errs.KindStateConflict, "Runner idle check requires captured legacy ownership")
	}
	updater := SoftwareUpdater{operations: control.local}
	return updater.WaitIdle(ctx, plan, expected)
}
