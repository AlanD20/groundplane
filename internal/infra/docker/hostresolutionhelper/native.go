package hostresolutionhelper

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type CapturedState struct {
	Managed  bool
	Baseline bool
}

// InspectCaptured observes only the fixed host resolver path and a retained
// baseline matching this operation's captured digest. No latest baseline wins.
func InspectCaptured(ctx context.Context, expectedSHA256 string) (CapturedState, error) {
	if err := validateContext(ctx); err != nil {
		return CapturedState{}, err
	}
	if !validCapturedDigest(expectedSHA256) {
		return CapturedState{}, errs.New(errs.KindValidationFailed, "captured host resolver baseline digest is invalid")
	}
	baseline, found, err := readBounded(BaselinePath)
	if err != nil {
		return CapturedState{}, err
	}
	defer clear(baseline)
	if found && !matchesBaseline(baseline, expectedSHA256) {
		return CapturedState{}, errs.New(errs.KindStateConflict, "saved host resolver baseline differs from the captured input")
	}
	live, exists, err := readBounded(ResolverMountPath)
	if err != nil {
		return CapturedState{}, err
	}
	defer clear(live)
	if !exists {
		return CapturedState{}, errs.New(errs.KindStateConflict, "host resolver file is missing")
	}
	state := CapturedState{Managed: bytes.Equal(live, managedResolver), Baseline: matchesBaseline(live, expectedSHA256)}
	if state.Managed && !found && !state.Baseline {
		return CapturedState{}, errs.New(errs.KindStateConflict, "local host DNS has no captured baseline for restoration")
	}
	if !state.Managed && !state.Baseline {
		return CapturedState{}, errs.New(errs.KindStateConflict, "host resolver was changed outside the captured operation")
	}
	return state, nil
}

func ApplyCaptured(ctx context.Context, expectedSHA256 string) error {
	state, err := InspectCaptured(ctx, expectedSHA256)
	if err != nil {
		return err
	}
	if state.Managed {
		return nil
	}
	if err := ensureStateDirectory(); err != nil {
		return err
	}
	baseline, found, err := readBounded(BaselinePath)
	if err != nil {
		return err
	}
	clear(baseline)
	if !found {
		live, _, err := readBounded(ResolverMountPath)
		if err != nil {
			return err
		}
		defer clear(live)
		if !matchesBaseline(live, expectedSHA256) {
			return errs.New(errs.KindStateConflict, "host resolver changed before baseline capture")
		}
		if err := writeAtomic(BaselinePath, live, 0o600); err != nil {
			return err
		}
	}
	if err := writeMountedResolver(ctx, managedResolver); err != nil {
		return err
	}
	observed, err := InspectCaptured(ctx, expectedSHA256)
	if err != nil {
		return err
	}
	if !observed.Managed {
		return errs.New(errs.KindStateConflict, "host resolver did not activate local DNS")
	}
	return nil
}

func RestoreCaptured(ctx context.Context, expectedSHA256 string) error {
	state, err := InspectCaptured(ctx, expectedSHA256)
	if err != nil {
		return err
	}
	if state.Baseline {
		return nil
	}
	baseline, found, err := readBounded(BaselinePath)
	if err != nil {
		return err
	}
	defer clear(baseline)
	if !found || !matchesBaseline(baseline, expectedSHA256) {
		return errs.New(errs.KindStateConflict, "captured host resolver baseline is unavailable")
	}
	if err := writeMountedResolver(ctx, baseline); err != nil {
		return err
	}
	observed, err := InspectCaptured(ctx, expectedSHA256)
	if err != nil {
		return err
	}
	if !observed.Baseline {
		return errs.New(errs.KindStateConflict, "host resolver baseline restoration was not observed")
	}
	// Retain the captured baseline independently of Task history and whether
	// CoreDNS is enabled. A later enable must not adopt a new host baseline.
	return nil
}

func matchesBaseline(content []byte, digest string) bool {
	observed := sha256.Sum256(content)
	return hex.EncodeToString(observed[:]) == digest
}

func validCapturedDigest(digest string) bool {
	decoded, err := hex.DecodeString(digest)
	return err == nil && len(decoded) == sha256.Size && hex.EncodeToString(decoded) == digest
}
