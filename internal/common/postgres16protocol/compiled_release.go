package postgres16protocol

import (
	"encoding/base64"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Set by the release packager. It is part of the authenticated Controller
// binary, never operator configuration or a file copied from a workload.
var releaseIndexBase64 string

func CompiledManagedRelease() (ManagedReleaseIndex, error) {
	if releaseIndexBase64 == "" {
		return ManagedReleaseIndex{}, errs.New(errs.KindStrategyNotImplemented,
			"this build does not contain a managed PostgreSQL backup release")
	}
	if len(releaseIndexBase64) > 21848 {
		return ManagedReleaseIndex{}, errs.New(errs.KindInternal, "managed PostgreSQL release exceeds its bound")
	}
	data, err := base64.StdEncoding.Strict().DecodeString(releaseIndexBase64)
	if err != nil {
		return ManagedReleaseIndex{}, errs.Wrap(errs.KindInternal, err)
	}
	return DecodeManagedReleaseIndex(data)
}
