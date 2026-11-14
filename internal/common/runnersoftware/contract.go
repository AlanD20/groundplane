// Package runnersoftware defines pinned upstream GitHub Runner updates.
package runnersoftware

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerconnectioncheck"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Release struct {
	Version      string `json:"version"`
	Architecture string `json:"architecture"`
	URL          string `json:"url"`
	SHA256       string `json:"sha256"`
	Size         int64  `json:"size"`
}

func ValidVersion(value string) bool {
	parts := strings.Split(value, ".")
	if len(parts) != 3 || len(value) > 32 {
		return false
	}
	for _, part := range parts {
		n, err := strconv.ParseUint(part, 10, 32)
		if err != nil || strconv.FormatUint(n, 10) != part {
			return false
		}
	}
	return true
}

func Newer(candidate, installed string) bool {
	if !ValidVersion(candidate) || !ValidVersion(installed) {
		return false
	}
	a, b := strings.Split(candidate, "."), strings.Split(installed, ".")
	for i := range a {
		x, _ := strconv.ParseUint(a[i], 10, 32)
		y, _ := strconv.ParseUint(b[i], 10, 32)
		if x != y {
			return x > y
		}
	}
	return false
}

func (release Release) Validate() error {
	arch := map[string]string{"amd64": "x64", "arm64": "arm64"}[release.Architecture]
	digest, err := hex.DecodeString(release.SHA256)
	want := fmt.Sprintf(
		"https://github.com/actions/runner/releases/download/v%s/actions-runner-linux-%s-%s.tar.gz",
		release.Version,
		arch,
		release.Version,
	)
	if !ValidVersion(release.Version) || arch == "" || release.URL != want || err != nil || len(digest) != 32 ||
		strings.ToLower(release.SHA256) != release.SHA256 ||
		release.Size <= 0 ||
		release.Size > 512<<20 {
		return errs.New(
			errs.KindValidationFailed,
			"GitHub Runner release must have an official architecture-matched download and SHA256 checksum",
		)
	}
	return nil
}

type Request struct {
	RequestID          string  `json:"request_id"`
	RunnerID           string  `json:"runner_id"`
	RuntimeEpoch       uint64  `json:"runtime_epoch"`
	PlanSHA256         string  `json:"plan_sha256"`
	ControllerEndpoint string  `json:"controller_endpoint"`
	RunnerHome         string  `json:"runner_home"`
	Release            Release `json:"release"`
}

func (request Request) Validate() error {
	base := runnerconnectioncheck.Request{
		RequestID:          request.RequestID,
		RunnerID:           request.RunnerID,
		RuntimeEpoch:       request.RuntimeEpoch,
		PlanSHA256:         request.PlanSHA256,
		ControllerEndpoint: request.ControllerEndpoint,
		RunnerHome:         request.RunnerHome,
	}
	if err := base.Validate(); err != nil {
		return err
	}
	return request.Release.Validate()
}

type Stage struct {
	Name    string `json:"name"`
	Outcome string `json:"outcome"`
	Failure string `json:"failure,omitempty"`
}
type Result struct {
	RequestID    string    `json:"request_id"`
	RunnerID     string    `json:"runner_id"`
	RuntimeEpoch uint64    `json:"runtime_epoch"`
	PlanSHA256   string    `json:"plan_sha256"`
	ObservedAt   time.Time `json:"observed_at"`
	Version      string    `json:"version"`
	Stages       []Stage   `json:"stages"`
}

func (result Result) Validate(request Request) error {
	if request.Validate() != nil || result.RequestID != request.RequestID || result.RunnerID != request.RunnerID ||
		result.RuntimeEpoch != request.RuntimeEpoch ||
		result.PlanSHA256 != request.PlanSHA256 ||
		result.Version != request.Release.Version ||
		result.ObservedAt.IsZero() ||
		len(result.Stages) != 4 {
		return errs.New(errs.KindValidationFailed, "Runner update result differs from its pinned release")
	}
	for i, name := range []string{"download", "idle", "activate", "reconnect"} {
		if result.Stages[i].Name != name || result.Stages[i].Outcome != "passed" || result.Stages[i].Failure != "" {
			return errs.New(errs.KindValidationFailed, "Runner update has incomplete verification")
		}
	}
	return nil
}
