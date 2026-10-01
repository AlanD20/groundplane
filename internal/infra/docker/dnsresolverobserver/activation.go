package dnsresolverobserver

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type observedConfiguration struct {
	parsed               parsedArtifact
	effectiveDigest      [sha512.Size]byte
	imageDigest          [sha256.Size]byte
	imageConfigAuthority [sha256.Size]byte
}

// CoreDNS notices atomically replaced files on its reload interval. Only valid
// but absent or stale serving evidence is pending; ownership, bytes and malformed
// evidence still fail immediately. The caller's proof deadline bounds the wait.
func (executor *Executor) awaitConfiguration(ctx context.Context, request Request) (observedConfiguration, error) {
	for {
		if err := ctx.Err(); err != nil {
			return observedConfiguration{}, errs.Wrap(errs.KindStateConflict, err)
		}
		configuration, ready, err := executor.inspectConfiguration(ctx, request)
		if err != nil || ready {
			return configuration, err
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return observedConfiguration{}, errs.Wrap(errs.KindStateConflict, ctx.Err())
		case <-timer.C:
		}
	}
}

func (executor *Executor) inspectConfiguration(
	ctx context.Context, request Request,
) (observedConfiguration, bool, error) {
	var result observedConfiguration
	runtime, err := executor.runtime.Inspect(ctx, request)
	if err != nil {
		return result, false, err
	}
	defer clear(runtime.artifact)
	if sha256.Sum256(runtime.artifact) != request.ArtifactSHA256 {
		return result, false, errs.New(errs.KindStateConflict, "DNS resolver mounted artifact digest changed")
	}
	result.parsed, err = parseArtifact(runtime.artifact)
	if err != nil {
		return result, false, err
	}
	result.effectiveDigest, err = effectiveConfigSHA512(request.ArtifactTarget, runtime.artifact)
	if err != nil {
		return result, false, err
	}
	metrics, err := executor.metrics.Read(ctx, request.MetricsURL)
	if err != nil {
		return result, false, err
	}
	defer clear(metrics)
	metric, present, err := reloadMetricSHA512(metrics, request.ReloadMetric)
	if err != nil {
		return result, false, err
	}
	// CoreDNS publishes this metric only after a reload, including failed ones.
	// The successful instance-start log is always required; a metric alone can
	// never establish serving authority. Fresh startup has no metric yet.
	if runtime.servingConfiguration == nil || *runtime.servingConfiguration != result.effectiveDigest ||
		(present && metric != result.effectiveDigest) {
		return result, false, nil
	}
	result.imageDigest = runtime.verifiedImageDigest
	result.imageConfigAuthority = runtime.imageConfigAuthority
	return result, true, nil
}
