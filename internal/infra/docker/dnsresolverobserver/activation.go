package dnsresolverobserver

import (
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"time"

	"github.com/AlanD20/groundplane-component-sdk/dnsresolver"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type observedConfiguration struct {
	parsed               parsedArtifact
	effectiveDigest      [sha512.Size]byte
	imageDigest          [sha256.Size]byte
	imageConfigAuthority [sha256.Size]byte
	marked               bool
}

// CoreDNS notices atomically replaced files on its reload interval. Missing
// serving evidence is pending; invalid ownership or mounted configuration fails
// immediately. The caller's proof deadline bounds the wait.
func (executor *Executor) awaitConfiguration(ctx context.Context, request Request) (observedConfiguration, error) {
	var marked bool
	for {
		if err := ctx.Err(); err != nil {
			return observedConfiguration{}, configurationWaitError(err, marked)
		}
		configuration, ready, err := executor.inspectConfiguration(ctx, request)
		if err != nil || ready {
			return configuration, err
		}
		marked = configuration.marked
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return observedConfiguration{}, configurationWaitError(ctx.Err(), marked)
		case <-timer.C:
		}
	}
}

func configurationWaitError(err error, marked bool) error {
	if marked {
		return errs.WrapJoined(
			errs.KindStateConflict,
			errs.New(errs.KindStateConflict, "DNS resolver did not serve the captured configuration digest"),
			err,
		)
	}
	return errs.Wrap(errs.KindStateConflict, err)
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
	digest, marked, err := dnsresolver.CorefileServingProof(runtime.artifact)
	if err != nil {
		return result, false, errs.Wrap(errs.KindStateConflict, err)
	}
	result.marked = marked
	var ready bool
	if marked {
		ready = executor.observeServingDigest(ctx, request, digest)
	} else {
		ready, err = executor.inspectLegacyActivation(ctx, request, runtime, result.effectiveDigest)
	}
	if err != nil || !ready {
		return result, false, err
	}
	result.imageDigest = runtime.verifiedImageDigest
	result.imageConfigAuthority = runtime.imageConfigAuthority
	return result, true, nil
}

func (executor *Executor) inspectLegacyActivation(
	ctx context.Context,
	request Request,
	runtime runtimeEvidence,
	expected [sha512.Size]byte,
) (bool, error) {
	if runtime.legacyServingDigest == nil {
		return false, errs.New(errs.KindStateConflict, "DNS resolver startup log evidence is unavailable")
	}
	serving, err := runtime.legacyServingDigest(ctx)
	if err != nil {
		return false, err
	}
	metrics, err := executor.metrics.Read(ctx, request.MetricsURL)
	if err != nil {
		return false, err
	}
	defer clear(metrics)
	metric, present, err := reloadMetricSHA512(metrics, request.ReloadMetric)
	if err != nil {
		return false, err
	}
	// CoreDNS publishes this metric only after a reload, including failed ones.
	// Legacy files require the successful instance-start entry. Fresh startup
	// may have no metric yet.
	return serving != nil && *serving == expected && (!present || metric == expected), nil
}
