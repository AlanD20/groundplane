package registryimages

import (
	"context"
	"errors"
	"runtime"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolvePublicImage resolves registry metadata for the selected Host platform
// without inspecting the Controller's Docker daemon.
func (Local) ResolvePublicImage(ctx context.Context, requested, architecture string) (imagefetch.Plan, error) {
	ref, err := remotefetch.Reference(requested)
	if err != nil {
		return imagefetch.Plan{}, err
	}
	registry, err := newClient(nil, nil, "", "", architecture)
	if err != nil {
		return imagefetch.Plan{}, err
	}
	defer registry.Close()
	plan, err := registry.Resolve(ctx, ref)
	if err != nil {
		return imagefetch.Plan{}, err
	}
	return plan, remotefetch.ValidatePlan(plan)
}

// FetchPublicImage downloads the exact manifest without registry credentials.
// After a lost execution result, the Agent calls only ObservePublicImage.
func (Local) FetchPublicImage(ctx context.Context, plan imagefetch.Plan) (string, error) {
	return publicImage(ctx, plan, true)
}

func (Local) ObservePublicImage(ctx context.Context, plan imagefetch.Plan) (string, error) {
	return publicImage(ctx, plan, false)
}

func publicImage(ctx context.Context, plan imagefetch.Plan, fetch bool) (identity string, failure error) {
	if err := remotefetch.ValidatePlan(plan); err != nil {
		return "", err
	}
	if plan.Architecture != runtime.GOARCH {
		return "", errs.New(errs.KindStateConflict, "Remote Fetch captured another Host platform")
	}
	registry, err := NewLocal(ctx, false)
	if err != nil {
		return "", err
	}
	defer func() { failure = errors.Join(failure, registry.Close()) }()
	if fetch {
		if _, err := registry.Fetch(ctx, plan, func(imagefetch.Progress) error { return ctx.Err() }); err != nil {
			return "", err
		}
	}
	actual, err := registry.engine.ImageInspect(ctx, plan.Reference())
	if err != nil {
		return "", err
	}
	if _, err := verifyImage(actual, plan); err != nil {
		return "", err
	}
	return actual.ID, nil
}
