package registryimages

import (
	"context"
	"errors"
	"runtime"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/infra/registryconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/moby/moby/client"
)

// Local opens managed credentials and host clients for each operation. No
// credential snapshot or background client outlives the requesting operation.
type Local struct{}

func (Local) Resolve(ctx context.Context, requested string) (imagefetch.Plan, error) {
	registryClient, err := NewLocal(ctx, strings.HasPrefix(requested, imagefetch.RegistryAuthority+"/"))
	if err != nil {
		return imagefetch.Plan{}, err
	}
	plan, err := registryClient.Resolve(ctx, requested)
	return plan, errors.Join(err, registryClient.Close())
}

func (Local) Fetch(ctx context.Context, plan imagefetch.Plan, report imagefetch.Reporter) (string, error) {
	registryClient, err := NewLocal(ctx, strings.HasPrefix(plan.Repository, imagefetch.RegistryAuthority+"/"))
	if err != nil {
		return "", err
	}
	imageID, err := registryClient.Fetch(ctx, plan, report)
	return imageID, errors.Join(err, registryClient.Close())
}

// NewLocal owns the host Docker client and uses only installer-managed registry
// credentials and trust. Its caller must Close it after the operation finishes.
func NewLocal(ctx context.Context, managed bool) (*Client, error) {
	var certificate []byte
	var username, password string
	if managed {
		settings, err := registryconfiguration.Read(ctx)
		if err != nil {
			return nil, err
		}
		certificate, username, password = settings.Certificate, settings.Username, settings.Password
	}
	engine, err := client.New(client.WithHost("unix:///var/run/docker.sock"))
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	registryClient, err := New(engine, certificate, username, password, runtime.GOARCH)
	if err != nil {
		return nil, errors.Join(err, engine.Close())
	}
	registryClient.ownedEngine = engine
	return registryClient, nil
}
