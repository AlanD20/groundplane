package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/localdiag"
	ageinfra "github.com/AlanD20/groundplane/internal/infra/age"
	"github.com/AlanD20/groundplane/internal/infra/docker/etcdcontainer"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type controllerRecipientReader func(context.Context, string) (string, error)
type controllerEndpointProber func(context.Context, []string) ([]localdiag.EtcdEndpoint, error)

// InspectControllerKey reads the existing key selected by controller.yaml and
// returns only its path and fingerprint.
func InspectControllerKey(ctx context.Context) (localdiag.ControllerKey, error) {
	return inspectControllerKeyFromConfig(ctx, ControllerConfigPath(), ageinfra.ReadControllerRecipient)
}

func inspectControllerKeyFromConfig(
	ctx context.Context,
	configPath string,
	readRecipient controllerRecipientReader,
) (localdiag.ControllerKey, error) {
	cfg, err := loadControllerConfig(ctx, configPath)
	if err != nil {
		return localdiag.ControllerKey{}, err
	}
	return inspectControllerKey(ctx, cfg.AgeKeyPath, readRecipient)
}

func inspectControllerKey(
	ctx context.Context,
	path string,
	readRecipient controllerRecipientReader,
) (localdiag.ControllerKey, error) {
	recipient, err := readRecipient(ctx, path)
	if err != nil {
		return localdiag.ControllerKey{}, err
	}
	digest := sha256.Sum256([]byte(recipient))
	return localdiag.ControllerKey{
		Path:        path,
		Fingerprint: "sha256:" + hex.EncodeToString(digest[:]),
	}, nil
}

// InspectControllerEtcd probes the Controller-managed loopback endpoint
// directly instead of consulting the Controller's REST host projection.
func InspectControllerEtcd(ctx context.Context) ([]localdiag.EtcdEndpoint, error) {
	return inspectControllerEtcdFromConfig(ctx, ControllerConfigPath(), etcd.ProbeEndpoints)
}

func inspectControllerEtcdFromConfig(
	ctx context.Context,
	configPath string,
	probe controllerEndpointProber,
) ([]localdiag.EtcdEndpoint, error) {
	if _, err := loadControllerConfig(ctx, configPath); err != nil {
		return nil, err
	}
	return probe(ctx, etcdcontainer.Endpoints())
}

func loadControllerConfig(ctx context.Context, path string) (config.ControllerConfig, error) {
	cfg := config.DefaultControllerConfig()
	if err := config.Load(ctx, path, &cfg); err != nil {
		return config.ControllerConfig{}, err
	}
	if err := cfg.Validate(); err != nil {
		return config.ControllerConfig{}, err
	}
	return cfg, nil
}
