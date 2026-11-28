package config

import (
	"context"
	"encoding/hex"
	"net/netip"
	"os"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imageref"
	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublicIngressConfig identifies the local Host and its independently
// provisioned transport. Main Controller and Agent YAML remain unchanged.
type PublicIngressConfig struct {
	HostID string                   `yaml:"host_id"`
	Bridge string                   `yaml:"bridge_id"`
	Image  string                   `yaml:"caddy_image"`
	Relay  PublicIngressRelayConfig `yaml:"relay"`
}

type PublicIngressRelayConfig struct {
	Image   string     `yaml:"image"`
	Gateway netip.Addr `yaml:"gateway"`
	EdgeIP  netip.Addr `yaml:"edge_ip"`
	Key     string     `yaml:"key"`
}

func (relay PublicIngressRelayConfig) Listen() netip.AddrPort {
	return netip.AddrPortFrom(relay.Gateway, ingressexecution.RelayPort)
}

func (c PublicIngressConfig) Validate() error {
	if ids.Validate(ids.KindHost, c.HostID) != nil || len(c.Bridge) != 64 ||
		c.Bridge != strings.ToLower(c.Bridge) || !imageref.IsDigestPinned(c.Image) ||
		!strings.HasPrefix(c.Image, "docker.io/library/caddy@sha256:") {
		return errs.New(
			errs.KindValidationFailed,
			"public ingress requires an exact Host, Docker bridge, and pinned Caddy image",
		)
	}
	if _, err := hex.DecodeString(c.Bridge); err != nil {
		return errs.New(errs.KindValidationFailed, "public ingress Docker bridge identity is invalid")
	}
	r := c.Relay
	key, err := hex.DecodeString(r.Key)
	defer clear(key)
	if err != nil || len(key) != 32 || strings.Trim(r.Key, "0") == "" || !imageref.IsDigestPinned(r.Image) ||
		!r.Gateway.Is4() || !r.Gateway.IsPrivate() || !r.EdgeIP.Is4() || !r.EdgeIP.IsPrivate() ||
		r.Gateway == r.EdgeIP {
		return errs.New(
			errs.KindValidationFailed,
			"public ingress relay needs a pinned image, dedicated addresses and private key",
		)
	}
	return nil
}

// LoadPublicIngress loads only the feature companion document. The managed
// Agent can read the host-persisted companion through its existing state mount.
func LoadPublicIngress(ctx context.Context, mainPath string) (*PublicIngressConfig, error) {
	info, err := os.Lstat(mainPath + ".public-ingress.yaml")
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return nil, errs.New(errs.KindStateConflict, "public ingress companion must be a private regular file")
	}
	var cfg *PublicIngressConfig
	err = LoadCompanion(ctx, mainPath, "public-ingress", &cfg)
	if err != nil {
		return nil, err
	}
	if cfg == nil {
		return nil, errs.New(errs.KindValidationFailed, "public ingress companion is empty")
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}
