package remoteagentplatform

import (
	"context"
	"encoding/json"
	"io"
	"path/filepath"

	"github.com/AlanD20/groundplane/internal/common/config"
	"github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const VerifyInstallArgument = "verify-remote-agent-config"
const installConfigDirectory = "/run/groundplane/remote-agent"

// VerifyInstall is the installer's offline identity proof. It loads the same
// configuration and credential material as startup, without opening Docker,
// networking, or Agent state.
func VerifyInstall(ctx context.Context, configPath string, out io.Writer) error {
	if configPath != filepath.Join(installConfigDirectory, "agent.yaml") {
		return errs.New(errs.KindValidationFailed, "Remote Agent installer requires its private configuration mount")
	}
	cfg := config.DefaultAgentConfig()
	if err := config.Load(ctx, configPath, &cfg); err != nil {
		return err
	}
	if err := config.LoadCompanion(ctx, configPath, "remote-agent", &cfg.Remote); err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	if cfg.Remote == nil {
		return errs.New(errs.KindValidationFailed, "Remote Agent companion configuration is required")
	}
	for _, path := range []string{cfg.Remote.Certificate, cfg.Remote.PrivateKey,
		cfg.Remote.ControllerCACertificate, cfg.Remote.EnrollmentCACertificate} {
		if filepath.Dir(path) != installConfigDirectory {
			return errs.New(
				errs.KindValidationFailed,
				"Remote Agent credentials must be inside the private configuration mount",
			)
		}
	}
	channel, err := AgentChannel(cfg)
	if err != nil {
		return err
	}
	return json.NewEncoder(out).Encode(struct {
		Identity   remotehost.Identity `json:"identity"`
		VolumeRoot string              `json:"volume_root"`
	}{Identity: channel.Identity, VolumeRoot: cfg.Storage.VolumeRoot})
}
