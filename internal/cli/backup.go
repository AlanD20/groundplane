package cli

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// backup: policy show | policy set [...] | run | points | restore
// <source> | rotate-key | export-key. Per-environment, toggleable off,
// fully selectable sources. See mvp.md, "Backup", and blueprint.md,
// "x-gp-backup".
func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "backup", Short: "Backup policy, runs, recovery points, and restores"}

	policy := &cobra.Command{Use: "policy", Short: "The environment's one backup policy"}
	policy.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show the backup policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runShow(cmd, "/api/v1/environments/"+target(app, app.Scope.Environment)+"/backup-policy")
		},
	})

	var frequency string
	var keep int
	var encryption string
	var sources []string
	var off bool
	set := &cobra.Command{
		Use:   "set",
		Short: "Set the backup policy",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			path := "/api/v1/environments/" + target(app, app.Scope.Environment) + "/backup-policy"
			return runEdit(cmd, path, map[string]interface{}{
				"frequency": frequency, "keep": keep, "encryption": encryption,
				"sources": sources, "enabled": !off,
			})
		},
	}
	set.Flags().StringVar(&frequency, "frequency", "", "systemd calendar expression, e.g. '*-*-* 03:15:00'")
	set.Flags().IntVar(&keep, "keep", 0, "how many previous recovery points to retain")
	set.Flags().StringVar(&encryption, "encryption", "age", "age | none")
	set.Flags().StringSliceVar(&sources, "source", nil, "repeatable: <attach-id> | volume:<name> | config")
	set.Flags().BoolVar(&off, "off", false, "disable backups for this environment (policy and sources are kept, just don't run)")
	policy.AddCommand(set)
	cmd.AddCommand(policy)

	var runSources []string
	run := &cobra.Command{
		Use:   "run",
		Short: "Run a backup now",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			path := "/api/v1/environments/" + target(app, app.Scope.Environment) + "/backup-run"
			return runAction(cmd, path, map[string]interface{}{"source_ids": runSources})
		},
	}
	run.Flags().StringSliceVar(&runSources, "source", nil, "restrict to specific source id(s) (default: every enabled source)")
	cmd.AddCommand(run)

	points := &cobra.Command{
		Use:   "points",
		Short: "List recovery points (visible only after dump/encrypt/upload/verify all succeed)",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runList(cmd, "/api/v1/environments/"+target(app, app.Scope.Environment)+"/recovery-points", nil)
		},
	}
	cmd.AddCommand(points)

	var point, ageIdentityPath string
	restore := &cobra.Command{
		Use:   "restore <source>",
		Short: "Restore a source from a recovery point (defaults to the latest)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			ageIdentity := ""
			if ageIdentityPath != "" {
				var err error
				ageIdentity, err = readAgeIdentity(ageIdentityPath)
				if err != nil {
					return err
				}
			}
			path := "/api/v1/environments/" + target(app, app.Scope.Environment) + "/restore"
			return runAction(cmd, path, map[string]string{
				"source_id": args[0], "recovery_point_id": point, "age_identity": ageIdentity,
			})
		},
	}
	restore.Flags().StringVar(&point, "point", "", "recovery point id (default: latest)")
	restore.Flags().StringVar(&ageIdentityPath, "age-identity", "", "exported age identity file (needed if the recovery point's key era was rotated away)")
	cmd.AddCommand(restore)

	cmd.AddCommand(&cobra.Command{
		Use:   "rotate-key",
		Short: "Rotate the environment's backup encryption age key (bumps key_era; affects new recovery points only)",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runAction(cmd, "/api/v1/environments/"+target(app, app.Scope.Environment)+"/rotate-key", nil)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "export-key",
		Short: "Export the current age identity for off-host disaster recovery",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runReveal(cmd, "/api/v1/environments/"+target(app, app.Scope.Environment)+"/export-key")
		},
	})

	return cmd
}

func readAgeIdentity(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("backup: open age identity: %w", err)
	}
	defer file.Close()
	const maxIdentitySize = 4096
	contents, err := io.ReadAll(io.LimitReader(file, maxIdentitySize+1))
	if err != nil {
		return "", fmt.Errorf("backup: read age identity: %w", err)
	}
	if len(contents) > maxIdentitySize {
		return "", fmt.Errorf("backup: age identity exceeds %d bytes", maxIdentitySize)
	}
	identity := strings.TrimSpace(string(contents))
	if identity == "" {
		return "", fmt.Errorf("backup: age identity is empty")
	}
	return identity, nil
}
