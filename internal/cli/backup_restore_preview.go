package cli

import (
	"encoding/json"
	"fmt"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newBackupRestorePreviewCmd() *cobra.Command {
	var point, identityPath string
	command := &cobra.Command{
		Use:   "preview-restore <source>",
		Short: "Review the exact Restore Point, target versions and compatibility",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			environment, err := backupPolicyEnvironmentID(cmd)
			if err != nil {
				return err
			}
			identity := ""
			if identityPath != "" {
				identity, err = readAgeIdentity(identityPath)
				if err != nil {
					return err
				}
			}
			preview, err := fromContext(
				cmd,
			).Client.PreviewRestoreBackup(
				cmd.Context(),
				environment,
				apiTypes.RestoreRequest{SourceID: args[0], RecoveryPointID: point, AgeIdentity: identity},
			)
			if err != nil {
				return err
			}
			encoded, err := json.MarshalIndent(preview, "", "  ")
			if err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
			if _, err := fmt.Fprintln(cmd.OutOrStdout(), string(encoded)); err != nil {
				return errs.Wrap(errs.KindInternal, err)
			}
			return nil
		},
	}
	command.Flags().StringVar(&point, "point", "", "recovery point id (default: latest)")
	command.Flags().
		StringVar(&identityPath, "age-identity", "", "exported age identity file when the Point uses an earlier era")
	return command
}
