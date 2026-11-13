package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/spf13/cobra"
)

func newSecretEditCmd(platform *bool) *cobra.Command {
	var file, key string
	var revision int64
	cmd := &cobra.Command{Use: "edit <key>", Short: "Replace a Secret value while preserving its stable reference", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		id, err := resolveSecretTarget(cmd, args[0], *platform)
		if err != nil {
			return err
		}
		app := fromContext(cmd)
		selected := revision
		if selected == 0 {
			current, err := app.Client.ShowSecret(cmd.Context(), id)
			if err != nil {
				return err
			}
			selected = current.Revision
		}
		value, err := readSecretValue(file, cmd.InOrStdin())
		if err != nil {
			return err
		}
		secret, err := app.Client.EditSecret(cmd.Context(), id, apiTypes.SecretEditRequest{Revision: selected, Value: value}, key)
		value = ""
		if err != nil {
			return err
		}
		return renderSecret(cmd, secret)
	}}
	cmd.Flags().StringVar(&file, "value-file", "", "read replacement value from PATH or - for stdin")
	_ = cmd.MarkFlagRequired("value-file")
	cmd.Flags().Int64Var(&revision, "revision", 0, "exact metadata revision; defaults to a fresh Secret read")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "reuse with the same revision and value to resolve uncertain acceptance")
	return cmd
}
