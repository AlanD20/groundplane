package cli

import (
	"github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newControllerUpdateCmd() *cobra.Command {
	var release string
	cmd := &cobra.Command{
		Use: "update", Short: "Update the native Controller from a staged immutable release", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !controllerupgrade.Digest(release).Valid() {
				return errs.New(errs.KindValidationFailed, "release must be sha256 followed by 64 lowercase hex digits")
			}
			accepted, err := fromContext(cmd).Client.UpdateController(cmd.Context(), release)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	cmd.Flags().StringVar(&release, "release", "", "digest of the staged release manifest (sha256:…)")
	_ = cmd.MarkFlagRequired("release")
	return withExecutionClass(cmd, executionAPI)
}
