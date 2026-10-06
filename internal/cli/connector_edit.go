package cli

import (
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

func newConnectorEditCmd() *cobra.Command {
	var name, endpoint, bucket, prefix, region string
	var accessSecret, accessFile, secretSecret, secretFile string
	var pathStyle, virtualStyle bool
	cmd := &cobra.Command{
		Use: "edit <name>", Short: "Edit connector settings or rotate credentials", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveConnectorTarget(cmd, args[0])
			if err != nil {
				return err
			}
			input := apiTypes.ConnectorEditRequest{}
			for _, field := range []struct {
				name   string
				value  *string
				target **string
			}{
				{"name", &name, &input.Name}, {"endpoint", &endpoint, &input.Endpoint},
				{"bucket", &bucket, &input.Bucket}, {"prefix", &prefix, &input.Prefix}, {"region", &region, &input.Region},
			} {
				if cmd.Flags().Changed(field.name) {
					*field.target = field.value
				}
			}
			if cmd.Flags().Changed("path-style") || cmd.Flags().Changed("virtual-hosted-style") {
				addressing, err := connectorPathStyle(cmd, pathStyle, virtualStyle)
				if err != nil {
					return err
				}
				input.PathStyle = &addressing
			}
			if accessFile == "-" && secretFile == "-" {
				return errs.New(errs.KindValidationFailed, "only one connector credential may read from stdin")
			}
			input.Credentials = make(map[string]apiTypes.ConnectorCredentialInput, 2)
			defer clearConnectorCredentialInputs(input.Credentials)
			for _, credential := range []struct{ name, ref, file string }{
				{"access_key", accessSecret, accessFile}, {"secret_key", secretSecret, secretFile},
			} {
				if credential.ref == "" && credential.file == "" {
					continue
				}
				value, err := connectorCredentialInput(
					cmd.InOrStdin(),
					credential.name,
					credential.ref,
					credential.file,
				)
				if err != nil {
					return err
				}
				input.Credentials[credential.name] = value
			}
			if input.Name == nil && input.Endpoint == nil && input.Bucket == nil && input.Prefix == nil &&
				input.Region == nil &&
				input.PathStyle == nil &&
				len(input.Credentials) == 0 {
				return errs.New(
					errs.KindValidationFailed,
					"connector edit requires at least one changed setting or credential",
				)
			}
			connector, err := fromContext(cmd).Client.EditConnector(cmd.Context(), id, input)
			if err != nil {
				return err
			}
			return renderConnector(cmd, connector)
		},
	}
	cmd.Flags().StringVar(&name, "name", "", "new connector name")
	cmd.Flags().StringVar(&endpoint, "endpoint", "", "absolute S3-compatible endpoint URL")
	cmd.Flags().StringVar(&bucket, "bucket", "", "bucket name")
	cmd.Flags().StringVar(&prefix, "prefix", "", "relative object-key prefix; empty clears it")
	cmd.Flags().StringVar(&region, "region", "", "explicit region")
	cmd.Flags().BoolVar(&pathStyle, "path-style", false, "use path-style S3 addressing")
	cmd.Flags().BoolVar(&virtualStyle, "virtual-hosted-style", false, "use virtual-hosted-style S3 addressing")
	cmd.Flags().StringVar(&accessSecret, "access-key-secret", "", "env_var Secret key for access_key")
	cmd.Flags().StringVar(&accessFile, "access-key-value-file", "", "read replacement access_key from PATH, or -")
	cmd.Flags().StringVar(&secretSecret, "secret-key-secret", "", "env_var Secret key for secret_key")
	cmd.Flags().StringVar(&secretFile, "secret-key-value-file", "", "read replacement secret_key from PATH, or -")
	return cmd
}
