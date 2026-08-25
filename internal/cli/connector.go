package cli

import (
	"io"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// connector: list | add | show | remove.
func newConnectorCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "connector", Short: "Environment S3-compatible backup connectors"}
	cmd.AddCommand(&cobra.Command{
		Use: "list", Short: "List connectors", Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			environmentID, err := connectorEnvironmentID(cmd)
			if err != nil {
				return err
			}
			page, err := fromContext(cmd).Client.ListConnectors(cmd.Context(), environmentID, 0, "")
			if err != nil {
				return err
			}
			items := make([]map[string]any, len(page.Items))
			for index, connector := range page.Items {
				items[index] = connectorFields(connector)
			}
			headers, rows := tabulateVia(fromContext(cmd), items)
			return fromContext(cmd).Out.Render(headers, rows, page)
		},
	})

	var endpoint, bucket, prefix, region string
	var accessKeySecret, accessKeyValueFile, secretKeySecret, secretKeyValueFile string
	var pathStyle, virtualHostedStyle bool
	add := &cobra.Command{
		Use: "add <name>", Short: "Add an S3-compatible connector", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			environmentID, err := connectorEnvironmentID(cmd)
			if err != nil {
				return err
			}
			addressing, err := connectorPathStyle(cmd, pathStyle, virtualHostedStyle)
			if err != nil {
				return err
			}
			if endpoint == "" || bucket == "" || region == "" {
				return errs.New(
					errs.KindValidationFailed,
					"connector add requires --endpoint, --bucket, and --region",
				)
			}
			if accessKeyValueFile == "-" && secretKeyValueFile == "-" {
				return errs.New(
					errs.KindValidationFailed,
					"only one Connector credential may read from stdin",
				)
			}
			accessKey, err := connectorCredentialInput(
				cmd.InOrStdin(), "access key", accessKeySecret, accessKeyValueFile,
			)
			if err != nil {
				return err
			}
			secretKey, err := connectorCredentialInput(
				cmd.InOrStdin(), "secret key", secretKeySecret, secretKeyValueFile,
			)
			if err != nil {
				accessKey.Value = ""
				return err
			}
			credentials := map[string]apiTypes.ConnectorCredentialInput{
				"access_key": accessKey, "secret_key": secretKey,
			}
			defer clearConnectorCredentialInputs(credentials)
			connector, err := fromContext(cmd).Client.CreateConnector(
				cmd.Context(),
				environmentID,
				apiTypes.ConnectorCreateRequest{
					Name: args[0], Kind: "s3-compatible", Endpoint: endpoint, Bucket: bucket,
					Prefix: prefix, Region: region, PathStyle: &addressing, Credentials: credentials,
				},
			)
			if err != nil {
				return err
			}
			return renderConnector(cmd, connector)
		},
	}
	add.Flags().StringVar(&endpoint, "endpoint", "", "absolute S3-compatible endpoint URL")
	add.Flags().StringVar(&bucket, "bucket", "", "bucket name")
	add.Flags().StringVar(&prefix, "prefix", "", "optional relative object-key prefix")
	add.Flags().StringVar(&region, "region", "", "region, including auto when selected explicitly")
	add.Flags().BoolVar(&pathStyle, "path-style", false, "use path-style S3 addressing")
	add.Flags().BoolVar(&virtualHostedStyle, "virtual-hosted-style", false, "use virtual-hosted-style S3 addressing")
	add.Flags().StringVar(&accessKeySecret, "access-key-secret", "", "env_var Secret key for access_key")
	add.Flags().StringVar(&accessKeyValueFile, "access-key-value-file", "", "read direct access_key from PATH, or -")
	add.Flags().StringVar(&secretKeySecret, "secret-key-secret", "", "env_var Secret key for secret_key")
	add.Flags().StringVar(&secretKeyValueFile, "secret-key-value-file", "", "read direct secret_key from PATH, or -")
	cmd.AddCommand(add)

	cmd.AddCommand(&cobra.Command{
		Use: "show <name>", Short: "Show connector metadata", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveConnectorTarget(cmd, args[0])
			if err != nil {
				return err
			}
			connector, err := fromContext(cmd).Client.ShowConnector(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderConnector(cmd, connector)
		},
	})
	cmd.AddCommand(&cobra.Command{
		Use: "remove <name>", Aliases: []string{"rm", "delete", "del"},
		Short: "Remove a connector (dispatches a task)", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := resolveConnectorTarget(cmd, args[0])
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.RemoveConnector(cmd.Context(), id)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	})
	return cmd
}

func connectorEnvironmentID(cmd *cobra.Command) (string, error) {
	argument := fromContext(cmd).Scope.Environment
	if argument == "" {
		return "", errs.New(errs.KindValidationFailed, "connector command requires --env")
	}
	return resolveEnvironmentTarget(cmd, argument)
}

func connectorPathStyle(cmd *cobra.Command, pathStyle bool, virtualHostedStyle bool) (bool, error) {
	pathChanged := cmd.Flags().Changed("path-style")
	virtualChanged := cmd.Flags().Changed("virtual-hosted-style")
	if pathChanged == virtualChanged || pathChanged && !pathStyle || virtualChanged && !virtualHostedStyle {
		return false, errs.New(
			errs.KindValidationFailed,
			"connector add requires exactly one of --path-style or --virtual-hosted-style",
		)
	}
	return pathChanged, nil
}

func connectorCredentialInput(
	stdin io.Reader,
	label string,
	secretRef string,
	valueFile string,
) (apiTypes.ConnectorCredentialInput, error) {
	if (secretRef == "") == (valueFile == "") {
		return apiTypes.ConnectorCredentialInput{}, errs.Newf(
			errs.KindValidationFailed,
			"Connector %s requires exactly one Secret reference or value file",
			label,
		)
	}
	if secretRef != "" {
		return apiTypes.ConnectorCredentialInput{SecretRef: secretRef}, nil
	}
	value, err := readValueFile(valueFile, stdin, apiTypes.MaximumSecretValueBytes, "Connector "+label)
	if err != nil {
		return apiTypes.ConnectorCredentialInput{}, err
	}
	return apiTypes.ConnectorCredentialInput{Value: value}, nil
}

func clearConnectorCredentialInputs(credentials map[string]apiTypes.ConnectorCredentialInput) {
	for name, credential := range credentials {
		credential.Value = ""
		credentials[name] = credential
	}
}

func resolveConnectorTarget(cmd *cobra.Command, argument string) (string, error) {
	app := fromContext(cmd)
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	environmentID, err := connectorEnvironmentID(cmd)
	if err != nil {
		return "", err
	}
	cursor := ""
	for {
		page, err := app.Client.ListConnectors(cmd.Context(), environmentID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, connector := range page.Items {
			if connector.Name == argument {
				return target(app, connector.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindConnectorNotFound, "Connector %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func renderConnector(cmd *cobra.Command, connector apiTypes.Connector) error {
	fields := connectorFields(connector)
	headers, values := fieldsOfVia(fields)
	return fromContext(cmd).Out.RenderOne(headers, values, connector)
}

func connectorFields(connector apiTypes.Connector) map[string]any {
	return map[string]any{
		"id": connector.ID, "environment_id": connector.EnvironmentID, "name": connector.Name,
		"kind": connector.Kind, "endpoint": connector.Endpoint, "bucket": connector.Bucket,
		"prefix": connector.Prefix, "region": connector.Region, "path_style": connector.PathStyle,
		"credentials": connector.Credentials,
	}
}
