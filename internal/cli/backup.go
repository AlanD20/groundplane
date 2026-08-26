package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

type backupPolicySourceReference struct {
	Kind   apiTypes.BackupSourceKind
	Target string
}

// backup: policy show | policy set [...] | run | points | restore
// <source> | rotate-key | export-key. Per-environment, toggleable off,
// fully selectable sources. See mvp.md, "Backup", and blueprint.md,
// "x-gp-backup".
func newBackupCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "Backup policy, runs, recovery points, and restores",
	}

	policy := &cobra.Command{Use: "policy", Short: "The environment's one backup policy"}
	policy.AddCommand(&cobra.Command{
		Use:   "show",
		Short: "Show the backup policy",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			environmentID, err := backupPolicyEnvironmentID(cmd)
			if err != nil {
				return err
			}
			backupPolicy, err := fromContext(cmd).Client.ShowBackupPolicy(cmd.Context(), environmentID)
			if err != nil {
				return err
			}
			return renderBackupPolicy(cmd, backupPolicy)
		},
	})

	var connector string
	var frequency string
	var keep string
	var encryption string
	var sources []string
	var off bool
	set := &cobra.Command{
		Use:   "set",
		Short: "Set the backup policy",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			keepValue, err := parseBackupPolicyKeep(cmd, keep)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			if off {
				if err := validateBackupPolicyOffFlags(cmd); err != nil {
					return err
				}
				environmentID, err := backupPolicyEnvironmentID(cmd)
				if err != nil {
					return err
				}
				current, err := app.Client.ShowBackupPolicy(cmd.Context(), environmentID)
				if err != nil {
					return err
				}
				current.Enabled = false
				replacement := backupPolicyReplacement(current)
				updated, err := app.Client.SetBackupPolicy(cmd.Context(), environmentID, replacement)
				if err != nil {
					return err
				}
				return renderBackupPolicy(cmd, updated)
			}
			sourceReferences, err := parseBackupPolicySources(sources)
			if err != nil {
				return err
			}
			environmentID, err := backupPolicyEnvironmentID(cmd)
			if err != nil {
				return err
			}
			connectorID, err := resolveBackupPolicyConnector(
				cmd.Context(), app, environmentID, connector,
			)
			if err != nil {
				return err
			}
			resolvedSources, err := resolveBackupPolicySources(
				cmd.Context(), app, environmentID, sourceReferences,
			)
			if err != nil {
				return err
			}
			backupPolicy, err := app.Client.SetBackupPolicy(
				cmd.Context(),
				environmentID,
				apiTypes.BackupPolicyReplacementRequest{
					Enabled: true, Frequency: frequency, Keep: keepValue,
					Encryption:  apiTypes.BackupEncryption(encryption),
					ConnectorID: connectorID, Sources: resolvedSources,
				},
			)
			if err != nil {
				return err
			}
			return renderBackupPolicy(cmd, backupPolicy)
		},
	}
	set.Flags().StringVar(&connector, "connector", "", "Connector label, or stable id with --id")
	set.Flags().
		StringVar(&frequency, "frequency", "", "UTC daily or weekly expression, e.g. '*-*-* 03:15:00'")
	set.Flags().StringVar(&keep, "keep", "", "how many previous recovery points to retain")
	set.Flags().StringVar(&encryption, "encryption", "", "age | none")
	set.Flags().
		StringArrayVar(&sources, "source", nil, "repeatable: attach:<name|id> | volume:<name|id> | config")
	set.Flags().
		BoolVar(&off, "off", false, "disable backups for this environment (policy and sources are kept, just don't run)")
	policy.AddCommand(set)
	cmd.AddCommand(policy)

	run := &cobra.Command{
		Use:   "run",
		Short: "Run all configured backup sources now",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			app := fromContext(cmd)
			path := "/api/v1/environments/" + target(app, app.Scope.Environment) + "/backup-run"
			return runAction(cmd, path, nil)
		},
	}
	cmd.AddCommand(run)

	points := &cobra.Command{
		Use:   "points",
		Short: "List recovery points (visible only after dump/encrypt/upload/verify all succeed)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runList(
				cmd,
				"/api/v1/environments/"+target(app, app.Scope.Environment)+"/recovery-points",
				nil,
			)
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
	restore.Flags().StringVar(
		&ageIdentityPath,
		"age-identity",
		"",
		"exported age identity file (needed if the recovery point's key era was rotated away)",
	)
	cmd.AddCommand(restore)

	cmd.AddCommand(&cobra.Command{
		Use:   "rotate-key",
		Short: "Rotate the environment's backup encryption age key (bumps key_era; affects new recovery points only)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runAction(
				cmd,
				"/api/v1/environments/"+target(app, app.Scope.Environment)+"/rotate-key",
				nil,
			)
		},
	})

	cmd.AddCommand(&cobra.Command{
		Use:   "export-key",
		Short: "Export the current age identity for off-host disaster recovery",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			app := fromContext(cmd)
			return runExportKey(
				cmd,
				"/api/v1/environments/"+target(app, app.Scope.Environment)+"/export-key",
			)
		},
	})

	return cmd
}

func parseBackupPolicyKeep(cmd *cobra.Command, raw string) (int64, error) {
	if !cmd.Flags().Changed("keep") {
		return 0, nil
	}
	if raw == "" || raw[0] < '1' || raw[0] > '9' {
		return 0, invalidBackupPolicyKeep()
	}
	for index := 1; index < len(raw); index++ {
		if raw[index] < '0' || raw[index] > '9' {
			return 0, invalidBackupPolicyKeep()
		}
	}
	keep, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || !apiTypes.ValidBackupPolicyKeep(keep) {
		return 0, invalidBackupPolicyKeep()
	}
	return keep, nil
}

func invalidBackupPolicyKeep() error {
	return errs.New(
		errs.KindValidationFailed,
		"backup policy keep must be a canonical base-10 integer between 1 and 9007199254740991",
	)
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

func parseBackupPolicySources(arguments []string) ([]backupPolicySourceReference, error) {
	if len(arguments) > apiTypes.MaximumBackupPolicySources {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"backup policy supports at most %d sources",
			apiTypes.MaximumBackupPolicySources,
		)
	}
	sources := make([]backupPolicySourceReference, 0, len(arguments))
	seen := make(map[backupPolicySourceReference]struct{}, len(arguments))
	for _, argument := range arguments {
		var source backupPolicySourceReference
		switch {
		case argument == "config":
			source = backupPolicySourceReference{Kind: apiTypes.BackupSourceConfig}
		case strings.HasPrefix(argument, "attach:"):
			source = backupPolicySourceReference{
				Kind: apiTypes.BackupSourceAttach, Target: strings.TrimPrefix(argument, "attach:"),
			}
			if source.Target == "" {
				return nil, errs.New(errs.KindValidationFailed, "backup policy attach source requires a label")
			}
		case strings.HasPrefix(argument, "volume:"):
			source = backupPolicySourceReference{
				Kind: apiTypes.BackupSourceVolume, Target: strings.TrimPrefix(argument, "volume:"),
			}
			if source.Target == "" {
				return nil, errs.New(errs.KindValidationFailed, "backup policy volume source requires a label")
			}
		default:
			return nil, errs.Newf(errs.KindValidationFailed, "backup policy source %q is invalid", argument)
		}
		if _, duplicate := seen[source]; duplicate {
			return nil, errs.Newf(errs.KindValidationFailed, "backup policy source %q is duplicated", argument)
		}
		seen[source] = struct{}{}
		sources = append(sources, source)
	}
	return sources, nil
}

func backupPolicyEnvironmentID(cmd *cobra.Command) (string, error) {
	argument := fromContext(cmd).Scope.Environment
	if argument == "" {
		return "", errs.New(errs.KindValidationFailed, "backup policy command requires --environment")
	}
	return resolveEnvironmentTarget(cmd, argument)
}

func validateBackupPolicyOffFlags(cmd *cobra.Command) error {
	for _, name := range []string{"connector", "frequency", "keep", "encryption", "source"} {
		if cmd.Flags().Changed(name) {
			return errs.New(
				errs.KindValidationFailed,
				"backup policy --off cannot be combined with configuration flags",
			)
		}
	}
	return nil
}

func backupPolicyReplacement(policy apiTypes.BackupPolicy) apiTypes.BackupPolicyReplacementRequest {
	sources := make([]apiTypes.BackupSourceInput, len(policy.Sources))
	for index, source := range policy.Sources {
		sources[index] = apiTypes.BackupSourceInput{Kind: source.Kind, TargetID: source.TargetID}
	}
	return apiTypes.BackupPolicyReplacementRequest{
		Enabled: policy.Enabled, Frequency: policy.Frequency, Keep: policy.Keep,
		Encryption: policy.Encryption, ConnectorID: policy.ConnectorID, Sources: sources,
	}
}

func resolveBackupPolicyConnector(
	ctx context.Context,
	app *App,
	environmentID string,
	argument string,
) (string, error) {
	if argument == "" {
		return "", nil
	}
	if app.Scope.AsID {
		return target(app, argument), nil
	}
	cursor := ""
	for {
		page, err := app.Client.ListConnectors(ctx, environmentID, 200, cursor)
		if err != nil {
			return "", err
		}
		for _, connector := range page.Items {
			if connector.Name == argument {
				return target(app, connector.ID), nil
			}
		}
		if page.NextCursor == "" {
			return "", errs.Newf(errs.KindConnectorNotFound, "connector %q was not found", argument)
		}
		cursor = page.NextCursor
	}
}

func resolveBackupPolicySources(
	ctx context.Context,
	app *App,
	environmentID string,
	references []backupPolicySourceReference,
) ([]apiTypes.BackupSourceInput, error) {
	attachIDs := map[string]string{}
	volumeIDs := map[string]string{}
	if !app.Scope.AsID {
		attachNames := backupPolicySourceTargets(references, apiTypes.BackupSourceAttach)
		volumeNames := backupPolicySourceTargets(references, apiTypes.BackupSourceVolume)
		var err error
		attachIDs, err = resolveBackupPolicyAttachNames(ctx, app, environmentID, attachNames)
		if err != nil {
			return nil, err
		}
		volumeIDs, err = resolveBackupPolicyVolumeNames(ctx, app, environmentID, volumeNames)
		if err != nil {
			return nil, err
		}
	}
	resolved := make([]apiTypes.BackupSourceInput, 0, len(references))
	seen := make(map[apiTypes.BackupSourceInput]struct{}, len(references))
	for _, reference := range references {
		source := apiTypes.BackupSourceInput{Kind: reference.Kind}
		switch reference.Kind {
		case apiTypes.BackupSourceAttach:
			source.TargetID = attachIDs[reference.Target]
			if app.Scope.AsID {
				source.TargetID = target(app, reference.Target)
			}
		case apiTypes.BackupSourceVolume:
			source.TargetID = volumeIDs[reference.Target]
			if app.Scope.AsID {
				source.TargetID = target(app, reference.Target)
			}
		case apiTypes.BackupSourceConfig:
			source.TargetID = environmentID
		}
		if _, duplicate := seen[source]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "backup policy sources resolve to a duplicate target")
		}
		seen[source] = struct{}{}
		resolved = append(resolved, source)
	}
	return resolved, nil
}

func backupPolicySourceTargets(
	references []backupPolicySourceReference,
	kind apiTypes.BackupSourceKind,
) map[string]struct{} {
	targets := map[string]struct{}{}
	for _, reference := range references {
		if reference.Kind == kind {
			targets[reference.Target] = struct{}{}
		}
	}
	return targets
}

func resolveBackupPolicyAttachNames(
	ctx context.Context,
	app *App,
	environmentID string,
	names map[string]struct{},
) (map[string]string, error) {
	resolved := make(map[string]string, len(names))
	if len(names) == 0 {
		return resolved, nil
	}
	cursor := ""
	for {
		page, err := app.Client.ListAttaches(ctx, environmentID, 200, cursor)
		if err != nil {
			return nil, err
		}
		for _, attach := range page.Items {
			if _, wanted := names[attach.Name]; wanted {
				resolved[attach.Name] = target(app, attach.ID)
			}
		}
		if len(resolved) == len(names) {
			return resolved, nil
		}
		if page.NextCursor == "" {
			for name := range names {
				if resolved[name] == "" {
					return nil, errs.Newf(errs.KindAttachNotFound, "attach name %q was not found", name)
				}
			}
		}
		cursor = page.NextCursor
	}
}

func resolveBackupPolicyVolumeNames(
	ctx context.Context,
	app *App,
	environmentID string,
	names map[string]struct{},
) (map[string]string, error) {
	resolved := make(map[string]string, len(names))
	if len(names) == 0 {
		return resolved, nil
	}
	cursor := ""
	for {
		page, err := app.Client.ListVolumes(ctx, environmentID, 200, cursor)
		if err != nil {
			return nil, err
		}
		for _, volume := range page.Items {
			if _, wanted := names[volume.Slug]; wanted {
				resolved[volume.Slug] = target(app, volume.ID)
			}
		}
		if len(resolved) == len(names) {
			return resolved, nil
		}
		if page.NextCursor == "" {
			for name := range names {
				if resolved[name] == "" {
					return nil, errs.Newf(errs.KindVolumeNotFound, "volume name %q was not found", name)
				}
			}
		}
		cursor = page.NextCursor
	}
}

func renderBackupPolicy(cmd *cobra.Command, policy apiTypes.BackupPolicy) error {
	fields, values := fieldsOfVia(map[string]any{
		"enabled": policy.Enabled, "frequency": policy.Frequency, "keep": policy.Keep,
		"encryption": policy.Encryption, "connector_id": policy.ConnectorID,
		"sources": policy.Sources, "age_recipient": policy.AgeRecipient,
		"key_era": policy.KeyEra, "key_created_at": policy.KeyCreatedAt,
		"key_rotated_at": policy.KeyRotatedAt,
	})
	return fromContext(cmd).Out.RenderOne(fields, values, policy)
}
