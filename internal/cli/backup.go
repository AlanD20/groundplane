package cli

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	clicommon "github.com/AlanD20/groundplane/internal/cli/common"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/spf13/cobra"
)

// backup: policy show | policy set [...] | run | points | remove-point
// <point> | restore <source> | rotate-key | export-key. Per-environment,
// toggleable off, fully selectable sources. See mvp.md, "Backup", and
// blueprint.md, "x-gp-backup".
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
		StringVar(&frequency, "frequency", "", "five-field UTC cron expression, e.g. '15 3 * * *'")
	set.Flags().StringVar(&keep, "keep", "", "how many previous recovery points to retain")
	set.Flags().StringVar(&encryption, "encryption", "", "age | none")
	set.Flags().
		StringArrayVar(&sources, "source", nil, "repeatable: attach:<name|id> | volume:<name|id> | config")
	set.Flags().
		BoolVar(&off, "off", false, "disable backups for this environment (policy and sources are kept, just don't run)")
	policy.AddCommand(set)
	cmd.AddCommand(policy)

	var runAfter []string
	run := &cobra.Command{
		Use:   "run",
		Short: "Run all configured backup sources now",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			after, err := validatedTaskAfter(runAfter)
			if err != nil {
				return err
			}
			environmentID, err := backupPolicyEnvironmentID(cmd)
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.RunBackup(cmd.Context(), environmentID, after)
			if err != nil {
				return err
			}
			return renderTaskAccepted(cmd, accepted)
		},
	}
	addTaskAfterFlag(run, &runAfter)
	cmd.AddCommand(run)

	var pointCursor string
	points := &cobra.Command{
		Use:   "points",
		Short: "List recovery points (visible only after dump/encrypt/upload/verify all succeed)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			environmentID, err := backupPolicyEnvironmentID(cmd)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			page, err := app.Client.ListRecoveryPoints(cmd.Context(), environmentID, pointCursor)
			if err != nil {
				return err
			}
			headers, rows := clicommon.RecoveryPointTable(page.Items)
			return app.Out.Render(headers, rows, page)
		},
	}
	points.Flags().StringVar(&pointCursor, "cursor", "", "opaque continuation cursor from a previous page")
	cmd.AddCommand(points)

	var removePointKey string
	var removePointAfter []string
	removePoint := &cobra.Command{
		Use:   "remove-point <point>",
		Short: "Delete one recovery point and its stored object",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			after, err := validatedTaskAfter(removePointAfter)
			if err != nil {
				return err
			}
			environmentID, err := backupPolicyEnvironmentID(cmd)
			if err != nil {
				return err
			}
			accepted, err := fromContext(cmd).Client.RemoveRecoveryPoint(
				cmd.Context(), environmentID, args[0], removePointKey, after,
			)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	removePoint.Flags().StringVar(
		&removePointKey,
		"idempotency-key",
		"",
		"reuse for an uncertain request with exactly the same inputs",
	)
	addTaskAfterFlag(removePoint, &removePointAfter)
	cmd.AddCommand(removePoint)

	var point, ageIdentityPath, restoreKey string
	var versionReview string
	var acknowledgeVersionDifference bool
	var restoreAfter []string
	restore := &cobra.Command{
		Use:   "restore <source>",
		Short: "Restore a source from a recovery point (defaults to the latest)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			after, err := validatedTaskAfter(restoreAfter)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			environmentID, err := backupPolicyEnvironmentID(cmd)
			if err != nil {
				return err
			}
			ageIdentity := ""
			if ageIdentityPath != "" {
				var err error
				ageIdentity, err = readAgeIdentity(ageIdentityPath)
				if err != nil {
					return err
				}
			}
			accepted, err := app.Client.RestoreBackup(
				cmd.Context(),
				environmentID,
				restoreKey,
				apiTypes.RestoreRequest{
					After:                        after,
					SourceID:                     args[0],
					RecoveryPointID:              point,
					AgeIdentity:                  ageIdentity,
					VersionReviewSHA256:          versionReview,
					AcknowledgeVersionDifference: acknowledgeVersionDifference,
				},
			)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	restore.Flags().StringVar(&point, "point", "", "recovery point id (default: latest)")
	restore.Flags().
		StringVar(&versionReview, "version-review", "", "exact digest from backup preview-restore (required for database Restore)")
	restore.Flags().
		BoolVar(&acknowledgeVersionDifference, "acknowledge-version-difference", false, "accept unverified compatibility for the reviewed source/target versions")
	restore.Flags().
		StringVar(&restoreKey, "idempotency-key", "", "reuse for an uncertain request with exactly the same inputs")
	restore.Flags().StringVar(
		&ageIdentityPath,
		"age-identity",
		"",
		"exported age identity file (needed if the recovery point's key era was rotated away)",
	)
	addTaskAfterFlag(restore, &restoreAfter)
	cmd.AddCommand(restore)
	cmd.AddCommand(newBackupRestorePreviewCmd())

	var rotateAfter []string
	rotate := &cobra.Command{
		Use:   "rotate-key",
		Short: "Rotate the environment's backup encryption age key (bumps key_era; affects new recovery points only)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			after, err := validatedTaskAfter(rotateAfter)
			if err != nil {
				return err
			}
			app := fromContext(cmd)
			accepted, err := app.Client.RotateBackupKey(
				cmd.Context(),
				target(app, app.Scope.Environment), after,
			)
			if err != nil {
				return err
			}
			return renderDispatchedTask(cmd, accepted)
		},
	}
	addTaskAfterFlag(rotate, &rotateAfter)
	cmd.AddCommand(rotate)

	var exportPath string
	export := &cobra.Command{
		Use:   "export-key",
		Short: "Export the current age identity for off-host disaster recovery",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			if cmd.Flags().Changed("output") {
				return errs.New(
					errs.KindValidationFailed,
					"backup export-key does not support global --output; use --file",
				)
			}
			app := fromContext(cmd)
			return runExportKey(
				cmd,
				"/api/v1/environments/"+target(app, app.Scope.Environment)+"/export-key",
				exportPath,
			)
		},
	}
	export.Flags().StringVar(&exportPath, "file", "-", "write the private identity to PATH, or - for stdout")
	cmd.AddCommand(export)

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

func renderBackupPolicy(cmd *cobra.Command, policy apiTypes.BackupPolicy) error {
	fields, values := fieldsOfVia(map[string]any{
		"enabled": policy.Enabled, "frequency": policy.Frequency, "keep": policy.Keep,
		"encryption": policy.Encryption, "connector_id": policy.ConnectorID,
		"sources": policy.Sources, "age_recipient": policy.AgeRecipient,
		"key_era": policy.KeyEra, "key_created_at": policy.KeyCreatedAt,
		"key_rotated_at": policy.KeyRotatedAt, "next_run_at": policy.NextRunAt,
	})
	return fromContext(cmd).Out.RenderOne(fields, values, policy)
}
