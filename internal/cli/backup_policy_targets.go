package cli

import (
	"context"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
	"strings"
)

type backupPolicySourceReference struct {
	Kind   apiTypes.BackupSourceKind
	Target string
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
		page, err := app.Client.ListVolumes(ctx, environmentID, 100, cursor)
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
