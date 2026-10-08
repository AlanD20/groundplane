package desiredauthoring

import (
	"context"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupqueries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func SetBackup(ctx context.Context, store Store, input *core.BlueprintDesiredInput,
	projection *environmentprojection.EnvironmentComposeProjection, policy backupqueries.BackupPolicyProjection,
) error {
	authored := &core.BackupSpec{
		Enabled:    policy.Enabled,
		Frequency:  policy.Frequency,
		Keep:       policy.Keep,
		Encryption: policy.Encryption,
	}
	resolved := &environmentprojection.EnvironmentBlueprintBackupPolicy{
		Enabled:     policy.Enabled,
		Frequency:   policy.Frequency,
		Keep:        policy.Keep,
		Encryption:  policy.Encryption,
		ConnectorID: policy.ConnectorID,
	}
	keys := make([]string, 0, len(policy.Sources)+1)
	if policy.ConnectorID != "" {
		keys = append(keys, connectors.RecordKey(policy.ConnectorID))
	}
	for _, source := range policy.Sources {
		if source.Kind == core.BackupSourceAttach {
			keys = append(keys, attachments.AttachKey(source.TargetID))
		}
	}
	names := make(map[string]string, len(keys))
	if len(keys) != 0 {
		state, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
		if err != nil {
			return err
		}
		if state == nil || len(state.Values) != len(keys) {
			return errs.New(errs.KindInternal, "Backup authoring identities are incomplete")
		}
		for index, value := range state.Values {
			if value == nil {
				if !policy.Enabled && policy.ConnectorID != "" && index == 0 {
					continue // A removed destination is not a dependency of a disabled policy.
				}
				return errs.New(errs.KindStateConflict, "Backup authoring target is unavailable")
			}
			if policy.ConnectorID != "" && index == 0 {
				record, err := connectors.DecodeRecord(value.Value)
				if err != nil {
					return err
				}
				authored.Connector = record.Connector.Name
			} else {
				record, err := attachments.DecodeAttachRecord(value.Value)
				if err != nil {
					return err
				}
				names[record.ID] = record.Name
			}
		}
	}
	for _, source := range policy.Sources {
		entry := core.BackupSourceSpec{Kind: source.Kind}
		switch source.Kind {
		case core.BackupSourceAttach:
			entry.Ref = names[source.TargetID]
		case core.BackupSourceVolume:
			for _, volume := range projection.Volumes {
				if volume.ID == source.TargetID {
					entry.Ref = volume.Slug
					break
				}
			}
		case core.BackupSourceConfig:
		default:
			return errs.New(errs.KindValidationFailed, "Backup source kind is invalid")
		}
		if source.Kind != core.BackupSourceConfig && entry.Ref == "" {
			return errs.New(errs.KindStateConflict, "Backup authored target is absent")
		}
		authored.Sources = append(authored.Sources, entry)
		resolved.Sources = append(
			resolved.Sources,
			environmentprojection.EnvironmentBlueprintBackupPolicySource{
				ID:       source.ID,
				Kind:     source.Kind,
				TargetID: source.TargetID,
			},
		)
	}
	input.Backup, projection.Backup = authored, resolved
	return nil
}
