package backuppolicy

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// BackupPolicySourceSelection is the stable-id form accepted by the human API
// after label resolution. It carries no persistence revisions or indexes.
type BackupPolicySourceSelection struct {
	Kind     core.BackupSourceKind
	TargetID string
}

// BackupPolicyReplacementInput is one complete desired singleton document.
type BackupPolicyReplacementInput struct {
	EnvironmentID string
	Enabled       bool
	Frequency     string
	Keep          int64
	Encryption    string
	ConnectorID   string
	Sources       []BackupPolicySourceSelection
}

func ValidateReplacementInput(
	ctx context.Context,
	input BackupPolicyReplacementInput,
) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, input.EnvironmentID); err != nil {
		return err
	}
	if len(input.Sources) > MaximumBackupPolicySources {
		return errs.New(
			errs.KindValidationFailed,
			"backup policy may select at most 12 sources",
		)
	}
	configured := input.Frequency != "" || input.Keep != 0 || input.Encryption != "" ||
		input.ConnectorID != "" || len(input.Sources) != 0
	if input.Enabled || configured {
		if input.Frequency == "" || input.Keep <= 0 || input.Keep > MaximumBackupPolicyKeep ||
			(input.Encryption != "age" && input.Encryption != "none") ||
			input.Enabled && (len(input.Sources) == 0 || input.ConnectorID == "") {
			return errs.New(errs.KindValidationFailed, "configured backup policy is incomplete")
		}
		if err := ValidateFrequency(input.Frequency); err != nil {
			return err
		}
		if err := recordcodec.ValidateID(ids.KindConnector, input.ConnectorID); err != nil {
			return err
		}
	}
	seen := make(map[struct {
		kind     core.BackupSourceKind
		targetID string
	}]struct{}, len(input.Sources))
	for _, source := range input.Sources {
		probe := BackupSourceRecord{
			ID:            ids.New(ids.KindBackupSource),
			EnvironmentID: input.EnvironmentID,
			Kind:          source.Kind,
			TargetID:      source.TargetID,
			CreatedAt:     time.Now().UTC(),
		}
		if err := ValidateBackupSourceRecord(probe); err != nil {
			return err
		}
		identity := struct {
			kind     core.BackupSourceKind
			targetID string
		}{kind: source.Kind, targetID: source.TargetID}
		if _, duplicate := seen[identity]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup policy source identities must be unique")
		}
		seen[identity] = struct{}{}
		if source.Kind == core.BackupSourceConfig && input.Encryption != "age" {
			return errs.New(errs.KindValidationFailed, "config backup source requires age encryption")
		}
	}
	return nil
}
