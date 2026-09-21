package environmentprojection

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentBlueprintBackupPolicy struct {
	Enabled     bool                                     `json:"enabled"`
	Frequency   string                                   `json:"frequency,omitempty"`
	Keep        int64                                    `json:"keep,omitempty"`
	Encryption  string                                   `json:"encryption,omitempty"`
	ConnectorID string                                   `json:"connector_id,omitempty"`
	Sources     []EnvironmentBlueprintBackupPolicySource `json:"sources,omitempty"`
}

type EnvironmentBlueprintBackupPolicySource struct {
	ID       string                `json:"id"`
	Kind     core.BackupSourceKind `json:"kind"`
	TargetID string                `json:"target_id"`
}

func CloneEnvironmentBlueprintBackupPolicy(source *EnvironmentBlueprintBackupPolicy) *EnvironmentBlueprintBackupPolicy {
	if source == nil {
		return nil
	}
	clone := *source
	clone.Sources = append([]EnvironmentBlueprintBackupPolicySource(nil), source.Sources...)
	return &clone
}

func ValidateEnvironmentBlueprintBackupPolicy(
	environmentID string,
	policy *EnvironmentBlueprintBackupPolicy,
) error {
	if policy == nil {
		return nil
	}
	selections := make([]backuppolicy.BackupPolicySourceSelection, len(policy.Sources))
	seenIDs := make(map[string]struct{}, len(policy.Sources))
	for index, source := range policy.Sources {
		if ids.Validate(ids.KindBackupSource, source.ID) != nil {
			return errs.New(errs.KindValidationFailed, "Blueprint Backup source identity is invalid")
		}
		if _, duplicate := seenIDs[source.ID]; duplicate {
			return errs.New(errs.KindValidationFailed, "Blueprint Backup source identity is duplicated")
		}
		seenIDs[source.ID] = struct{}{}
		selections[index] = backuppolicy.BackupPolicySourceSelection{Kind: source.Kind, TargetID: source.TargetID}
	}
	return backuppolicy.ValidateReplacementInput(context.Background(), backuppolicy.BackupPolicyReplacementInput{
		EnvironmentID: environmentID, Enabled: policy.Enabled, Frequency: policy.Frequency,
		Keep: policy.Keep, Encryption: policy.Encryption, ConnectorID: policy.ConnectorID,
		Sources: selections,
	})
}
