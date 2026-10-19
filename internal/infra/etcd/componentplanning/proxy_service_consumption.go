package componentplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Planner) ValidateBlueprintProxyServiceSource(ctx context.Context,
	environmentID, parentID, serviceID string, revision int64) error {
	return ValidateBlueprintProxyServiceSource(ctx, repository.store, environmentID, parentID, serviceID, revision)
}

// ValidateBlueprintProxyServiceSource constrains a later Service child to the
// sources pinned before its router ran. It grants no executable authority; the
// child's normal native capture and atomic publication must still compare the
// same source revisions. A skipped router keeps its earlier applied receipt and
// introduces no new preflight for this parent.
func ValidateBlueprintProxyServiceSource(ctx context.Context, store planningStore,
	environmentID, parentID, serviceID string, revision int64) error {
	_, err := readBlueprintProxyServiceSource(ctx, store, environmentID, parentID, serviceID, revision)
	return err
}

func ReadBlueprintProxyReplacementImage(ctx context.Context, store planningStore,
	environmentID, parentID, serviceID string, revision int64) (*ProxyReplacementImage, error) {
	source, err := readBlueprintProxyServiceSource(ctx, store, environmentID, parentID, serviceID, revision)
	if err != nil || source == nil {
		return nil, err
	}
	return cloneProxyReplacementImage(source.Replacement), nil
}

func ReadBlueprintProxyServiceRolloutReference(ctx context.Context, store planningStore,
	environmentID, parentID, serviceID string, revision int64,
) (*ProxyServiceRolloutReference, error) {
	source, err := readBlueprintProxyServiceSource(ctx, store, environmentID, parentID, serviceID, revision)
	if err != nil || source == nil || !source.ReplacementRequired {
		return nil, err
	}
	if source.Rollout == nil || source.Rollout.Validate() != nil {
		return nil, errs.New(errs.KindStateConflict, "applied router has no pre-switch native rollout for this Service")
	}
	owned := *source.Rollout
	return &owned, nil
}

func readBlueprintProxyServiceSource(ctx context.Context, store planningStore,
	environmentID, parentID, serviceID string, revision int64) (*ProxyServiceSource, error) {
	if ctx == nil || store == nil || revision <= 0 || ids.Validate(ids.KindEnvironment, environmentID) != nil ||
		ids.Validate(ids.KindTask, parentID) != nil || ids.Validate(ids.KindService, serviceID) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint router Service capture request is invalid")
	}
	owned, found, err := blueprints.ReadOwnedIdentitiesRevision(ctx, store, environmentID, parentID, revision)
	if err != nil {
		return nil, err
	}
	if !found || owned.ReadRevision != revision {
		return nil, errs.New(errs.KindStateConflict, "Blueprint router Service membership is unavailable")
	}
	for _, identity := range owned.Record.Components {
		key := blueprintunits.AppliedKey(
			environmentID,
			blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: identity.ID},
		)
		read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
			return nil, errs.New(errs.KindInternal, "Blueprint router receipt read is incomplete")
		}
		if read.Values[0] == nil {
			continue
		}
		receipt, err := blueprintunits.DecodeApplied(read.Values[0].Value)
		receiptRevision := read.Values[0].ModRevision
		keyvalue.ClearValues(read.Values)
		if err != nil {
			return nil, err
		}
		if receipt.EnvironmentID != environmentID || receipt.Target.ID != identity.ID ||
			receipt.Target.Kind != ids.KindComponent {
			return nil, errs.New(errs.KindStateConflict, "Blueprint router receipt ownership changed")
		}
		if receipt.ParentTaskID != parentID {
			continue
		}
		preflight, err := readAppliedProxyServicePreflight(ctx, store, receipt, receiptRevision, revision)
		if err != nil {
			return nil, err
		}
		if preflight == nil {
			continue
		}
		if receipt.State != blueprintunits.Applied {
			return nil, errs.New(
				errs.KindStateConflict,
				"Blueprint router has not applied its captured Service selection",
			)
		}
		index := slices.IndexFunc(
			preflight.Services,
			func(source ProxyServiceSource) bool { return source.ServiceID == serviceID },
		)
		if index < 0 {
			return nil, errs.New(
				errs.KindStateConflict,
				"Blueprint Service is missing from the applied router's captured membership",
			)
		}
		source := preflight.Services[index]
		if err := compareProxyServiceSource(ctx, store, source, revision); err != nil {
			return nil, err
		}
		if source.Rollout != nil && preflight.SourceHold != nil {
			condition, err := readProxySourceHoldCondition(ctx, store, *preflight.SourceHold, revision)
			if err != nil {
				return nil, err
			}
			rollout := *source.Rollout
			rollout.sourceHoldCondition = &condition
			source.Rollout = &rollout
		}
		return &source, nil
	}
	return nil, nil
}

// Controller metadata and Agent execution are distinct closed authorities.
// Both must be bound to their exact receipt; a missing capture is never inferred
// from the current Environment or another parent's execution.
func readAppliedProxyServicePreflight(ctx context.Context, store planningStore,
	receipt blueprintunits.AppliedRecord, receiptRevision, revision int64,
) (*ProxyServicePreflight, error) {
	if receipt.ControllerProxySelection {
		selection, found, err := ReadControllerProxySelection(ctx, store, receipt.EnvironmentID,
			receipt.ParentTaskID, receipt.Target.ID, revision)
		if err != nil {
			return nil, err
		}
		if !found || selection.Revision != receiptRevision ||
			selection.Record.OperationID != receipt.ControllerOperationID {
			return nil, errs.New(
				errs.KindStateConflict,
				"disabled router Service capture differs from its atomic receipt",
			)
		}
		return cloneProxyServicePreflight(&selection.Record.Preflight), nil
	}
	if receipt.Execution == nil {
		return nil, nil // Controller metadata has no proxy replacement.
	}
	runtime, found, err := ReadRuntime(ctx, store, receipt.Target.ID, revision)
	if err != nil {
		return nil, err
	}
	if !found || runtime.Record.EnvironmentID != receipt.EnvironmentID ||
		runtime.Record.RevisionID != receipt.ParentTaskID ||
		runtime.Record.Component.Desired.ID != receipt.Target.ID ||
		runtime.Record.Execution != *receipt.Execution {
		return nil, errs.New(
			errs.KindStateConflict,
			"Applied router dependencies do not match the acknowledged runtime",
		)
	}
	return cloneProxyServicePreflight(runtime.Record.ProxyPreflight), nil
}

func compareProxyServiceSource(
	ctx context.Context,
	store planningStore,
	source ProxyServiceSource,
	revision int64,
) error {
	keys := proxyServiceSourceKeys(source.ServiceID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Blueprint router Service capture comparison is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, expected := range source.versions() {
		value := read.Values[index]
		if keyvalue.RevisionOf(value) != expected.Revision {
			return errs.New(
				errs.KindStateConflict,
				"Service runtime changed after its Environment router was prepared; Apply the current Blueprint again",
			)
		}
		if value == nil {
			continue
		}
		digest := sha256.Sum256(value.Value)
		if hex.EncodeToString(digest[:]) != expected.SHA256 {
			return errs.New(
				errs.KindStateConflict,
				"Service runtime differs from its Environment router's captured source",
			)
		}
	}
	return nil
}
