package componentplanning

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ProxyServiceRolloutReference struct {
	ServiceID           string `json:"service_id"`
	WorkItemID          string `json:"work_item_id"`
	SHA256              string `json:"sha256"`
	Revision            int64  `json:"revision"`
	NativeRevision      int64  `json:"native_revision"`
	sourceHoldCondition *keyvalue.Condition
}

// SourceHoldCondition is a read-generated fence, not operator-provided capture
// metadata. Final child publication must preserve it while taking ownership.
func (reference ProxyServiceRolloutReference) SourceHoldCondition() (keyvalue.Condition, bool) {
	if reference.sourceHoldCondition == nil {
		return keyvalue.Condition{}, false
	}
	return *reference.sourceHoldCondition, true
}

func ProxyServiceRolloutKey(environmentID, parentID, taskID string) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(
		environmentID,
		parentID,
	) + "proxy-service-rollouts/" + taskID
}

func ProxyServiceNativeRolloutKey(environmentID, parentID, taskID string) string {
	return blueprints.EnvironmentBlueprintRevisionPrefixFinal(environmentID, parentID) + "service-rollouts/" + taskID
}

func (reference ProxyServiceRolloutReference) Validate() error {
	digest, err := hex.DecodeString(reference.SHA256)
	if ids.Validate(ids.KindService, reference.ServiceID) != nil ||
		ids.Validate(ids.KindWorkItem, reference.WorkItemID) != nil ||
		reference.Revision <= 0 ||
		reference.NativeRevision <= 0 ||
		err != nil ||
		len(digest) != sha256.Size ||
		hex.EncodeToString(digest) != reference.SHA256 {
		return errs.New(errs.KindValidationFailed, "pre-switch Service rollout reference is invalid")
	}
	return nil
}

func BindProxyServiceRollouts(captured ProxyServicePreflight,
	rollouts []ProxyServiceRolloutReference,
) (*ProxyServicePreflight, []keyvalue.Condition, error) {
	if captured.validate() != nil || captured.CandidateInput == nil {
		return nil, nil, errs.New(
			errs.KindValidationFailed,
			"pre-switch rollouts require a complete candidate Environment input",
		)
	}
	preflight := cloneProxyServicePreflight(&captured)
	var conditions []keyvalue.Condition
	expected := 0
	for _, source := range preflight.Services {
		if source.ReplacementRequired {
			expected++
		}
	}
	if len(rollouts) != expected {
		return nil, nil, errs.New(
			errs.KindValidationFailed,
			"pre-switch Service rollout membership is incomplete",
		)
	}
	for ordinal, rollout := range rollouts {
		if rollout.Validate() != nil || ordinal > 0 && rollouts[ordinal-1].ServiceID >= rollout.ServiceID {
			return nil, nil, errs.New(
				errs.KindValidationFailed,
				"pre-switch Service rollout membership is invalid",
			)
		}
		index := slices.IndexFunc(
			preflight.Services,
			func(source ProxyServiceSource) bool { return source.ServiceID == rollout.ServiceID },
		)
		if index < 0 || !preflight.Services[index].ReplacementRequired ||
			rollout.WorkItemID == preflight.CandidateInput.SourceID ||
			rollout.WorkItemID == preflight.ParentTaskID {
			return nil, nil, errs.New(
				errs.KindValidationFailed,
				"pre-switch Service rollout belongs to another selection",
			)
		}
		if existing := preflight.Services[index].Rollout; existing != nil &&
			(existing.ServiceID != rollout.ServiceID || existing.WorkItemID != rollout.WorkItemID ||
				existing.SHA256 != rollout.SHA256 || existing.Revision != rollout.Revision || existing.NativeRevision != rollout.NativeRevision) {
			return nil, nil, errs.New(errs.KindStateConflict, "Service work already captured a different rollout")
		}
		owned := rollout
		preflight.Services[index].Rollout = &owned
		conditions = append(
			conditions,
			keyvalue.Condition{
				Key:         ProxyServiceRolloutKey(preflight.EnvironmentID, preflight.ParentTaskID, rollout.WorkItemID),
				ModRevision: rollout.Revision,
			},
			keyvalue.Condition{
				Key: ProxyServiceNativeRolloutKey(
					preflight.EnvironmentID,
					preflight.ParentTaskID,
					rollout.WorkItemID,
				),
				ModRevision: rollout.NativeRevision,
			},
		)
	}
	return preflight, conditions, nil
}
