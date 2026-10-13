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
	TaskID              string `json:"task_id"`
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
		ids.Validate(ids.KindTask, reference.TaskID) != nil ||
		reference.Revision <= 0 ||
		reference.NativeRevision <= 0 ||
		err != nil ||
		len(digest) != sha256.Size ||
		hex.EncodeToString(digest) != reference.SHA256 {
		return errs.New(errs.KindValidationFailed, "pre-switch Service rollout reference is invalid")
	}
	return nil
}

func (preparation ComponentTaskPreparation) WithProxyServiceRollouts(
	rollouts []ProxyServiceRolloutReference,
) (ComponentTaskPreparation, error) {
	if preparation.proxyServicePreflight == nil || preparation.proxyServicePreflight.CandidateInput == nil {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"pre-switch rollouts require a complete candidate Environment input",
		)
	}
	result := cloneComponentTaskPreparation(preparation)
	preflight := result.proxyServicePreflight
	expected := 0
	for _, source := range preflight.Services {
		if source.ReplacementRequired {
			expected++
		}
	}
	if len(rollouts) != expected {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"pre-switch Service rollout membership is incomplete",
		)
	}
	for ordinal, rollout := range rollouts {
		if rollout.Validate() != nil || ordinal > 0 && rollouts[ordinal-1].ServiceID >= rollout.ServiceID {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindValidationFailed,
				"pre-switch Service rollout membership is invalid",
			)
		}
		index := slices.IndexFunc(
			preflight.Services,
			func(source ProxyServiceSource) bool { return source.ServiceID == rollout.ServiceID },
		)
		if index < 0 || !preflight.Services[index].ReplacementRequired ||
			rollout.TaskID == preflight.CandidateInput.SourceID ||
			rollout.TaskID == preflight.ParentTaskID {
			return ComponentTaskPreparation{}, errs.New(
				errs.KindValidationFailed,
				"pre-switch Service rollout belongs to another selection",
			)
		}
		owned := rollout
		preflight.Services[index].Rollout = &owned
		result.proxyServiceConditions = append(
			result.proxyServiceConditions,
			keyvalue.Condition{
				Key:         ProxyServiceRolloutKey(preflight.EnvironmentID, preflight.ParentTaskID, rollout.TaskID),
				ModRevision: rollout.Revision,
			},
			keyvalue.Condition{
				Key: ProxyServiceNativeRolloutKey(
					preflight.EnvironmentID,
					preflight.ParentTaskID,
					rollout.TaskID,
				),
				ModRevision: rollout.NativeRevision,
			},
		)
	}
	result.proxyServiceConditionCount += 2 * len(rollouts)
	return result, nil
}
