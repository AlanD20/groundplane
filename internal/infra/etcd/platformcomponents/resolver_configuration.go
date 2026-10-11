package platformcomponents

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResolverConfigurationAttempt suppresses automatic replay of a failed request.
// A new authored revision or runtime binding can request new work; an unchanged
// failure remains available for explicit Retry, not a background retry loop.
type ResolverConfigurationAttempt struct {
	Fingerprint       string `json:"fingerprint"`
	TaskID            string `json:"task_id"`
	PredecessorTaskID string `json:"predecessor_task_id"`
}

func ResolverConfigurationAttemptKey(componentID string) string {
	return "/v1/indexes/platform-resolver-configuration-attempt/" + componentID
}

func ResolverConfigurationFingerprint(
	listener string,
	captures []EnvironmentDNSCapture,
	zoneListeners []string,
) (string, error) {
	if _, err := ParseZoneListeners(zoneListeners); err != nil {
		return "", err
	}
	value, err := json.Marshal(struct {
		Listener string                  `json:"listener"`
		Records  []EnvironmentDNSCapture `json:"records"`
		Zones    []string                `json:"zones"`
	}{listener, captures, zoneListeners})
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

// An unchanged binding does not need a reload merely to replace the captured
// authored revision. Settlement still compares the current immutable authority.
func SameEnvironmentDNSBindings(left, right []EnvironmentDNSCapture) bool {
	return slices.EqualFunc(left, right, func(a, b EnvironmentDNSCapture) bool {
		return a.Binding == b.Binding
	})
}

func validResolverConfigurationAttempt(value ResolverConfigurationAttempt) bool {
	return recordcodec.ValidSHA256(value.Fingerprint) &&
		ids.Validate(ids.KindTask, value.TaskID) == nil &&
		ids.Validate(ids.KindTask, value.PredecessorTaskID) == nil && value.TaskID != value.PredecessorTaskID
}

func EncodeResolverConfigurationAttempt(value ResolverConfigurationAttempt) ([]byte, error) {
	if !validResolverConfigurationAttempt(value) {
		return nil, errs.New(errs.KindValidationFailed, "resolver configuration attempt is invalid")
	}
	return recordcodec.Encode("resolver_configuration_attempt", value)
}

func DecodeResolverConfigurationAttempt(data []byte) (ResolverConfigurationAttempt, error) {
	value, err := recordcodec.Decode[ResolverConfigurationAttempt](data, "resolver_configuration_attempt")
	if err != nil || !validResolverConfigurationAttempt(value) {
		return ResolverConfigurationAttempt{}, errs.New(errs.KindInternal, "resolver configuration attempt is corrupt")
	}
	return value, nil
}
