package environmentchanges

import (
	"bytes"

	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// EnvironmentZoneRemovalAuthorities binds the desired revision being edited
// to the independently mutable projection last acknowledged by the runtime.
// Applied has revision zero and an empty record before the first runtime apply.
type EnvironmentZoneRemovalAuthorities struct {
	Desired etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
	Applied etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection]
}

// ZoneRemovalAppliedEvidenceMatches fences absence as well as a captured runtime.
func ZoneRemovalAppliedEvidenceMatches(intent ZoneRemovalIntent, value *etcdstore.KeyValue) bool {
	if etcdstore.RevisionOf(value) != intent.AppliedProjectionRevision {
		return false
	}
	if value == nil {
		return validZoneRemovalAppliedProjection(intent)
	}
	applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(value.Value)
	if err != nil || !SameServiceRemovalProjection(applied, intent.AppliedProjection) {
		return false
	}
	want, err := projectionrecord.EncodeEnvironmentComposeProjectionStorage(intent.AppliedProjection)
	if err != nil {
		return false
	}
	defer clear(want)
	return bytes.Equal(want, value.Value)
}

func validZoneRemovalAppliedProjection(intent ZoneRemovalIntent) bool {
	if intent.AppliedProjectionRevision == 0 {
		return SameServiceRemovalProjection(intent.AppliedProjection, projectionrecord.EnvironmentComposeProjection{})
	}
	return intent.AppliedProjectionRevision > 0 && intent.AppliedProjection.EnvironmentID == intent.EnvironmentID &&
		projectionrecord.ValidateEnvironmentComposeProjection(intent.AppliedProjection) == nil
}
