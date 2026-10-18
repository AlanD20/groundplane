package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"

	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func decodeReleaseProjection(
	value *etcdstore.KeyValue,
	environmentID, serviceID string,
) (domain.ServiceProjection, error) {
	if value == nil {
		return domain.ServiceProjection{}, nil
	}
	projection, err := releases.DecodeReleaseRecord[domain.ServiceProjection](value.Value, "service-release-projection")
	if err != nil || projection.EnvironmentID != environmentID || projection.ServiceID != serviceID ||
		projection.ActiveOperationID != "" {
		return domain.ServiceProjection{}, releases.CorruptReleaseRecord()
	}
	return projection, nil
}

// releaseTerminalRecordMutations encodes the indivisible member checkpoint,
// history, retention and serving projection before considering its batch size.
func releaseTerminalRecordMutations(
	keys []string,
	checkpoint domain.Checkpoint,
	terminal domain.TerminalSummary,
	retention domain.RollbackMaterial,
	projection domain.ServiceProjection,
	writeProjection bool,
) ([]etcdstore.Mutation, error) {
	mutations := make([]etcdstore.Mutation, 0, 4)
	checkpointValue, err := releases.EncodeReleaseRecord("release-checkpoint", checkpoint)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[1], Value: checkpointValue})
	terminalValue, err := releases.EncodeReleaseRecord("release-terminal-summary", terminal)
	if err != nil {
		etcdstore.ZeroMutationBytes(mutations)
		return nil, err
	}
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[3], Value: terminalValue})
	retentionValue, err := releases.EncodeReleaseRecord("release-retention", retention)
	if err != nil {
		etcdstore.ZeroMutationBytes(mutations)
		return nil, err
	}
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[4], Value: retentionValue})
	if writeProjection {
		projectionValue, err := releases.EncodeReleaseRecord("service-release-projection", projection)
		if err != nil {
			etcdstore.ZeroMutationBytes(mutations)
			return nil, err
		}
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[2], Value: projectionValue},
		)
	}
	return mutations, nil
}
