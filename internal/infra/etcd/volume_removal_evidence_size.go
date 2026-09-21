package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VolumeRemovalEvidenceTransactionSize measures the actual wire request while
// enforcing evidence-specific physical key and value bounds. It does not write
// records or authorize publication. The staging owner selects its row prefix
// using the returned size and the unchanged 900-KiB ceiling.
func (s *store) VolumeRemovalEvidenceTransactionSize(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) (int, error) {
	if len(conditions)+len(mutations) > removalrecord.EvidenceTransactionOperations {
		return 0, errs.New(errs.KindInternal, "volume removal evidence operation budget exceeded")
	}
	for _, condition := range conditions {
		key, err := s.physicalKey(condition.Key)
		if err != nil {
			return 0, err
		}
		if len(key) > removalrecord.EvidenceKeyBytes {
			return 0, errs.New(errs.KindInternal, "volume removal evidence comparison key is oversized")
		}
	}
	for _, mutation := range mutations {
		key, err := s.physicalKey(mutation.Key)
		if err != nil {
			return 0, err
		}
		if len(key) > removalrecord.EvidenceKeyBytes || len(mutation.Value) > removalrecord.EvidenceRecordBytes {
			return 0, errs.New(errs.KindInternal, "volume removal evidence mutation is oversized")
		}
	}
	return s.transactionSize(conditions, mutations)
}
