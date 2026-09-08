package etcd

import (
	"crypto/sha256"
	"fmt"

	domain "github.com/AlanD20/groundplane/internal/core/release"
)

func releaseTestWorkloadSeal(reference string) domain.WorkloadSeal {
	return domain.WorkloadSeal{RequestedReference: reference,
		LocalImageID: fmt.Sprintf("sha256:%x", sha256.Sum256([]byte(reference))), ReplicaCount: 1}
}

func releaseTestPriorWorkload(reference string) *domain.WorkloadSeal {
	seal := releaseTestWorkloadSeal(reference)
	return &seal
}
