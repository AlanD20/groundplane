package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// VolumeEvidenceStageAudit uses the production physical-key encoder over the
// existing MVCC fixture. Faults affect only this test store's next operation.
type VolumeEvidenceStageAudit struct {
	Store
	BeforeCommit        func()
	LoseResponse        bool
	ReportedRowOverhead int
	Comparisons         int
	Mutations           int
	WireBytes           int
	SizedRows           map[int]int
	WrittenKeys         []string
}

func NewVolumeEvidenceStageAudit(backend Store) *VolumeEvidenceStageAudit {
	return &VolumeEvidenceStageAudit{Store: backend, SizedRows: make(map[int]int)}
}

func (audit *VolumeEvidenceStageAudit) VolumeRemovalEvidenceTransactionSize(
	conditions []Condition, mutations []Mutation,
) (int, error) {
	size, err := (&store{root: "/groundplane"}).VolumeRemovalEvidenceTransactionSize(conditions, mutations)
	if err != nil {
		return 0, err
	}
	rows := max(0, len(conditions)-2)
	reported := size + rows*audit.ReportedRowOverhead
	audit.SizedRows[rows] = reported
	return reported, nil
}

func (audit *VolumeEvidenceStageAudit) Transact(
	ctx context.Context, conditions []Condition, mutations []Mutation,
) (TransactionResult, error) {
	size, err := (&store{root: "/groundplane"}).VolumeRemovalEvidenceTransactionSize(conditions, mutations)
	if err != nil {
		return TransactionResult{}, err
	}
	audit.Comparisons, audit.Mutations, audit.WireBytes = len(conditions), len(mutations), size
	audit.WrittenKeys = nil
	for _, mutation := range mutations {
		audit.WrittenKeys = append(audit.WrittenKeys, mutation.Key)
	}
	if audit.BeforeCommit != nil {
		before := audit.BeforeCommit
		audit.BeforeCommit = nil
		before()
	}
	result, err := audit.Store.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && audit.LoseResponse {
		audit.LoseResponse = false
		return TransactionResult{}, errs.New(errs.KindRequestFailed, "lost evidence staging response")
	}
	return result, err
}
