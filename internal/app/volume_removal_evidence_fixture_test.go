package app

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// volumeEvidenceStageAudit uses the public production request measurement over the
// existing MVCC fixture. Faults affect only this test store's next operation.
type volumeEvidenceStageAudit struct {
	testkeyvalue.Store

	BeforeCommit        func()
	LoseResponse        bool
	ReportedRowOverhead int
	Comparisons         int
	Mutations           int
	WireBytes           int
	SizedRows           map[int]int
	WrittenKeys         []string
}

func newVolumeEvidenceStageAudit(backend testkeyvalue.Store) *volumeEvidenceStageAudit {
	return &volumeEvidenceStageAudit{Store: backend, SizedRows: make(map[int]int)}
}

func (audit *volumeEvidenceStageAudit) VolumeRemovalEvidenceTransactionSize(
	conditions []testkeyvalue.Condition, mutations []testkeyvalue.Mutation,
) (int, error) {
	size, err := measureVolumeRemovalEvidenceTransaction(conditions, mutations)
	if err != nil {
		return 0, err
	}
	rows := max(0, len(conditions)-2)
	reported := size + rows*audit.ReportedRowOverhead
	audit.SizedRows[rows] = reported
	return reported, nil
}

func (audit *volumeEvidenceStageAudit) Transact(
	ctx context.Context, conditions []testkeyvalue.Condition, mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	size, err := measureVolumeRemovalEvidenceTransaction(conditions, mutations)
	if err != nil {
		return testkeyvalue.TransactionResult{}, err
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
		return testkeyvalue.TransactionResult{}, errs.New(errs.KindRequestFailed, "lost evidence staging response")
	}
	return result, err
}

func measureVolumeRemovalEvidenceTransaction(
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (int, error) {
	if len(conditions)+len(mutations) > removalrecord.EvidenceTransactionOperations {
		return 0, errs.New(errs.KindInternal, "volume removal evidence operation budget exceeded")
	}
	for _, condition := range conditions {
		if condition.Key == "" || !strings.HasPrefix(condition.Key, "/") {
			return 0, errs.New(errs.KindValidationFailed, "etcd logical keys must begin with /")
		}
		if len("/groundplane"+condition.Key) > removalrecord.EvidenceKeyBytes {
			return 0, errs.New(errs.KindInternal, "volume removal evidence comparison key is oversized")
		}
	}
	for _, mutation := range mutations {
		if mutation.Key == "" || !strings.HasPrefix(mutation.Key, "/") {
			return 0, errs.New(errs.KindValidationFailed, "etcd logical keys must begin with /")
		}
		if len("/groundplane"+mutation.Key) > removalrecord.EvidenceKeyBytes ||
			len(mutation.Value) > removalrecord.EvidenceRecordBytes {
			return 0, errs.New(errs.KindInternal, "volume removal evidence mutation is oversized")
		}
	}
	budget, err := etcd.MeasureTransactionBudget(
		context.Background(),
		"/groundplane/",
		conditions,
		mutations,
	)
	if err != nil {
		return 0, err
	}
	return budget.Bytes, nil
}
