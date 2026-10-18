package backupwork

import (
	"strconv"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

type credentialGrant struct {
	AttemptID     string              `json:"attempt_id"`
	Input         workexecution.Input `json:"input"`
	SourceOrdinal uint32              `json:"source_ordinal"`
	Deadline      time.Time           `json:"deadline"`
}

// PrepareCredentialGrant joins the authenticated credential reader's guards
// with a bounded delivery record. Plaintext may be opened only after this change
// commits successfully; an uncertain response is not a delivery authorization.
func PrepareCredentialGrant(attempt workexecution.Attempt, ordinal uint32,
	conditions []keyvalue.Condition,
) (workqueue.Change, error) {
	if err := attempt.Validate(); err != nil {
		return workqueue.Change{}, err
	}
	if len(conditions) == 0 {
		return workqueue.Change{}, conflict("Backup credential delivery requires ownership guards")
	}
	encoded, err := recordcodec.Encode("backup-work-credential-grant", credentialGrant{
		AttemptID: attempt.ID, Input: attempt.Input, SourceOrdinal: ordinal, Deadline: attempt.Deadline,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	// Reconnect replaces this source's grant instead of accumulating records.
	key := credentialGrantKey(attempt.WorkItemID, ordinal)
	return workqueue.Change{Conditions: conditions,
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: encoded}}}, nil
}

func credentialGrantKey(workID string, ordinal uint32) string {
	return credentialGrantPrefix(workID) + strconv.FormatUint(uint64(ordinal), 10)
}

func credentialGrantPrefix(workID string) string {
	return "/v1/runtime/backup-work-credential-grants/" + workID + "/"
}
