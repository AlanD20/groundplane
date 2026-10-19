package componentplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProxySourceHold names the visible Apply's ordinary source-reference root.
// Dependent work acquires its references before this Apply releases the root.
type ProxySourceHold struct {
	OperationID      string `json:"operation_id"`
	MembershipCount  uint64 `json:"membership_count"`
	MembershipSHA256 string `json:"membership_sha256"`
}

func readProxySourceHoldCondition(ctx context.Context, store planningStore, hold ProxySourceHold,
	revision int64,
) (keyvalue.Condition, error) {
	if hold.Validate() != nil {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "pending proxy source hold is invalid")
	}
	key := scriptsourceevidence.ScriptSourceRootKey(hold.OperationID)
	read, err := store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return keyvalue.Condition{}, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return keyvalue.Condition{}, errs.New(errs.KindInternal, "pending proxy source hold read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[0].Key != key {
		return keyvalue.Condition{}, errs.New(errs.KindStateConflict, "pending proxy source hold is unavailable")
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(read.Values[0].Value)
	if err != nil || root.OperationID != hold.OperationID || root.MembershipCount != hold.MembershipCount ||
		root.MembershipSHA256 != hold.MembershipSHA256 || root.Phase != scriptsourceevidence.ScriptOperationSourceActive {
		return keyvalue.Condition{}, errs.New(
			errs.KindStateConflict,
			"pending proxy source hold changed before Service publication",
		)
	}
	return keyvalue.Condition{Key: key, ModRevision: read.Values[0].ModRevision}, nil
}

func (hold ProxySourceHold) Validate() error {
	digest, err := hex.DecodeString(hold.MembershipSHA256)
	if ids.Validate(ids.KindOperation, hold.OperationID) != nil || hold.MembershipCount == 0 ||
		err != nil || len(digest) != sha256.Size || hex.EncodeToString(digest) != hold.MembershipSHA256 {
		return errs.New(errs.KindValidationFailed, "pending proxy source hold is invalid")
	}
	return nil
}

// ProxySourceOperationID gives the visible Apply one source-retention owner.
func ProxySourceOperationID(task workexecution.Task) (string, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionApply || task.Scope.EnvironmentID == "" {
		return "", errs.New(errs.KindValidationFailed, "Proxy sources require a captured Apply")
	}
	return ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "proxy-sources"), nil
}

// BindWorkProxySourceHold returns source activation writes for the same
// transaction that seals executable work input. It never commits independently.
func BindWorkProxySourceHold(work workqueue.View, preflight ProxyServicePreflight, hold ProxySourceHold,
	fragment scriptsourcepublication.ScriptSourcePublicationFragment,
) (*ProxyServicePreflight, workqueue.Change, error) {
	task, item := work.Task.Task, work.Item.Item
	operationID, err := ProxySourceOperationID(task)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	if work.ReadRevision <= 0 || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		item.Validate() != nil || item.TaskID != task.ID || item.Operation != workexecution.Component ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.ForwardRevoked ||
		task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil ||
		hold.Validate() != nil || hold.OperationID != operationID || preflight.validate() != nil ||
		preflight.ParentTaskID != task.ID || preflight.EnvironmentID != task.Scope.EnvironmentID ||
		preflight.CandidateInput == nil || preflight.CandidateInput.ComponentID != item.TargetID ||
		len(fragment.StagedRequirements()) != 0 {
		return nil, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"Proxy source hold differs from captured work",
		)
	}
	if preflight.SourceHold != nil && *preflight.SourceHold != hold {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Proxy work already holds different sources")
	}
	rootKey := scriptsourceevidence.ScriptSourceRootKey(hold.OperationID)
	rootFound := false
	for _, mutation := range fragment.Mutations() {
		if mutation.Key != rootKey {
			continue
		}
		root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(mutation.Value)
		if err != nil || mutation.Type != keyvalue.MutationPut || mutation.Prefix || rootFound ||
			root.OperationID != hold.OperationID || root.MembershipCount != hold.MembershipCount ||
			root.MembershipSHA256 != hold.MembershipSHA256 || root.Phase != scriptsourceevidence.ScriptOperationSourceActive {
			return nil, workqueue.Change{}, errs.New(
				errs.KindValidationFailed,
				"pending proxy source hold differs from its publication",
			)
		}
		rootFound = true
	}
	if !rootFound {
		return nil, workqueue.Change{}, errs.New(
			errs.KindValidationFailed,
			"pending proxy source hold has no root publication",
		)
	}
	result := cloneProxyServicePreflight(&preflight)
	owned := hold
	result.SourceHold = &owned
	change := workqueue.Change{
		Conditions: append(slices.Clone(fragment.Conditions()),
			keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
			keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: work.Item.Revision}),
		Mutations: cloneProxySourceMutations(fragment.Mutations()),
	}
	return result, change, nil
}

func cloneProxySourceMutations(mutations []keyvalue.Mutation) []keyvalue.Mutation {
	result := slices.Clone(mutations)
	for index := range result {
		result[index].Value = slices.Clone(result[index].Value)
	}
	return result
}
