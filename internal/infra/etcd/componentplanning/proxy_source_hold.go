package componentplanning

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProxySourceHold names the visible Apply's ordinary source-reference root.
// Child Tasks acquire their own references before this parent may finish.
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

func (preparation ComponentTaskPreparation) WithProxySourceHold(hold ProxySourceHold,
	fragment scriptsourcepublication.ScriptSourcePublicationFragment,
) (ComponentTaskPreparation, error) {
	if hold.Validate() != nil || preparation.proxyServicePreflight == nil ||
		preparation.proxyServicePreflight.CandidateInput == nil || len(fragment.StagedRequirements()) != 0 {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"pending proxy source hold requires captured existing sources",
		)
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
			return ComponentTaskPreparation{}, errs.New(
				errs.KindValidationFailed,
				"pending proxy source hold differs from its publication",
			)
		}
		rootFound = true
	}
	if !rootFound {
		return ComponentTaskPreparation{}, errs.New(
			errs.KindValidationFailed,
			"pending proxy source hold has no root publication",
		)
	}
	result := cloneComponentTaskPreparation(preparation)
	owned := hold
	result.proxyServicePreflight.SourceHold = &owned
	result.proxyServiceConditions = append(result.proxyServiceConditions, fragment.Conditions()...)
	result.proxySourceMutations = cloneProxySourceMutations(fragment.Mutations())
	return result, nil
}

func cloneProxySourceMutations(mutations []keyvalue.Mutation) []keyvalue.Mutation {
	result := slices.Clone(mutations)
	for index := range result {
		result[index].Value = slices.Clone(result[index].Value)
	}
	return result
}

func (publication Publication) ValidateProxySourceHoldParent(parentID, operationID string) error {
	preflight := publication.preparation.proxyServicePreflight
	if preflight == nil || preflight.SourceHold == nil {
		return nil
	}
	if preflight.ParentTaskID != parentID || preflight.SourceHold.OperationID != operationID ||
		preflight.SourceHold.Validate() != nil {
		return errs.New(errs.KindStateConflict, "pending proxy source hold belongs to another Apply")
	}
	return nil
}
