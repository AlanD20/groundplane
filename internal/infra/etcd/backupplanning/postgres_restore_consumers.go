package backupplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type postgresRestoreConsumerIdentity struct {
	ServiceID     string
	EnvironmentID string
}

// A selected Attach's own Service is not the entire database consumer set.
// Granted and credential-reusing Attaches can reach the same database; their
// ready Services must be sealed and stopped as part of the Restore boundary.
func selectPostgresRestoreConsumers(ctx context.Context, snapshot *restoreSnapshot,
	current backupruntime.BackupPostgresSourceSnapshot, revision int64,
) ([]postgresRestoreConsumerIdentity, []backupruntime.BackupRestorePostgresDependentIndex, error) {
	owner := postgresRestoreConsumerIdentity{ServiceID: current.ConsumerServiceID,
		EnvironmentID: current.ConsumerEnvironmentID}
	selected := map[string]postgresRestoreConsumerIdentity{owner.ServiceID: owner}
	indexes := make([]backupruntime.BackupRestorePostgresDependentIndex, 0)
	// A credential child inherits its parent's grant facts. Walk credential
	// descendants of every discovered Attach, including grant consumers, while
	// the grant edge itself is selected only from the original database owner.
	queue := []string{current.AttachID}
	seen := map[string]bool{current.AttachID: true}
	for len(queue) != 0 {
		parentID := queue[0]
		queue = queue[1:]
		prefixes := []struct {
			prefix string
			grant  bool
		}{{attachments.AttachCredentialByPrefix(parentID), false}}
		if parentID == current.AttachID {
			prefixes = append(prefixes, struct {
				prefix string
				grant  bool
			}{attachments.AttachGrantedByPrefix(parentID), true})
		}
		for _, index := range prefixes {
			read, err := snapshot.fixedSnapshotStore.Range(ctx, etcdstore.RangeRequest{
				Prefix: index.prefix, Limit: backupruntime.MaxPostgresRestoreConsumers + 1,
				Revision: revision})
			if err != nil {
				return nil, nil, err
			}
			if read.More || len(read.Values) > backupruntime.MaxPostgresRestoreConsumers {
				etcdstore.ClearRangeValues(read.Values)
				return nil, nil, errs.New(
					errs.KindValidationFailed,
					"PostgreSQL Restore consumer closure exceeds its bound",
				)
			}
			for _, row := range read.Values {
				id := strings.TrimPrefix(row.Key, index.prefix)
				if !strings.HasPrefix(row.Key, index.prefix) || ids.Validate(ids.KindAttach, id) != nil ||
					!bytes.Equal(row.Value, []byte(id)) || row.ModRevision <= 0 {
					etcdstore.ClearRangeValues(read.Values)
					return nil, nil, errs.New(errs.KindStateConflict, "PostgreSQL Restore consumer index is invalid")
				}
				if previous, exists := snapshot.conditions[row.Key]; exists && previous != row.ModRevision {
					etcdstore.ClearRangeValues(read.Values)
					return nil, nil, errs.New(
						errs.KindStateConflict,
						"PostgreSQL Restore consumer index changed at its fixed view",
					)
				}
				snapshot.conditions[row.Key] = row.ModRevision
				childRead, err := snapshot.GetMany(ctx, etcdstore.GetManyRequest{
					Keys: []string{attachments.AttachKey(id)}, Revision: revision})
				if err != nil {
					etcdstore.ClearRangeValues(read.Values)
					return nil, nil, err
				}
				if len(childRead.Values) != 1 || childRead.Values[0] == nil {
					etcdstore.ClearValues(childRead.Values)
					etcdstore.ClearRangeValues(read.Values)
					return nil, nil, errs.New(
						errs.KindStateConflict,
						"PostgreSQL Restore dependent Attach is unavailable",
					)
				}
				child, decodeErr := attachments.DecodeAttachRecord(childRead.Values[0].Value)
				childRevision := childRead.Values[0].ModRevision
				childSHA := sha256.Sum256(childRead.Values[0].Value)
				etcdstore.ClearValues(childRead.Values)
				if decodeErr != nil || child.ID != id || string(child.Status) != backupAttachStatusReady ||
					child.BackingServiceID != current.BackingServiceID ||
					child.EnvironmentID != current.ConsumerEnvironmentID ||
					index.grant && !slices.Contains(child.GrantAttachIDs, parentID) ||
					!index.grant && child.CredentialAttachID != parentID {
					etcdstore.ClearRangeValues(read.Values)
					return nil, nil, errs.New(errs.KindStateConflict, "PostgreSQL Restore dependent Attach changed")
				}
				indexes = append(indexes, backupruntime.BackupRestorePostgresDependentIndex{
					Key: row.Key, Revision: row.ModRevision, AttachID: id,
					AttachRevision: childRevision, AttachSHA256: hex.EncodeToString(childSHA[:])})
				identity := postgresRestoreConsumerIdentity{ServiceID: child.ServiceID,
					EnvironmentID: child.EnvironmentID}
				if previous, exists := selected[identity.ServiceID]; exists && previous != identity {
					etcdstore.ClearRangeValues(read.Values)
					return nil, nil, errs.New(
						errs.KindStateConflict,
						"PostgreSQL Restore consumer Service ownership is ambiguous",
					)
				}
				selected[identity.ServiceID] = identity
				if !seen[id] {
					seen[id] = true
					queue = append(queue, id)
				}
				if len(seen) > backupruntime.MaxPostgresRestoreConsumers {
					etcdstore.ClearRangeValues(read.Values)
					return nil, nil, errs.New(
						errs.KindValidationFailed,
						"PostgreSQL Restore consumer closure exceeds its bound",
					)
				}
			}
			etcdstore.ClearRangeValues(read.Values)
		}
	}
	if len(selected) > backupruntime.MaxPostgresRestoreConsumers {
		return nil, nil, errs.New(errs.KindValidationFailed, "PostgreSQL Restore consumer closure exceeds its bound")
	}
	result := make([]postgresRestoreConsumerIdentity, 0, len(selected))
	for _, identity := range selected {
		result = append(result, identity)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ServiceID < result[j].ServiceID })
	sort.Slice(indexes, func(i, j int) bool { return indexes[i].Key < indexes[j].Key })
	return result, indexes, nil
}
