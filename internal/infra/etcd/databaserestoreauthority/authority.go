// Package databaserestoreauthority verifies a database Restore's exact
// selected Attach, dependency indexes and applied Service facts.
package databaserestoreauthority

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicefactauthority"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type store interface {
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
}

type keyExpectation struct {
	revision int64
	sha256   string
}

// Read re-reads the complete selected authority at the caller's fixed view.
// The returned exact-key compares belong in the same claim, checkpoint or
// terminal transaction as the receipt that depends on them.
func Read(
	ctx context.Context,
	storage store,
	record backupruntime.BackupRestoreRecord,
	revision int64,
) ([]etcdstore.Condition, error) {
	if storage == nil || revision <= 0 {
		return nil, conflict()
	}
	target := databaseTarget(record)
	if target == nil {
		return nil, conflict()
	}
	expected := make(map[string]keyExpectation)
	add := func(key string, value keyExpectation) error {
		if key == "" || value.revision <= 0 {
			return conflict()
		}
		if previous, found := expected[key]; found {
			if previous.revision != value.revision ||
				previous.sha256 != "" && value.sha256 != "" && previous.sha256 != value.sha256 {
				return conflict()
			}
			if previous.sha256 == "" {
				expected[key] = value
			}
			return nil
		}
		expected[key] = value
		return nil
	}
	base := []struct {
		key   string
		value keyExpectation
	}{
		{attachments.AttachKey(target.attachID), keyExpectation{target.attachRevision, target.attachSHA256}},
		{attachments.AttachFactsKey(target.attachID), keyExpectation{target.attachFactsRevision, ""}},
		{hierarchy.ProjectKey(target.backingProjectID), keyExpectation{target.backingProjectRevision, ""}},
		{hierarchy.EnvironmentKey(target.backingEnvironmentID), keyExpectation{target.backingEnvironmentRevision, ""}},
		{
			hierarchy.EnvironmentKey(target.consumerEnvironmentID),
			keyExpectation{target.consumerEnvironmentRevision, ""},
		},
	}
	for _, item := range base {
		if err := add(item.key, item.value); err != nil {
			return nil, err
		}
	}
	serviceSnapshots := append(
		[]backupruntime.BackupRestoreDatabaseServiceSnapshot{target.databaseService},
		target.consumers...,
	)
	for _, service := range serviceSnapshots {
		kind := servicefactauthority.ReleaseRuntime
		if service.ServiceID == target.databaseService.ServiceID {
			kind = servicefactauthority.BackingRuntime
		}
		facts := []struct {
			key   string
			value keyExpectation
		}{
			{
				blueprints.EnvironmentBlueprintHeadKey(service.EnvironmentID),
				keyExpectation{service.ServiceRevision, service.ServiceSHA256},
			},
			{
				services.ServiceRuntimeKey(service.ServiceID),
				keyExpectation{service.IntentRevision, service.IntentSHA256},
			},
			{
				servicefactauthority.Key(kind, service.EnvironmentID, service.ServiceID),
				keyExpectation{service.ComposeRevision, service.ComposeSHA256},
			},
		}
		for _, item := range facts {
			if err := add(item.key, item.value); err != nil {
				return nil, err
			}
		}
	}
	for _, index := range target.dependentIndexes {
		if err := add(index.Key, keyExpectation{index.Revision, ""}); err != nil {
			return nil, err
		}
		if err := add(attachments.AttachKey(index.AttachID), keyExpectation{
			revision: index.AttachRevision,
			sha256:   index.AttachSHA256,
		}); err != nil {
			return nil, err
		}
	}
	keys := make([]string, 0, len(expected))
	for key := range expected {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	read, err := storage.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			etcdstore.ClearValues(read.Values)
		}
		return nil, conflict()
	}
	defer etcdstore.ClearValues(read.Values)
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for index, key := range keys {
		value := read.Values[index]
		proof := expected[key]
		if value == nil || value.Key != key || value.ModRevision != proof.revision {
			return nil, conflict()
		}
		if proof.sha256 != "" {
			digest := sha256.Sum256(value.Value)
			if hex.EncodeToString(digest[:]) != proof.sha256 {
				return nil, conflict()
			}
		}
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: proof.revision})
	}
	prefixes := []string{
		attachments.AttachGrantedByPrefix(target.attachID),
		attachments.AttachCredentialByPrefix(target.attachID),
	}
	parents := map[string]bool{target.attachID: true}
	for _, index := range target.dependentIndexes {
		if !parents[index.AttachID] {
			parents[index.AttachID] = true
			prefixes = append(prefixes, attachments.AttachCredentialByPrefix(index.AttachID))
		}
	}
	sort.Strings(prefixes)
	for _, index := range target.dependentIndexes {
		bound := false
		for _, prefix := range prefixes {
			if index.Key == prefix+index.AttachID {
				bound = true
				break
			}
		}
		if !bound {
			return nil, conflict()
		}
	}
	for _, prefix := range prefixes {
		page, err := storage.Range(ctx, etcdstore.RangeRequest{
			Prefix:   prefix,
			Limit:    backupruntime.MaxDatabaseRestoreConsumers + 1,
			Revision: revision,
		})
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision != revision || page.More ||
			len(page.Values) > backupruntime.MaxDatabaseRestoreConsumers {
			if page != nil {
				etcdstore.ClearRangeValues(page.Values)
			}
			return nil, conflict()
		}
		seen := make(map[string]bool, len(page.Values))
		for _, row := range page.Values {
			id := strings.TrimPrefix(row.Key, prefix)
			proof, found := expected[row.Key]
			if !found || proof.revision != row.ModRevision || !bytes.Equal(row.Value, []byte(id)) {
				etcdstore.ClearRangeValues(page.Values)
				return nil, conflict()
			}
			seen[row.Key] = true
		}
		for _, index := range target.dependentIndexes {
			if strings.HasPrefix(index.Key, prefix) && !seen[index.Key] {
				etcdstore.ClearRangeValues(page.Values)
				return nil, conflict()
			}
		}
		etcdstore.ClearRangeValues(page.Values)
	}
	return conditions, nil
}

func conflict() error {
	return errs.New(errs.KindStateConflict, "database Restore selected target or consumer closure changed")
}

type selectedTarget struct {
	consumerEnvironmentID       string
	consumerEnvironmentRevision int64
	attachID                    string
	attachRevision              int64
	attachFactsRevision         int64
	attachSHA256                string
	backingProjectID            string
	backingProjectRevision      int64
	backingEnvironmentID        string
	backingEnvironmentRevision  int64
	databaseService             backupruntime.BackupRestoreDatabaseServiceSnapshot
	consumers                   []backupruntime.BackupRestoreDatabaseServiceSnapshot
	dependentIndexes            []backupruntime.BackupRestoreDatabaseDependentIndex
}

func databaseTarget(record backupruntime.BackupRestoreRecord) *selectedTarget {
	if target := record.CurrentTarget.Postgres; target != nil {
		return selectedDatabaseTarget(target.Source.ConsumerEnvironmentID, target.ConsumerEnvironmentRevision,
			target.Source.AttachID, target.Source.AttachRevision, target.Source.AttachFactsRevision,
			target.AttachSHA256, target.Source.BackingProjectID, target.Source.BackingProjectRevision,
			target.Source.BackingEnvironmentID, target.Source.BackingEnvironmentRevision,
			target.DatabaseService, target.Consumers, target.DependentIndexes)
	}
	if target := record.CurrentTarget.MySQL; target != nil {
		return selectedDatabaseTarget(target.Source.ConsumerEnvironmentID, target.ConsumerEnvironmentRevision,
			target.Source.AttachID, target.Source.AttachRevision, target.Source.AttachFactsRevision,
			target.AttachSHA256, target.Source.BackingProjectID, target.Source.BackingProjectRevision,
			target.Source.BackingEnvironmentID, target.Source.BackingEnvironmentRevision,
			target.DatabaseService, target.Consumers, target.DependentIndexes)
	}
	return nil
}

func selectedDatabaseTarget(consumerEnvironmentID string, consumerEnvironmentRevision int64,
	attachID string, attachRevision, attachFactsRevision int64, attachSHA256,
	backingProjectID string, backingProjectRevision int64, backingEnvironmentID string,
	backingEnvironmentRevision int64, databaseService backupruntime.BackupRestoreDatabaseServiceSnapshot,
	consumers []backupruntime.BackupRestoreDatabaseServiceSnapshot,
	dependentIndexes []backupruntime.BackupRestoreDatabaseDependentIndex,
) *selectedTarget {
	return &selectedTarget{
		consumerEnvironmentID: consumerEnvironmentID, consumerEnvironmentRevision: consumerEnvironmentRevision,
		attachID: attachID, attachRevision: attachRevision, attachFactsRevision: attachFactsRevision,
		attachSHA256: attachSHA256, backingProjectID: backingProjectID,
		backingProjectRevision: backingProjectRevision, backingEnvironmentID: backingEnvironmentID,
		backingEnvironmentRevision: backingEnvironmentRevision, databaseService: databaseService,
		consumers: consumers, dependentIndexes: dependentIndexes,
	}
}
