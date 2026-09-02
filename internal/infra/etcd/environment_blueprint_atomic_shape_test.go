package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentBlueprintAtomicShape struct {
	name                         string
	releases                     int
	hooks                        int
	physicalSources              int
	attaches                     bool
	backup                       bool
	comparisons                  int
	successMutations             int
	failureReads                 int
	detachRetainedBeforeFinal    bool
	tombstoneRetainedBeforeFinal bool
}

func TestEnvironmentBlueprintFinalPublicationExactLegalShapes(t *testing.T) {
	tests := []environmentBlueprintAtomicShape{
		{
			name:     "QA eleven Releases two hooks three physical sources",
			releases: 11, hooks: 2, physicalSources: 3,
			comparisons: 31, successMutations: 43, failureReads: 31,
		},
		{
			name:     "maximum non-Backup Script",
			releases: 32, hooks: 16, physicalSources: 17,
			comparisons: 45, successMutations: 99, failureReads: 45,
		},
		{
			name:     "maximum non-Backup Script and two candidate Attaches",
			releases: 32, hooks: 16, physicalSources: 17, attaches: true,
			comparisons: 118, successMutations: 148, failureReads: 118,
		},
		{
			name:     "Backup-only maximum",
			attaches: true, backup: true,
			comparisons: 152, successMutations: 90, failureReads: 152,
		},
		{
			name:     "combined Backup and Script maximum",
			releases: 32, hooks: 16, physicalSources: 17, attaches: true, backup: true,
			comparisons: 175, successMutations: 176, failureReads: 175,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			published, err := publishEnvironmentBlueprintAtomicShape(t, test, false)
			if err != nil {
				t.Fatalf("PublishEnvironmentBlueprintDesiredRevision() error = %v", err)
			}
			if published.audit.comparisons != test.comparisons ||
				published.audit.successMutations != test.successMutations ||
				published.audit.failureReads != test.failureReads {
				t.Fatalf(
					"final publication shape = %d/%d/%d, want %d/%d/%d",
					published.audit.comparisons,
					published.audit.successMutations,
					published.audit.failureReads,
					test.comparisons,
					test.successMutations,
					test.failureReads,
				)
			}
			outcome, _, conflict, classifyErr := published.result.Classify()
			if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
				t.Fatalf("final publication = %v/%v/%v", outcome, conflict, classifyErr)
			}
			assertEnvironmentBlueprintAtomicShapeVisible(t, published, published.store.revision)
		})
	}
}

func TestEnvironmentBlueprintCombinedMaximumInjectedFailurePublishesNoAuthority(t *testing.T) {
	test := environmentBlueprintAtomicShape{
		name: "combined failure", releases: 32, hooks: 16, physicalSources: 17,
		attaches: true, backup: true,
		comparisons: 175, successMutations: 176, failureReads: 175,
	}
	published, err := publishEnvironmentBlueprintAtomicShape(t, test, true)
	if !isKind(err, errs.KindInternal) {
		t.Fatalf("PublishEnvironmentBlueprintDesiredRevision(injected failure) error = %v", err)
	}
	if published.audit.comparisons != test.comparisons ||
		published.audit.successMutations != test.successMutations ||
		published.audit.failureReads != test.failureReads {
		t.Fatalf(
			"injected final publication shape = %d/%d/%d, want %d/%d/%d",
			published.audit.comparisons,
			published.audit.successMutations,
			published.audit.failureReads,
			test.comparisons,
			test.successMutations,
			test.failureReads,
		)
	}
	for _, key := range append(
		[]string{
			environmentBlueprintHeadKey(published.environmentID),
			taskKey(published.task.ID),
			taskQueueKey(published.task.Executor, published.task.ID),
			published.markerKey,
			releasePublicationKey(published.releasePublicationID),
			backupKeyKey(published.environmentID),
			backupKeyValueKey(published.environmentID),
		},
		published.candidateAttachKeys()...,
	) {
		read, getErr := published.store.Get(context.Background(), key)
		if getErr != nil || read.Entry != nil {
			t.Fatalf("failed publication exposed %q = %#v, %v", key, read, getErr)
		}
	}
	for _, sourceID := range published.newBackupSourceIDs {
		for _, key := range []string{
			backupSourceKey(sourceID),
			backupSourceEnvironmentKey(published.environmentID, sourceID),
		} {
			read, getErr := published.store.Get(context.Background(), key)
			if getErr != nil || read.Entry != nil {
				t.Fatalf("failed publication exposed %q = %#v, %v", key, read, getErr)
			}
		}
	}
	for _, source := range published.newBackupSources {
		read, getErr := published.store.Get(context.Background(), backupSourceIdentityKey(
			published.environmentID, source.Kind, source.TargetID,
		))
		if getErr != nil || read.Entry != nil {
			t.Fatalf("failed publication exposed source identity %#v = %#v, %v", source, read, getErr)
		}
	}
	for key, revision := range published.preexistingBackupRevisions {
		read, getErr := published.store.Get(context.Background(), key)
		if getErr != nil || read.Entry == nil || read.Entry.ModRevision != revision {
			t.Fatalf("failed publication changed pre-existing Backup authority %q = %#v, %v", key, read, getErr)
		}
	}
	for _, retained := range published.retainedAttachRevisions {
		read, getErr := published.store.Get(context.Background(), attachKey(retained.Record.ID))
		if getErr != nil || read.Entry == nil || read.Entry.ModRevision != retained.Revision {
			t.Fatalf("failed publication changed retained Attach %q = %#v, %v", retained.Record.ID, read, getErr)
		}
	}
	for _, relation := range published.retainedGrantRelations {
		read, getErr := published.store.Get(context.Background(), attachGrantedByKey(relation[0], relation[1]))
		if getErr != nil || read.Entry != nil {
			t.Fatalf("failed publication exposed retained grant relation %#v = %#v, %v", relation, read, getErr)
		}
	}
	active, getErr := published.store.Get(context.Background(), scriptSetActiveKey(published.environmentID))
	if getErr != nil || active.Entry == nil || active.Entry.ModRevision != published.activeScriptRevision {
		t.Fatalf("failed publication changed active Script generation = %#v, %v", active, getErr)
	}
}

func TestEnvironmentBlueprintRetainedAttachDetachRaceHasOneWinner(t *testing.T) {
	published, err := publishEnvironmentBlueprintAtomicShape(t, environmentBlueprintAtomicShape{
		name: "retained Attach detach race", attaches: true, detachRetainedBeforeFinal: true,
	}, false)
	if err != nil {
		t.Fatalf("PublishEnvironmentBlueprintDesiredRevision(detach race) error = %v", err)
	}
	outcome, _, conflict, classifyErr := published.result.Classify()
	if classifyErr != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("retained Attach detach race = %v/%v/%v", outcome, conflict, classifyErr)
	}
	if published.detachWinner.Record.Status != core.AttachDetaching ||
		published.detachWinner.Revision <= published.retainedAttachRevisions[0].Revision {
		t.Fatalf("direct detach did not win = %#v", published.detachWinner)
	}
	for _, key := range append(
		[]string{
			environmentBlueprintHeadKey(published.environmentID),
			taskKey(published.task.ID),
			taskQueueKey(published.task.Executor, published.task.ID),
			published.markerKey,
		},
		published.candidateAttachKeys()...,
	) {
		read, getErr := published.store.Get(context.Background(), key)
		if getErr != nil || read.Entry != nil {
			t.Fatalf("raced publication exposed %q = %#v, %v", key, read, getErr)
		}
	}
	for _, relation := range published.retainedGrantRelations {
		read, getErr := published.store.Get(context.Background(), attachGrantedByKey(relation[0], relation[1]))
		if getErr != nil || read.Entry != nil {
			t.Fatalf("raced publication exposed retained grant relation %#v = %#v, %v", relation, read, getErr)
		}
	}
}

func TestEnvironmentBlueprintRetainedAttachDeletionRacePublishesNothing(t *testing.T) {
	published, err := publishEnvironmentBlueprintAtomicShape(t, environmentBlueprintAtomicShape{
		name: "retained Attach deletion race", attaches: true, tombstoneRetainedBeforeFinal: true,
	}, false)
	if err != nil {
		t.Fatalf("PublishEnvironmentBlueprintDesiredRevision(deletion race) error = %v", err)
	}
	outcome, _, conflict, classifyErr := published.result.Classify()
	if classifyErr != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindStateConflict) {
		t.Fatalf("retained Attach deletion race = %v/%v/%v", outcome, conflict, classifyErr)
	}
	for _, key := range append(
		[]string{
			environmentBlueprintHeadKey(published.environmentID),
			taskKey(published.task.ID),
			taskQueueKey(published.task.Executor, published.task.ID),
			published.markerKey,
		},
		published.candidateAttachKeys()...,
	) {
		read, getErr := published.store.Get(context.Background(), key)
		if getErr != nil || read.Entry != nil {
			t.Fatalf("deletion-raced publication exposed %q = %#v, %v", key, read, getErr)
		}
	}
	for _, relation := range published.retainedGrantRelations {
		read, getErr := published.store.Get(context.Background(), attachGrantedByKey(relation[0], relation[1]))
		if getErr != nil || read.Entry != nil {
			t.Fatalf("deletion-raced publication exposed retained grant relation %#v = %#v, %v", relation, read, getErr)
		}
	}
	tombstone, getErr := published.store.Get(
		context.Background(), deletionTombstoneKey("attach", published.retainedAttachRevisions[0].Record.ID),
	)
	if getErr != nil || tombstone.Entry == nil {
		t.Fatalf("retained Attach deletion tombstone = %#v, %v", tombstone, getErr)
	}
}
