package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testentries "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	testentryvalues "github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
)

// PublishManualJourneyEntry publishes Entry metadata through the desired-state
// publisher and stores its bytes through the immutable value repository.
func (fixture *ExecutedArtifactFixture) PublishManualJourneyEntry(
	t *testing.T,
	value string,
	ciphertext []byte,
	secretID string,
) (*testentryvalues.Repository, *SecretRepository, testentries.Record) {
	return fixture.publishJourneyEntry(t, value, ciphertext, secretID, "")
}

// PublishAppliedRemovalJourneyEntry supplies completed Agent evidence to the
// Entry acknowledgement path; it does not execute a host materialization.
func (fixture *ExecutedArtifactFixture) PublishAppliedRemovalJourneyEntry(
	t *testing.T, value string,
) (*testentryvalues.Repository, *SecretRepository, testentries.Record) {
	return fixture.publishJourneyEntry(t, value, nil, "", testtaskjournal.TaskResourceEntry)
}

func (fixture *ExecutedArtifactFixture) publishJourneyEntry(
	t *testing.T, value string, ciphertext []byte, secretID, resourceKind string,
) (*testentryvalues.Repository, *SecretRepository, testentries.Record) {
	t.Helper()
	ctx := context.Background()
	values, err := testentryvalues.New(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	secrets, err := newSecretRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	desired := core.EnvEntry{
		ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKindEnv, Key: "MANUAL_VALUE",
		Source: core.EntrySource{Kind: core.SourceLiteral, Literal: value}, Exposure: []string{"api"},
	}
	if len(ciphertext) != 0 {
		uid, gid := uint32(1000), uint32(1000)
		desired.Kind, desired.Key, desired.Path = core.EntryKindFile, "", "run/secrets/manual-value"
		desired.UID, desired.GID, desired.Secret = &uid, &gid, true
		desired.Source.Literal = ""
	}
	if secretID != "" {
		desired.Source = core.EntrySource{Kind: core.SourceSecretRef, SecretRef: secretID}
	}
	entry, err := testentries.NewBlueprintRecord(
		fixture.Environment.Record.ID,
		"manual-value",
		desired,
		ids.New(ids.KindConfig),
	)
	if err != nil {
		t.Fatal(err)
	}
	if desired.Secret {
		digest := sha256.Sum256(ciphertext)
		err = values.CreateSecret(ctx, testentryvalues.SecretGeneration{
			EnvironmentID: entry.EnvironmentID, EntryID: entry.Entry.ID, GenerationID: entry.CurrentValueGenerationID,
			EnvelopeVersion: 1, Cipher: "age-x25519", DigestAlgorithm: "sha256",
			CiphertextSHA256: hex.EncodeToString(digest[:]), Ciphertext: ciphertext,
			CreatedAt: fixture.Environment.Record.CreatedAt,
		})
	} else {
		digest := sha256.Sum256([]byte(value))
		err = values.CreatePlain(ctx, testentryvalues.PlainGeneration{
			EnvironmentID: entry.EnvironmentID, EntryID: entry.Entry.ID,
			GenerationID: entry.CurrentValueGenerationID, Content: []byte(value),
			PlaintextSHA256: hex.EncodeToString(digest[:]), CreatedAt: fixture.Environment.Record.CreatedAt,
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	current, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, entry.EnvironmentID)
	if err != nil || !found {
		t.Fatalf("Entry journey desired projection = %t, %v", found, err)
	}
	task := fixture.Task(t, 952)
	if resourceKind != "" {
		task.Params[testtaskjournal.TaskResourceKindParam] = resourceKind
	}
	task.RenderGeneration = int32(current.Record.RenderGeneration + 1)
	projection := current.Record
	projection.RevisionID, projection.RenderGeneration = task.ID, uint64(task.RenderGeneration)
	projection.Entries = append(projection.Entries, entry)
	entries, err := newEntryRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	if err := entries.BindBlueprintEntryEnvironment(ctx, entry.EnvironmentID, entry.Entry.ID); err != nil {
		t.Fatal(err)
	}
	fixture.Publish(t, task, projection, BlueprintReleasePublication{})
	owner, found, err := entries.ResolveBlueprintEntryEnvironment(ctx, entry.Entry.ID)
	if err != nil || !found || owner != entry.EnvironmentID {
		t.Fatal("published Entry lookup does not resolve its Environment")
	}
	published, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, owner)
	if err != nil || !found || len(published.Record.Entries) != 1 ||
		!testentries.EqualRecord(published.Record.Entries[0], entry) {
		t.Fatal("published Entry metadata substituted its identity, generation, or desired fields")
	}
	if desired.Secret {
		generation, found, err := values.GetSecret(ctx, entry.Entry.ID, entry.CurrentValueGenerationID)
		if err != nil || !found {
			t.Fatalf("persisted encrypted Entry generation = %t, %v", found, err)
		}
		defer clear(generation.Ciphertext)
		if !bytes.Equal(generation.Ciphertext, ciphertext) || bytes.Contains(generation.Ciphertext, []byte(value)) {
			t.Fatal("persisted encrypted generation substituted ciphertext or exposed plaintext")
		}
		plain, plainFound, err := values.GetPlain(ctx, entry.Entry.ID, entry.CurrentValueGenerationID)
		clear(plain.Content)
		if err != nil || plainFound {
			t.Fatal("encrypted generation also has a plaintext value record")
		}
		if published.Record.Entries[0].Entry.Source.Literal != "" {
			t.Fatal("published encrypted Entry metadata contains plaintext")
		}
	}
	agentID := ids.New(ids.KindAgent)
	claim, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != task.ID {
		t.Fatalf("Entry projection claim = %t, %v", found, err)
	}
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID,
		claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1, Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	return values, secrets, entry
}

func (fixture *ExecutedArtifactFixture) CheckManualJourneyEntryReference(
	t *testing.T,
	entry testentries.Record,
	active bool,
) {
	t.Helper()
	key := testscriptsourceevidence.ScriptSourceCountKey(testscriptsourcereference.SourceIdentity{
		Kind: testscriptsourcereference.SourceEntryValue, EntryID: entry.Entry.ID, ValueGenerationID: entry.CurrentValueGenerationID,
	})
	value := fixture.store.valueAt(key, fixture.store.revision)
	if !active {
		if value != nil {
			t.Fatal("terminal Entry generation retained a Script removal fence")
		}
		return
	}
	if value == nil {
		t.Fatal("active Entry generation has no Script removal fence")
	}
	count, err := testscriptsourceevidence.DecodeScriptSourceCount(value.Value)
	if err != nil || count.ReferencedExecutionCount != 1 {
		t.Fatalf("Entry generation reference count = %v", err)
	}
}
