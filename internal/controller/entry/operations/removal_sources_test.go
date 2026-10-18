package operations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-54: remove only this Entry from acknowledged files, retaining other
// applied generations and file ownership. Desired values are not cleanup input.
func TestRemovalSourcesPreservesAcknowledgedValuesAndOwnership(t *testing.T) {
	removed, retained := ids.New(ids.KindEnvEntry), ids.New(ids.KindEnvEntry)
	oldGeneration := ids.New(ids.KindConfig)
	selected := taskmaterialization.EntryValueReference{
		EntryID:           removed,
		ValueGenerationID: ids.New(ids.KindConfig),
		Storage:           taskmaterialization.EntryValueStorageSecret,
	}
	kept := taskmaterialization.EntryValueReference{
		EntryID:           retained,
		ValueGenerationID: oldGeneration,
		Storage:           taskmaterialization.EntryValueStoragePlain,
	}
	global := taskmaterialization.Source{Kind: taskmaterialization.SourceGeneratedEnvironment,
		GeneratedEnvironment: &taskmaterialization.GeneratedEnvironmentValueReference{FormatVersion: 1,
			Values: []taskmaterialization.GeneratedEnvironmentEntryReference{
				{Name: "REMOVE", Value: selected},
				{Name: "KEEP", Value: kept},
			}}}
	snapshot := runtimeconfiguration.Snapshot{
		EnvironmentID: ids.New(ids.KindEnvironment),
		Files: []taskmaterialization.Record{
			{
				Destination: ".env",
				OutputKind:  taskmaterialization.OutputGeneratedEnvironment,
				UID:         123,
				GID:         456,
				Mode:        0o600,
				Source:      global,
			},
			{
				Destination: "worker.env",
				ServiceID:   ids.New(ids.KindService),
				OutputKind:  taskmaterialization.OutputGeneratedEnvironment,
				Source: taskmaterialization.Source{Kind: taskmaterialization.SourceGeneratedEnvironment,
					GeneratedEnvironment: &taskmaterialization.GeneratedEnvironmentValueReference{FormatVersion: 1,
						Values: []taskmaterialization.GeneratedEnvironmentEntryReference{
							{Name: "REMOVE", Value: selected},
						}}},
			},
			{
				Destination: "secret.ini",
				OutputKind:  taskmaterialization.OutputSecretFile,
				UID:         1000,
				GID:         1001,
				Mode:        0o400,
				Source: taskmaterialization.Source{
					Kind:       taskmaterialization.SourceEntryValue,
					EntryValue: &selected,
				},
			},
			{Destination: "plain.ini", OutputKind: taskmaterialization.OutputPlainFile,
				Source: taskmaterialization.Source{Kind: taskmaterialization.SourceEntryValue, EntryValue: &selected}},
			{Destination: "unrelated.ini", OutputKind: taskmaterialization.OutputPlainFile,
				Source: taskmaterialization.Source{Kind: taskmaterialization.SourceEntryValue, EntryValue: &kept}},
		},
	}
	resolver := &removalSourceResolver{expectedEnvironment: snapshot.EnvironmentID, expectedGeneration: oldGeneration}
	service := &entryDesiredMutationService{materials: resolver}
	allocator := removalSourceAllocator(t)
	changes, err := service.removalSources(t.Context(), removed, snapshot, allocator)
	if err != nil || len(changes) != 4 || resolver.calls != 1 {
		t.Fatalf("removal sources: changes=%d reads=%d error=%v", len(changes), resolver.calls, err)
	}
	for _, change := range changes {
		switch change.Destination {
		case ".env":
			values := change.Source.GeneratedEnvironment.Values
			wantDigest := sha256.Sum256([]byte("KEEP=old\n"))
			if len(values) != 1 || values[0].Name != "KEEP" || values[0].Value != kept || change.UID != 123 ||
				change.GID != 456 || change.Mode != 0o600 || change.Length != 9 || change.SHA256 != hex.EncodeToString(wantDigest[:]) {
				t.Fatal("Entry removal changed retained applied content or file ownership")
			}
		case "worker.env", "secret.ini", "plain.ini":
			wantKind := taskmaterialization.OutputRemoveGeneratedEnv
			if change.Destination == "secret.ini" {
				wantKind = taskmaterialization.OutputRemoveSecretFile
				if change.UID != 1000 || change.GID != 1001 || change.Mode != 0o400 {
					t.Fatal("secret-file removal changed acknowledged ownership")
				}
			} else if change.Destination == "plain.ini" {
				wantKind = taskmaterialization.OutputRemovePlainFile
			}
			if change.OutputKind != wantKind || change.Source.Kind != taskmaterialization.SourceRemoval ||
				change.Length != 0 {
				t.Fatal("file removal retained a value source or changed output authority")
			}
		default:
			t.Fatal("unrelated file acquired cleanup authority")
		}
	}
	if len(snapshot.Files[0].Source.GeneratedEnvironment.Values) != 2 ||
		snapshot.Files[2].Source.EntryValue.EntryID != removed {
		t.Fatal("cleanup mutated its acknowledged input snapshot")
	}
	if !bytes.Equal(resolver.returned, make([]byte, len(resolver.returned))) {
		t.Fatal("cleanup retained resolved file plaintext")
	}
	// The same durable action reconstructs the same file identities after restart.
	again, err := service.removalSources(t.Context(), removed, snapshot, removalSourceAllocator(t))
	if err != nil || len(again) != len(changes) {
		t.Fatalf("recreated cleanup: %v", err)
	}
	for index := range changes {
		if changes[index].StepID != again[index].StepID ||
			changes[index].MaterializationID != again[index].MaterializationID {
			t.Fatal("recreated cleanup changed captured file identities")
		}
	}
}

// TASK-54: no applied file mentioning this Entry means no host cleanup. A
// partial renderer failure must clear returned plaintext and publish no sources.
func TestRemovalSourcesAbsentEntryAndPartialFailure(t *testing.T) {
	removed := ids.New(ids.KindEnvEntry)
	for _, fail := range []bool{false, true} {
		resolver := &removalSourceResolver{
			expectedEnvironment: ids.New(ids.KindEnvironment),
			expectedGeneration:  ids.New(ids.KindConfig),
			fail:                fail,
		}
		service := &entryDesiredMutationService{materials: resolver}
		snapshot := runtimeconfiguration.Snapshot{EnvironmentID: resolver.expectedEnvironment}
		snapshot.Files = []taskmaterialization.Record{
			{Destination: "unrelated.ini", OutputKind: taskmaterialization.OutputPlainFile,
				Source: taskmaterialization.Source{Kind: taskmaterialization.SourceEntryValue,
					EntryValue: &taskmaterialization.EntryValueReference{
						EntryID:           ids.New(ids.KindEnvEntry),
						ValueGenerationID: resolver.expectedGeneration,
					}}},
		}
		if fail {
			snapshot.Files = []taskmaterialization.Record{
				{Destination: ".env", OutputKind: taskmaterialization.OutputGeneratedEnvironment,
					Source: taskmaterialization.Source{Kind: taskmaterialization.SourceGeneratedEnvironment,
						GeneratedEnvironment: &taskmaterialization.GeneratedEnvironmentValueReference{
							Values: []taskmaterialization.GeneratedEnvironmentEntryReference{
								{Name: "REMOVE", Value: taskmaterialization.EntryValueReference{EntryID: removed}},
								{
									Name: "KEEP",
									Value: taskmaterialization.EntryValueReference{
										EntryID:           ids.New(ids.KindEnvEntry),
										ValueGenerationID: resolver.expectedGeneration,
									},
								}},
						}}},
			}
		}
		changes, err := service.removalSources(t.Context(), removed, snapshot, removalSourceAllocator(t))
		if fail {
			if !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) || changes != nil || resolver.calls != 1 ||
				!bytes.Equal(resolver.returned, make([]byte, len(resolver.returned))) {
				t.Fatal("partial cleanup render did not fail closed and clear plaintext")
			}
		} else if err != nil || len(changes) != 0 || resolver.calls != 0 {
			t.Fatal("unapplied Entry selected host cleanup or read file content")
		}
	}
}

func removalSourceAllocator(t *testing.T) *desiredrevision.BlueprintIdentityAllocator {
	t.Helper()
	at := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	allocator, err := desiredrevision.NewBlueprintIdentityAllocator(blueprints.EnvironmentBlueprintStageClaim{
		TaskID: ids.NewAt(ids.KindTask, at, 1), CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return allocator
}

type removalSourceResolver struct {
	expectedEnvironment, expectedGeneration string
	calls                                   int
	fail                                    bool
	returned                                []byte
}

func (resolver *removalSourceResolver) ResolveTaskMaterializationSource(
	_ context.Context,
	environmentID string,
	source taskmaterialization.Source,
) ([]byte, error) {
	resolver.calls++
	values := source.GeneratedEnvironment.Values
	if environmentID != resolver.expectedEnvironment || len(values) != 1 || values[0].Name != "KEEP" ||
		values[0].Value.ValueGenerationID != resolver.expectedGeneration {
		return nil, errs.New(errs.KindInternal, "cleanup selected a different applied value")
	}
	resolver.returned = []byte("KEEP=old\n")
	if resolver.fail {
		return resolver.returned, errs.New(errs.KindStorageUnavailable, "partial applied-value read")
	}
	return resolver.returned, nil
}

func (*removalSourceResolver) PinSecretValue(
	context.Context,
	string,
	string,
) (taskmaterialization.SecretValueReference, error) {
	return taskmaterialization.SecretValueReference{}, errs.New(errs.KindInternal, "cleanup must not pin a new Secret")
}
func (*removalSourceResolver) RetainComponentFile(context.Context, taskmaterialization.Record, uint64, []byte) error {
	return errs.New(errs.KindInternal, "cleanup must not capture a new Component file")
}
