package app

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	"github.com/AlanD20/groundplane/pkg/errs"
	"google.golang.org/protobuf/proto"
)

// TASK-53: platform fallback is valid only when it was captured. A project
// override keeps priority even if a platform value appears after capture.
func TestNativeBackupCredentialSelectionKeepsCapturedScope(t *testing.T) {
	t.Run("platform fallback", func(t *testing.T) {
		f := newNativeBackupCredentialFixture(t, core.SecretScopePlatform)
		f.resolveAndCheck(t)
	})
	t.Run("project override", func(t *testing.T) {
		f := newNativeBackupCredentialFixture(t, core.SecretScopeProject)
		addNativeBackupPlatformFallback(t, f)
		f.resolveAndCheck(t)
	})
}

// TASK-53: the current channel does not authorize a different captured input,
// changed Connector/envelope or a fallback that replaces a pinned project value.
func TestNativeBackupCredentialSelectionRejectsChangedSources(t *testing.T) {
	for _, change := range []string{"Connector revision", "direct envelope", "input digest", "attempt number", "generation", "project deletion", "project key removed"} {
		t.Run(change, func(t *testing.T) {
			f := newNativeBackupCredentialFixture(t, core.SecretScopeProject)
			claimed := proto.CloneOf(f.attempt)
			switch change {
			case "Connector revision":
				f.rewrite(t, f.connectorKey)
			case "direct envelope":
				f.rewrite(t, f.directKey)
			case "input digest":
				claimed.Input.Sha256[0] ^= 1
			case "attempt number":
				claimed.AttemptNumber++
			case "generation":
				claimed.ExecutorGeneration++
			case "project deletion":
				addNativeBackupPlatformFallback(t, f)
				current, err := f.store.Get(t.Context(), secrets.RecordKey(f.secretRecord.Secret.ID))
				if err != nil || current.Entry == nil {
					t.Fatalf("selected Secret record unavailable: %v", err)
				}
				at := time.Now().UTC()
				tombstone := deletions.DeletionTombstoneRecord{TargetKind: deletions.DeletionTargetSecret,
					TargetID: f.secretRecord.Secret.ID, TargetRevision: current.Entry.ModRevision,
					TaskID: ids.New(
						ids.KindTask,
					), Phase: deletions.DeletionPhaseFinalizing, CreatedAt: at, UpdatedAt: at}
				nativeBackupPut(
					t,
					f.store,
					deletions.TombstoneKey(string(deletions.DeletionTargetSecret), f.secretRecord.Secret.ID),
					tombstone,
					deletions.EncodeDeletionTombstone,
				)
			case "project key removed":
				addNativeBackupPlatformFallback(t, f)
				if _, err := f.store.Delete(t.Context(), secrets.SecretScopedKey(f.secretRecord.Secret)); err != nil {
					t.Fatal(err)
				}
			}
			input, slots, err := f.delivery.ResolveWorkSecretSlots(t.Context(), f.authorization, claimed, 0)
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || input != nil || slots != nil ||
				len(f.crypt.opened) != 0 {
				t.Fatalf("changed source opened credentials: input=%t slots=%d opens=%d error=%v",
					input != nil, len(slots), len(f.crypt.opened), err)
			}
		})
	}
}

func addNativeBackupPlatformFallback(t *testing.T, f *nativeBackupCredentialFixture) {
	t.Helper()
	record, err := secrets.NewPlatformRecord(
		ids.New(ids.KindSecret),
		"BACKUP_SECRET",
		core.SecretKindEnvVar,
		"",
		time.Now().UTC(),
	)
	if err != nil {
		t.Fatal(err)
	}
	nativeBackupPut(t, f.store, secrets.RecordKey(record.Secret.ID), record, secrets.EncodeRecord)
	if _, err := f.store.Put(t.Context(), secrets.SecretScopedKey(record.Secret), []byte(record.Secret.ID)); err != nil {
		t.Fatal(err)
	}
	read, err := f.store.Get(t.Context(), f.secretKey)
	if err != nil || read.Entry == nil {
		t.Fatalf("selected Secret value unavailable: %v", err)
	}
	value, err := secrets.DecodeEncryptedValue(read.Entry.Value)
	if err != nil {
		t.Fatal(err)
	}
	value.SecretID = record.Secret.ID
	clear(value.Ciphertext)
	value.Ciphertext = []byte("native-platform-envelope")
	defer clear(value.Ciphertext)
	digest := sha256.Sum256(value.Ciphertext)
	value.CiphertextSHA256 = hex.EncodeToString(digest[:])
	nativeBackupPut(t, f.store, secrets.ValueKey(record.Secret.ID), value, secrets.EncodeEncryptedValue)
}
