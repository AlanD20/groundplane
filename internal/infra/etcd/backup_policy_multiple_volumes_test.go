package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	projection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	kv "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BAK-01: two Volume sources share Blueprint authority. Their policy must
// publish atomically, while changing that authority must still reject the save.
func TestBackupPolicyMultipleVolumesPublishAndFenceBlueprintChanges(t *testing.T) {
	for _, changed := range []bool{false, true} {
		name := "publish"
		if changed {
			name = "Blueprint changed"
		}
		t.Run(name, func(t *testing.T) {
			fixture := newBackupPolicyReplacementFixture(t, false)
			volumes := []projection.EnvironmentVolumeIdentity{
				{ID: ids.NewAt(ids.KindVolume, fixture.now, 3501), Slug: "first", Key: "first"},
				{ID: ids.NewAt(ids.KindVolume, fixture.now, 3502), Slug: "second", Key: "second"},
			}
			seedBackupPolicyVolumeProjection(
				t,
				fixture.store,
				fixture.environment,
				fixture.project,
				volumes[0],
				3503,
				volumes[1],
			)
			input := fixture.policyInput(true, "age", fixture.connector.Record.Connector.ID)
			for _, volume := range volumes {
				input.Sources = append(input.Sources, backuppolicy.BackupPolicySourceSelection{
					Kind: core.BackupSourceVolume, TargetID: volume.ID,
				})
			}
			prepared := fixture.prepared(t, input)
			defer prepared.Destroy()
			if changed {
				key := blueprints.EnvironmentBlueprintHeadKey(fixture.environment.Record.ID)
				value, err := idempotency.EncodeTaskReference(ids.NewAt(ids.KindTask, fixture.now, 3504))
				if err != nil {
					t.Fatal(err)
				}
				result, err := fixture.store.Transact(context.Background(), nil, []kv.Mutation{{
					Type: kv.MutationPut, Key: key, Value: value,
				}})
				if err != nil || !result.Succeeded {
					t.Fatalf("change Blueprint revision: %v", err)
				}
			}
			marker := backupPolicyReplacementMarker(fixture.environment.Record.ID, "backup-multiple-volumes-0001")
			result, err := fixture.repository.ReplaceBackupPolicyProtected(context.Background(), prepared, marker)
			if changed {
				if err != nil || result.kind != idempotencyTransactionConflict ||
					!isKind(result.conflict, errs.KindStateConflict) {
					t.Fatalf("stale Blueprint save: conflict=%v, err=%v", result.conflict, err)
				}
				if _, found, readErr := fixture.repository.GetBackupPolicy(context.Background(), fixture.environment.Record.ID); readErr != nil ||
					found {
					t.Fatalf("rejected save published policy: found=%v, err=%v", found, readErr)
				}
				return
			}
			if err != nil || result.kind != idempotencyTransactionApplied {
				t.Fatalf("save multiple Volumes: %v", err)
			}
			stored, err := fixture.repository.GetBackupPolicyProjection(
				context.Background(),
				fixture.environment.Record.ID,
			)
			if err != nil || len(stored.Sources) != len(input.Sources) {
				t.Fatalf("stored multiple-Volume policy: %v", err)
			}
			for index, source := range stored.Sources {
				if source.TargetID != input.Sources[index].TargetID || source.Kind != input.Sources[index].Kind {
					t.Fatalf("source %d differs from selected target", index)
				}
			}
		})
	}
}
