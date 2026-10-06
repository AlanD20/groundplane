package volume

import (
	"reflect"
	"testing"

	"github.com/AlanD20/groundplane/internal/core"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
)

func TestUpdateAuthoredBackupVolumeReferences(t *testing.T) {
	t.Parallel()
	current := projectionrecord.EnvironmentComposeProjection{Volumes: []projectionrecord.EnvironmentVolumeIdentity{
		{ID: "vol_01K7T8AFR0ABCDEF0123456789", Slug: "data", Key: "data-key"},
	}}
	policy := core.BackupSpec{
		Enabled: true, Frequency: "0 3 * * *", Keep: 9, Encryption: "age", Connector: "archive",
		Sources: []core.BackupSourceSpec{
			{Kind: core.BackupSourceVolume, Ref: "data"},
			{Kind: core.BackupSourceConfig},
		},
	}

	t.Run("rename preserves policy and other sources", func(t *testing.T) {
		candidate := policy
		candidate.Sources = append([]core.BackupSourceSpec(nil), policy.Sources...)
		input := core.BlueprintDesiredInput{Backup: &candidate}
		err := updateAuthoredBackupVolumeReferences(&input, current, volumeMutationRequest{
			action: volumeMutationActionEdit, volumeID: current.Volumes[0].ID,
			key: current.Volumes[0].Key, slug: "renamed",
		})
		if err != nil {
			t.Fatal(err)
		}
		want := policy
		want.Sources = []core.BackupSourceSpec{
			{Kind: core.BackupSourceVolume, Ref: "renamed"},
			{Kind: core.BackupSourceConfig},
		}
		if !reflect.DeepEqual(*input.Backup, want) {
			t.Fatalf("renamed Backup = %#v, want %#v", *input.Backup, want)
		}
	})

	t.Run("removal preserves remaining source and policy", func(t *testing.T) {
		candidate := policy
		candidate.Sources = append([]core.BackupSourceSpec(nil), policy.Sources...)
		input := core.BlueprintDesiredInput{Backup: &candidate}
		err := updateAuthoredBackupVolumeReferences(&input, current, volumeMutationRequest{
			action: volumeMutationActionRemove, volumeID: current.Volumes[0].ID, key: current.Volumes[0].Key,
		})
		if err != nil {
			t.Fatal(err)
		}
		want := policy
		want.Sources = []core.BackupSourceSpec{{Kind: core.BackupSourceConfig}}
		if !reflect.DeepEqual(*input.Backup, want) {
			t.Fatalf("remaining Backup = %#v, want %#v", *input.Backup, want)
		}
	})

	t.Run("last source removal disables configured policy", func(t *testing.T) {
		candidate := policy
		candidate.Sources = []core.BackupSourceSpec{{Kind: core.BackupSourceVolume, Ref: "data"}}
		input := core.BlueprintDesiredInput{Backup: &candidate}
		err := updateAuthoredBackupVolumeReferences(&input, current, volumeMutationRequest{
			action: volumeMutationActionRemove, volumeID: current.Volumes[0].ID, key: current.Volumes[0].Key,
		})
		if err != nil {
			t.Fatal(err)
		}
		want := policy
		want.Enabled = false
		want.Sources = []core.BackupSourceSpec{}
		if !reflect.DeepEqual(*input.Backup, want) {
			t.Fatalf("disabled Backup = %#v, want %#v", *input.Backup, want)
		}
	})
}
