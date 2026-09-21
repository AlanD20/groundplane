package entry

import (
	"testing"

	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
)

// Rationale: the etcd adapter must map every capability-owned closed output
// and storage variant explicitly, while unknown variants fail closed.
func TestPersistedRemovalMaterializationConvertsClosedKinds(t *testing.T) {
	t.Parallel()
	outputs := []struct {
		input RemovalOutputKind
		want  testtaskmaterialization.OutputKind
	}{
		{input: RemovalOutputGeneratedEnvironment, want: testtaskmaterialization.OutputGeneratedEnvironment},
		{input: RemovalOutputPlainFile, want: testtaskmaterialization.OutputPlainFile},
		{input: RemovalOutputSecretFile, want: testtaskmaterialization.OutputSecretFile},
		{input: RemovalOutputRemoveGeneratedEnv, want: testtaskmaterialization.OutputRemoveGeneratedEnv},
		{input: RemovalOutputRemovePlainFile, want: testtaskmaterialization.OutputRemovePlainFile},
		{input: RemovalOutputRemoveSecretFile, want: testtaskmaterialization.OutputRemoveSecretFile},
	}
	for _, test := range outputs {
		converted, err := persistedRemovalMaterialization(RemovalMaterialization{
			OutputKind: test.input, Source: RemovalSource{Kind: RemovalSourceRemoval},
		})
		if err != nil || converted.OutputKind != test.want {
			t.Fatalf("persistedRemovalMaterialization(%q) = %q, %v", test.input, converted.OutputKind, err)
		}
	}
	for _, storage := range []struct {
		input RemovalValueStorage
		want  testtaskmaterialization.EntryValueStorage
	}{
		{input: RemovalValueStoragePlain, want: testtaskmaterialization.EntryValueStoragePlain},
		{input: RemovalValueStorageSecret, want: testtaskmaterialization.EntryValueStorageSecret},
	} {
		converted, err := persistedRemovalMaterialization(generatedRemovalMaterialization(storage.input))
		if err != nil || converted.Source.GeneratedEnvironment.Values[0].Value.Storage != storage.want {
			t.Fatalf("persistedRemovalMaterialization(storage %q) = %#v, %v", storage.input, converted, err)
		}
	}
	if _, err := persistedRemovalMaterialization(RemovalMaterialization{
		OutputKind: "future", Source: RemovalSource{Kind: RemovalSourceRemoval},
	}); err == nil {
		t.Fatal("persistedRemovalMaterialization(invalid output) error = nil")
	}
	if _, err := persistedRemovalMaterialization(generatedRemovalMaterialization("future")); err == nil {
		t.Fatal("persistedRemovalMaterialization(invalid storage) error = nil")
	}
}

func generatedRemovalMaterialization(storage RemovalValueStorage) RemovalMaterialization {
	return RemovalMaterialization{
		OutputKind: RemovalOutputGeneratedEnvironment,
		Source: RemovalSource{
			Kind: RemovalSourceGeneratedEnvironment,
			GeneratedEnvironment: &RemovalGeneratedEnvironment{
				Values: []RemovalGeneratedValue{{Storage: storage}},
			},
		},
	}
}
