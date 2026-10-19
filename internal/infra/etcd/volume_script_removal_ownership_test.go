package etcd

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TASK-158: a retained Script snapshot cannot reserve a Volume after removal
// owns it, including before Script execution or its own Task publication.
func TestVolumeRemovalOwnershipExcludesScriptPreparation(t *testing.T) {
	for _, removing := range []bool{false, true} {
		t.Run(map[bool]string{false: "unowned control", true: "removal owns Volume"}[removing], func(t *testing.T) {
			store, operationID, members, _, _, _ := scriptRunnerSnapshotSourceFixture(t)
			member := members[2]
			if removing {
				owner := removalrecord.Owner{
					VolumeID:      member.Reference.Source.VolumeID,
					EnvironmentID: member.Reference.SourceOwnerID,
					OperationID:   ids.New(ids.KindOperation),
				}
				value, err := removalrecord.EncodeOwner(owner)
				if err != nil {
					t.Fatal(err)
				}
				defer clear(value)
				result, err := store.Transact(
					t.Context(),
					[]testkeyvalue.Condition{{Key: removalrecord.OwnerKey(owner.VolumeID)}},
					[]testkeyvalue.Mutation{
						{Type: testkeyvalue.MutationPut, Key: removalrecord.OwnerKey(owner.VolumeID), Value: value},
					},
				)
				if err != nil || !result.Succeeded {
					t.Fatalf("seed removal owner: %v", err)
				}
			}
			authority, err := testscriptsourcepublication.NewAuthority(store)
			if err != nil {
				t.Fatal(err)
			}
			_, err = authority.Prepare(
				t.Context(),
				operationID,
				[]testscriptsourceevidence.ScriptSourcePreparationMember{member},
			)
			if !removing {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if !isKind(err, errs.KindStateConflict) {
				t.Fatalf("removed Volume reserved by Script: %v", err)
			}
			if store.valueAt(
				testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source),
				store.revision,
			) != nil {
				t.Fatal("rejected Script acquisition installed a source count")
			}
		})
	}
}

type volumeSourceOwnerRaceStore struct {
	hierarchyStore
	owner    removalrecord.Owner
	injected bool
}

func (store *volumeSourceOwnerRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	for _, mutation := range mutations {
		if !store.injected &&
			mutation.Key == testscriptsourceevidence.ScriptSourceCountKey(volumeScriptSource(store.owner.VolumeID)) {
			value, err := removalrecord.EncodeOwner(store.owner)
			if err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			defer clear(value)
			guards := append(
				volumeScriptAbsenceConditions(
					store.owner.VolumeID,
				),
				testkeyvalue.Condition{Key: removalrecord.OwnerKey(store.owner.VolumeID)},
			)
			result, err := store.hierarchyStore.Transact(
				ctx,
				guards,
				[]testkeyvalue.Mutation{
					{Type: testkeyvalue.MutationPut, Key: removalrecord.OwnerKey(store.owner.VolumeID), Value: value},
				},
			)
			if err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			if !result.Succeeded {
				return testkeyvalue.TransactionResult{}, errs.New(
					errs.KindInternal,
					"fixture owner race did not commit",
				)
			}
			store.injected = true
			break
		}
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

// Rationale: an owner acquired after the source read must still defeat the
// source-count transaction. Testing only a pre-existing owner misses this race.
func TestVolumeRemovalInitialOwnershipWinsSourceAcquisitionRace(t *testing.T) {
	store, operationID, members, _, _, _ := scriptRunnerSnapshotSourceFixture(t)
	member := members[2]
	transactions := &volumeSourceOwnerRaceStore{hierarchyStore: store, owner: removalrecord.Owner{
		VolumeID: member.Reference.Source.VolumeID, EnvironmentID: member.Reference.SourceOwnerID,
		OperationID: ids.New(ids.KindOperation),
	}}
	authority, err := testscriptsourcepublication.NewAuthority(transactions)
	if err != nil {
		t.Fatal(err)
	}
	_, err = authority.Prepare(
		context.Background(),
		operationID,
		[]testscriptsourceevidence.ScriptSourcePreparationMember{member},
	)
	if !transactions.injected || !isKind(err, errs.KindStateConflict) {
		t.Fatalf("late owner did not exclude source acquisition: %v", err)
	}
	if store.valueAt(testscriptsourceevidence.ScriptSourceCountKey(member.Reference.Source), store.revision) != nil ||
		store.valueAt(
			testscriptsourceevidence.ScriptSourceForwardReferenceKey(member.Reference),
			store.revision,
		) != nil {
		t.Fatal("losing source acquisition installed a count or membership")
	}
}

// Rationale: the owner lookup is a bounded, typed operation reference, not an
// unvalidated alternate runtime record or caller-controlled path.
func TestVolumeRemovalInitialOwnerCodecRejectsCorruption(t *testing.T) {
	owner := removalrecord.Owner{
		VolumeID:      ids.New(ids.KindVolume),
		EnvironmentID: ids.New(ids.KindEnvironment),
		OperationID:   ids.New(ids.KindOperation),
	}
	value, err := removalrecord.EncodeOwner(owner)
	if err != nil {
		t.Fatal(err)
	}
	if len(value) > 1024 || len(removalrecord.OwnerKey(owner.VolumeID)) > 512 {
		t.Fatal("owner record or key exceeds its bound")
	}
	decoded, err := removalrecord.DecodeOwner(value)
	if err != nil || decoded != owner {
		t.Fatalf("owner round trip: %v", err)
	}
	for _, invalid := range [][]byte{nil, value[:len(value)-1], append(append([]byte(nil), value...), 0), make([]byte, 1025)} {
		if _, err := removalrecord.DecodeOwner(invalid); !isKind(err, errs.KindInternal) {
			t.Fatalf("malformed owner accepted: %v", err)
		}
	}
	for _, invalid := range []removalrecord.Owner{
		{EnvironmentID: owner.EnvironmentID, OperationID: owner.OperationID},
		{VolumeID: owner.VolumeID, OperationID: owner.OperationID},
		{VolumeID: owner.VolumeID, EnvironmentID: owner.EnvironmentID},
	} {
		if _, err := removalrecord.EncodeOwner(invalid); !isKind(err, errs.KindValidationFailed) {
			t.Fatalf("invalid owner identity accepted: %v", err)
		}
	}
}

// Rationale: the publisher consumes the existing runtime record contract and
