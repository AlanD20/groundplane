package etcd

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPrepareEnvironmentComponentTaskReservesWithoutPublishing(t *testing.T) {
	// Rationale: rendering needs the exact Caddy address before Task creation,
	// but a failed Blueprint transaction must not leak that reservation.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	repository, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	project, environment := createEnvironmentBlueprintOwners(t, repository)
	now := environment.Record.CreatedAt.Add(time.Hour)
	zone, err := NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1401), Name: "frontend", Subnet: "10.40.10.0/29",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environment.Record.ID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord() error = %v", err)
	}
	previousTask := environmentBlueprintTestTask(t, project.Record, environment.Record, 1390)
	previousProjection := withTestEnvironmentComposeArtifact(EnvironmentComposeProjection{
		EnvironmentID:    environment.Record.ID,
		RevisionID:       previousTask.ID,
		RenderGeneration: 1,
		DesiredZones: []EnvironmentZoneProjection{{
			EnvironmentID: environment.Record.ID,
			Desired:       zone.Desired,
		}},
	})
	stageEnvironmentBlueprintForPublicationTest(
		t,
		repository,
		0,
		environmentBlueprintTestRevision(environment.Record.ID, previousTask, "services: {}\n"),
		previousProjection,
		environmentBlueprintTestMarker(previousTask, environment.Record.ID),
	)
	headValue, err := encodeTaskReference(previousTask.ID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	seedEnvironmentComponentCandidateValue(
		t, store, environmentBlueprintHeadKey(environment.Record.ID), headValue,
	)
	selected, found, err := repository.GetEnvironmentComposeProjection(ctx, environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("GetEnvironmentComposeProjection() = %#v, %v, %v", selected, found, err)
	}
	selectedZone, err := joinEnvironmentZone(selected, selected.Record.DesiredZones[0])
	if err != nil {
		t.Fatalf("joinEnvironmentZone() error = %v", err)
	}
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	component, err := NewComponentRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1402), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	current, err := components.CreateEnvironmentComponent(ctx, environment, project, component)
	if err != nil {
		t.Fatalf("CreateEnvironmentComponent() error = %v", err)
	}
	taskID := ids.NewAt(ids.KindTask, now, 1403)
	preparation, err := repository.PrepareEnvironmentComponentTask(
		ctx,
		taskID,
		environment.Record.ID,
		[]EnvironmentBlueprintZoneChange{{Current: &selectedZone, Record: zone}},
		[]EnvironmentComponentCandidateInput{{
			Current: current,
			Candidate: core.Component{
				ID: current.Record.Desired.ID, Owner: core.ComponentOwnerEnvironment,
				OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
				Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
					ZoneID: zone.Desired.ID,
				}},
				GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1404)},
			},
		}},
		now,
	)
	if err != nil {
		t.Fatalf("PrepareEnvironmentComponentTask() error = %v", err)
	}
	if preparation.Intent.TaskID != taskID || len(preparation.Intent.Candidates) != 1 ||
		preparation.Intent.Candidates[0].Candidate.Runtime.PinnedIPv4 != "10.40.10.6" ||
		len(preparation.addresses) != 1 || !preparation.addresses[0].Mutates ||
		preparation.addresses[0].Next.Reservations[current.Record.Desired.ID] != "10.40.10.6" {
		t.Fatalf("Component preparation = %#v", preparation)
	}
	if !reflect.DeepEqual(preparation.Intent.Candidates[0].Current, current.Record) ||
		preparation.Intent.Candidates[0].CurrentRevision != current.Revision {
		t.Fatalf("Component preparation changed its active base = %#v", preparation.Intent.Candidates[0])
	}
	for _, key := range []string{
		componentAddressRegistryKey(zone.Desired.ID),
		componentTaskIntentKey(taskID),
		componentTaskActiveEnvironmentKey(environment.Record.ID),
	} {
		result, getErr := store.Get(ctx, key)
		if getErr != nil || result.Entry != nil {
			t.Fatalf("preparation published %q = %#v, %v", key, result, getErr)
		}
	}

	seedEnvironmentComponentCandidateValue(
		t,
		store,
		componentTaskActiveEnvironmentKey(environment.Record.ID),
		[]byte(ids.NewAt(ids.KindTask, now, 1405)),
	)
	_, err = repository.PrepareEnvironmentComponentTask(
		ctx,
		ids.NewAt(ids.KindTask, now, 1406),
		environment.Record.ID,
		[]EnvironmentBlueprintZoneChange{{Current: &selectedZone, Record: zone}},
		[]EnvironmentComponentCandidateInput{{Current: current, Candidate: core.Component{
			ID: current.Record.Desired.ID, Owner: core.ComponentOwnerEnvironment,
			OwnerID: environment.Record.ID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
			Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
				ZoneID: zone.Desired.ID,
			}},
			GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1407)},
		}}},
		now,
	)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("PrepareEnvironmentComponentTask(active) error = %v", err)
	}
}

func seedEnvironmentComponentCandidateValue(
	t *testing.T,
	store *memoryHierarchyStore,
	key string,
	value []byte,
) {
	t.Helper()
	result, err := store.Transact(
		context.Background(),
		[]Condition{{Key: key}},
		[]Mutation{{Type: MutationPut, Key: key, Value: value}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed %q = %#v, %v", key, result, err)
	}
}
