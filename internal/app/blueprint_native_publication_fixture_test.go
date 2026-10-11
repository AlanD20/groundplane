package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testblueprintunits "github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	testcomponentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testscripts "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type nativeBlueprintRevisionFixture struct {
	Parent           etcd.TaskRecord
	Projection       testenvironmentprojection.EnvironmentComposeProjection
	Units            map[string]testblueprintunits.Unit
	Materializations []testtaskmaterialization.Record
}

type unexpectedBlueprintRollingObserver struct{ t *testing.T }

func (observer unexpectedBlueprintRollingObserver) ObserveServices(
	context.Context,
	string,
	[]*agentpb.ServiceObservationTarget,
) (*agentpb.ServiceObservationResult, error) {
	observer.t.Fatal("non-rolling Blueprint fixture unexpectedly observed physical replicas")
	return nil, nil
}

// PublishNativeBlueprintRevision publishes the visible authored parent, stages
// its exact complete runtime projection, claims the parent, and seals the
// desired Service-unit plan. Service work remains unpublished until the caller
// supplies a prepared private child to PublishNativeServiceUnit.
func (fixture *ExecutedArtifactFixture) PublishNativeBlueprintRevision(
	t *testing.T,
	parent etcd.TaskRecord,
	projection testenvironmentprojection.EnvironmentComposeProjection,
	scripts []testscripts.Record,
) nativeBlueprintRevisionFixture {
	t.Helper()
	ctx := t.Context()
	projection = testenvironmentprojection.CloneEnvironmentComposeProjection(projection)
	projection.RevisionID = parent.ID
	projection.RenderGeneration = uint64(parent.RenderGeneration)
	desired := publicationTestDesiredInput(projection)
	encodedDesired, err := testenvironmentprojection.EncodeEnvironmentDesiredInputStorage(desired)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(encodedDesired)
	clear(encodedDesired)
	materializations := append([]testtaskmaterialization.Record(nil), parent.Materializations...)
	parent.Actor = testtaskjournal.TaskActorOperator
	parent.Executor = testtaskjournal.TaskExecutorBlueprint
	parent.Type = testtaskjournal.TaskUpdate
	parent.Target = projection.EnvironmentID
	parent.PlanHash = hex.EncodeToString(digest[:])
	parent.Steps = nil
	parent.Materializations = nil
	parent.Configuration = nil
	parent.Params = map[string]string{testblueprints.EnvironmentDesiredRevisionParam: parent.ID}
	parent.Status = testtaskjournal.TaskStatusPending
	parent.NextEventSequence = 1

	owned, err := testenvironmentprojection.OwnedIdentitiesFromProjection(projection)
	if err != nil {
		t.Fatal(err)
	}
	owned.Scripts = make([]testenvironmentprojection.OwnedIdentity, len(scripts))
	for index, script := range scripts {
		name, nameErr := testscripts.BlueprintAuthoringKey(script)
		if nameErr != nil {
			t.Fatal(nameErr)
		}
		owned.Scripts[index] = testenvironmentprojection.OwnedIdentity{
			ID: script.Desired.ID, Name: name, BirthRevisionID: parent.ID,
		}
	}
	slices.SortFunc(owned.Scripts, func(left, right testenvironmentprojection.OwnedIdentity) int {
		return strings.Compare(left.Name, right.Name)
	})
	previous, found, err := fixture.Hierarchy.GetEnvironmentOwnedIdentities(ctx, projection.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	if found {
		owned, err = testenvironmentprojection.RetainOwnedIdentityBirths(owned, previous.Record)
		if err != nil {
			t.Fatal(err)
		}
	}
	marker := environmentBlueprintTestMarker(parent, projection.EnvironmentID)
	claim := stageEnvironmentBlueprintWithDependencyDigestForPublicationTest(
		t,
		fixture.store,
		fixture.head,
		environmentBlueprintTestRevision(projection.EnvironmentID, parent, string(projection.NormalizedCompose)),
		projection,
		marker,
		digest,
	)
	prerequisites, err := etcd.NewBlueprintPrerequisiteAuthority(
		projection.EnvironmentID,
		parent.ID,
		projection.BlueprintRequirements,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	repository, err := etcd.NewEnvironmentBlueprintRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repository.PublishEnvironmentBlueprintAuthoredRevision(ctx, etcd.BlueprintAuthoredPublication{
		Project: fixture.Project, Environment: fixture.Environment,
		ExpectedHeadRevision: fixture.head, Claim: claim,
		DesiredInput: desired, OwnedIdentities: owned,
		PrerequisiteAuthority: prerequisites,
		Parent:                parent, Marker: marker, ScriptPublication: fixture.hookScripts,
	})
	fixture.hookScripts.Clear()
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != etcd.IdempotencyKnownApplied {
		t.Fatalf("authored parent publication outcome=%v conflict=%v err=%v", outcome, conflict, classifyErr)
	}
	fixture.head = result.Revision()
	value, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Put(
		ctx,
		testblueprints.EnvironmentBlueprintEffectiveProjectionKey(projection.EnvironmentID, parent.ID),
		value,
	); err != nil {
		clear(value)
		t.Fatal(err)
	}
	clear(value)
	claimed, found, err := fixture.Tasks.ClaimNextBlueprintParent(ctx, parent.CreatedAt.Add(time.Second))
	if err != nil || !found || claimed.Record.ID != parent.ID {
		t.Fatalf("claim authored parent = %#v, %t, %v", claimed, found, err)
	}
	units := make(map[string]testblueprintunits.Unit, len(projection.DesiredServices))
	ordered := make([]testblueprintunits.Unit, 0, len(projection.DesiredServices))
	for _, service := range projection.DesiredServices {
		target := testblueprintunits.ResourceKey{Kind: ids.KindService, ID: service.Desired.ID}
		fingerprint := sha256.Sum256([]byte(parent.ID + "\x00" + service.Desired.ID))
		unit := testblueprintunits.Unit{
			Target: target, Fingerprint: hex.EncodeToString(fingerprint[:]), Writes: []testblueprintunits.ResourceKey{target},
		}
		units[service.Desired.ID] = unit
		ordered = append(ordered, unit)
	}
	slices.SortFunc(ordered, func(left, right testblueprintunits.Unit) int {
		return strings.Compare(left.Target.ID, right.Target.ID)
	})
	if _, err := fixture.Tasks.PublishBlueprintDesiredPlan(ctx, parent.ID, testblueprintunits.DesiredPlan{
		EnvironmentID: projection.EnvironmentID, ParentTaskID: parent.ID, Complete: true, Units: ordered,
	}); err != nil {
		t.Fatal(err)
	}
	return nativeBlueprintRevisionFixture{
		Parent: claimed.Record, Projection: projection, Units: units, Materializations: materializations,
	}
}

func (fixture *ExecutedArtifactFixture) PrepareNativeServiceUnit(
	t *testing.T,
	producer *blueprintrelease.Service,
	revision nativeBlueprintRevisionFixture,
	serviceID string,
	scripts []testscripts.Record,
	prefix []*agentpb.ExecutionStep,
) blueprintrelease.Prepared {
	t.Helper()
	createdAt := revision.Parent.CreatedAt.Add(2 * time.Second)
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation),
		Owner: revision.Parent.Owner, Actor: testtaskjournal.TaskActorSystem,
		Executor: testtaskjournal.TaskExecutorAgent, PlanID: ids.New(ids.KindPlan),
		RenderGeneration: revision.Parent.RenderGeneration, Type: testtaskjournal.TaskUpdate,
		Target: revision.Parent.Owner.EnvironmentID,
		Params: map[string]string{
			testblueprints.EnvironmentDesiredRevisionParam:      revision.Parent.ID,
			testtaskjournal.TaskBlueprintParentParam:            revision.Parent.ID,
			testtaskjournal.TaskMaterializationEnvironmentParam: revision.Parent.Owner.EnvironmentID,
			taskcontract.EnvironmentBlueprintProcedureParam:     string(taskcontract.BlueprintComposeProcedureNone),
		},
		TimeoutSeconds: revision.Parent.TimeoutSeconds - 2,
		Materializations: append(
			[]testtaskmaterialization.Record(nil),
			revision.Materializations...,
		),
		Status: testtaskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(revision.Projection.ComposeArtifact, artifact); err != nil {
		t.Fatal(err)
	}
	child.Params[testtaskjournal.TaskComposeArtifactParam] = artifact.GetArtifactId()
	prepared, err := producer.PrepareServiceUnit(t.Context(), blueprintrelease.PrepareServiceUnitInput{
		ServiceID: serviceID, Projection: revision.Projection, Scripts: scripts,
		Task: child, PrefixSteps: prefix, CreatedAt: createdAt,
		AllocateNamed: func(kind ids.Kind, purpose string) string {
			return ids.DeriveAt(kind, createdAt, child.ID, purpose)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return prepared
}

func (fixture *ExecutedArtifactFixture) PublishNativeServiceUnit(
	t *testing.T,
	revision nativeBlueprintRevisionFixture,
	serviceID string,
	prepared blueprintrelease.Prepared,
) {
	t.Helper()
	unit, found := revision.Units[serviceID]
	if !found {
		t.Fatal("native Service unit is absent")
	}
	if _, err := fixture.Tasks.PublishBlueprintChild(
		t.Context(), revision.Parent.ID, prepared.Task, unit, prepared.Publication,
		testcomponentplanning.Publication{}, etcd.BlueprintEntryRemovalPublication{},
		etcd.BlueprintServiceRemovalPublication{}, etcd.BlueprintZoneRemovalPublication{}, nil,
	); err != nil {
		t.Fatal(err)
	}
}
