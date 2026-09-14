package etcd

import (
	"context"
	"fmt"
	"net/netip"
	"testing"
	"time"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type environmentBlueprintAtomicPublication struct {
	store                      *memoryHierarchyStore
	audit                      *environmentBlueprintAtomicAuditStore
	result                     IdempotencyTransactionResult
	environmentID              string
	task                       TaskRecord
	markerKey                  string
	releasePublicationID       string
	activeScriptRevision       int64
	candidateAttachIDs         []string
	newBackupSourceIDs         []string
	newBackupSources           []EnvironmentBlueprintBackupPolicySourceInput
	volumeIDs                  []string
	retainedAttachRevisions    []Versioned[AttachRecord]
	retainedGrantRelations     [][2]string
	preexistingBackupRevisions map[string]int64
	detachWinner               Versioned[AttachRecord]
}

func (published environmentBlueprintAtomicPublication) candidateAttachKeys() []string {
	keys := make([]string, len(published.candidateAttachIDs))
	for index, id := range published.candidateAttachIDs {
		keys[index] = attachKey(id)
	}
	return keys
}

type environmentBlueprintAtomicAuditStore struct {
	hierarchyStore
	failFinal        bool
	comparisons      int
	successMutations int
	failureReads     int
	conditionByKey   map[string]Condition
}

// Only final publication is faulted; private source staging must remain usable.
func (store *environmentBlueprintAtomicAuditStore) TransactEnvironmentBlueprint(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.comparisons = len(conditions)
	store.successMutations = len(mutations)
	store.failureReads = len(conditions)
	store.conditionByKey = make(map[string]Condition, len(conditions))
	for _, condition := range conditions {
		store.conditionByKey[condition.Key] = condition
	}
	if store.failFinal {
		return TransactionResult{}, errs.New(errs.KindInternal, "injected final Blueprint publication failure")
	}
	return store.hierarchyStore.Transact(ctx, conditions, mutations)
}

type environmentBlueprintBackingScope struct {
	project     Versioned[ProjectRecord]
	environment Versioned[EnvironmentRecord]
	service     Versioned[ServiceRecord]
}

func publishEnvironmentBlueprintAtomicShape(
	t *testing.T,
	shape environmentBlueprintAtomicShape,
	failFinal bool,
) (environmentBlueprintAtomicPublication, error) {
	t.Helper()
	ctx := context.Background()
	fixture := newBackupPolicyReplacementFixture(t, false)
	hierarchy, err := newHierarchyRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	task := environmentBlueprintTestTask(t, fixture.project.Record, fixture.environment.Record, 6100)
	projection := environmentBlueprintTestProjection(fixture.environment.Record.ID, task, 1)
	projection.DesiredZones[0].Desired.Subnet = "10.240.0.0/27"
	if shape.realHookSources {
		// The terminal journey must publish the same Service that its Release executes.
		serviceID := ids.NewAt(ids.KindService, task.CreatedAt, 7600)
		projection.DesiredServices[0].Desired.ID = serviceID
		projection.DesiredRoutes[0].Desired.TargetServiceID = serviceID
	}

	var attachPreparation BlueprintAttachTaskPreparation
	var candidateAttachIDs []string
	var retainedAttachRevisions []Versioned[AttachRecord]
	var retainedGrantRelations [][2]string
	if shape.attaches {
		scopes := []environmentBlueprintBackingScope{
			seedEnvironmentBlueprintBackingScope(t, fixture, 6200),
			seedEnvironmentBlueprintBackingScope(t, fixture, 6300),
		}
		secondServiceID := ids.NewAt(ids.KindService, fixture.now, 6400)
		projection.DesiredServices = append(projection.DesiredServices, EnvironmentServiceProjection{
			EnvironmentID: fixture.environment.Record.ID,
			Desired: core.Service{
				ID: secondServiceID, Name: "worker", Image: "example/worker:1",
				Zones: []string{"default"},
			},
		})
		consumerIDs := []string{projection.DesiredServices[0].Desired.ID, secondServiceID}
		inputs := make([]EnvironmentBlueprintAttachCandidateInput, 2)
		for index := range inputs {
			record, facts, retained := environmentBlueprintMaximumAttachCandidate(
				t, fixture, task, scopes[index], consumerIDs[index], int64(6500+index*100),
			)
			inputs[index] = EnvironmentBlueprintAttachCandidateInput{
				Record: record, Facts: &facts,
				BackingProject:       scopes[index].project,
				BackingEnvironment:   scopes[index].environment,
				BackingService:       scopes[index].service,
				RetainedGrantTargets: retained,
			}
			candidateAttachIDs = append(candidateAttachIDs, record.ID)
			retainedAttachRevisions = append(retainedAttachRevisions, retained...)
			for _, target := range retained {
				retainedGrantRelations = append(retainedGrantRelations, [2]string{target.Record.ID, record.ID})
			}
		}
		attachReadRevision := fixture.store.revision
		for inputIndex := range inputs {
			for retainedIndex := range inputs[inputIndex].RetainedGrantTargets {
				inputs[inputIndex].RetainedGrantTargets[retainedIndex].ReadRevision = attachReadRevision
			}
		}
		for index := range retainedAttachRevisions {
			retainedAttachRevisions[index].ReadRevision = attachReadRevision
		}
		attachPreparation, err = PrepareEnvironmentBlueprintAttachTask(
			task.ID, fixture.environment.Record.ID, inputs, false, task.CreatedAt,
		)
		if err != nil {
			t.Fatalf("PrepareEnvironmentBlueprintAttachTask() error = %v", err)
		}
	}

	var currentAttachIDs []string
	if shape.backup {
		scope := seedEnvironmentBlueprintBackingScope(t, fixture, 6700)
		for index := 0; index < 4; index++ {
			record := environmentBlueprintExistingBackupAttach(
				t, fixture, scope, projection.DesiredServices[0].Desired.ID, int64(6800+index),
			)
			currentAttachIDs = append(currentAttachIDs, record.ID)
		}
		seedEnvironmentBlueprintCurrentBackupPolicy(t, fixture, currentAttachIDs)
		projection.Volumes = nil
		for index := 0; index < 6; index++ {
			projection.Volumes = append(projection.Volumes, EnvironmentVolumeIdentity{
				ID:   ids.NewAt(ids.KindVolume, fixture.now, int64(6900+index)),
				Slug: fmt.Sprintf("volume-%02d", index),
				Key:  fmt.Sprintf("volume-%02d", index),
			})
		}
	}
	projection = withTestEnvironmentComposeArtifact(projection)

	ensureEnvironmentBlueprintActiveScriptSet(t, fixture)
	fixedRevision := fixture.store.revision
	releaseSourceMembers := environmentBlueprintStagedReleaseSources(
		t, fixture, task, projection, fixedRevision, shape.physicalSources,
	)

	releaseGroupPreparation, err := hierarchy.PrepareReleaseGroupBlueprintMutation(
		ctx, fixture.environment.Record.ID, fixedRevision, nil, nil,
	)
	if err != nil {
		t.Fatalf("PrepareReleaseGroupBlueprintMutation() error = %v", err)
	}

	var scriptPublication BlueprintScriptPublication
	activeScriptRevision := int64(0)
	if shape.releases != 0 {
		active, getErr := fixture.store.Get(ctx, scriptSetActiveKey(fixture.environment.Record.ID))
		if getErr != nil || active.Entry == nil {
			t.Fatalf("read active Script set = %#v, %v", active, getErr)
		}
		activeScriptRevision = active.Entry.ModRevision
		scripts, repositoryErr := newScriptRepository(fixture.store)
		if repositoryErr != nil {
			t.Fatal(repositoryErr)
		}
		var desiredScripts []ScriptRecord
		if shape.realHookSources {
			desiredScripts = []ScriptRecord{blueprintTerminalFixtureScript(t, task)}
		}
		scriptPublication, err = scripts.PrepareBlueprintScriptPublication(
			ctx,
			fixture.environment.Record.ID,
			fixedRevision,
			task.ID,
			nil,
			desiredScripts,
			scriptBlueprintGenerations(desiredScripts),
		)
		if err != nil {
			t.Fatalf("PrepareBlueprintScriptPublication() error = %v", err)
		}
		defer scriptPublication.Clear()
	}

	var backupPreparation BlueprintBackupPolicyPreparation
	var newBackupSourceIDs []string
	var newBackupSources []EnvironmentBlueprintBackupPolicySourceInput
	preexistingBackupRevisions := map[string]int64{}
	if shape.backup {
		sourceInputs := make([]EnvironmentBlueprintBackupPolicySourceInput, 0, MaximumBackupPolicySources)
		for index, attachID := range currentAttachIDs {
			sourceInputs = append(sourceInputs, EnvironmentBlueprintBackupPolicySourceInput{
				CandidateID: ids.NewAt(ids.KindBackupSource, fixture.now, int64(7000+index)),
				Kind:        core.BackupSourceAttach, TargetID: attachID,
			})
		}
		for index, attachID := range candidateAttachIDs {
			sourceID := ids.NewAt(ids.KindBackupSource, fixture.now, int64(7100+index))
			sourceInputs = append(sourceInputs, EnvironmentBlueprintBackupPolicySourceInput{
				CandidateID: sourceID, Kind: core.BackupSourceAttach, TargetID: attachID,
			})
			newBackupSourceIDs = append(newBackupSourceIDs, sourceID)
			newBackupSources = append(newBackupSources, sourceInputs[len(sourceInputs)-1])
		}
		for index, volume := range projection.Volumes {
			sourceID := ids.NewAt(ids.KindBackupSource, fixture.now, int64(7200+index))
			sourceInputs = append(sourceInputs, EnvironmentBlueprintBackupPolicySourceInput{
				CandidateID: sourceID, Kind: core.BackupSourceVolume, TargetID: volume.ID,
			})
			newBackupSourceIDs = append(newBackupSourceIDs, sourceID)
			newBackupSources = append(newBackupSources, sourceInputs[len(sourceInputs)-1])
		}
		backupPreparation, err = fixture.repository.PrepareEnvironmentBlueprintBackupPolicy(
			ctx,
			EnvironmentBlueprintBackupPolicyInput{
				EnvironmentID:     fixture.environment.Record.ID,
				TaskID:            task.ID,
				ReadRevision:      fixedRevision,
				Enabled:           true,
				Frequency:         "*-*-* 04:00:00",
				Keep:              12,
				Encryption:        "age",
				ConnectorName:     fixture.connector.Record.Connector.Name,
				Sources:           sourceInputs,
				Projection:        projection,
				AttachPreparation: attachPreparation,
				CreatedAt:         task.CreatedAt,
			},
		)
		if err != nil {
			t.Fatalf("PrepareEnvironmentBlueprintBackupPolicy() error = %v", err)
		}
		defer backupPreparation.Clear()
		identity, identityErr := age.GenerateX25519Identity()
		if identityErr != nil {
			t.Fatal(identityErr)
		}
		if err = backupPreparation.SupplyInitialKey(BackupPolicyInitialKeyMaterial{
			Recipient:  identity.Recipient().String(),
			Ciphertext: []byte("controller-sealed-shape-age-identity"),
		}); err != nil {
			t.Fatalf("SupplyInitialKey() error = %v", err)
		}
		projection.Backup = backupPreparation.Projection()
		for _, key := range []string{
			backupPolicyKey(fixture.environment.Record.ID),
			environmentCoordinationKey(fixture.environment.Record.ID),
			backupPolicyConnectorReferenceKey(fixture.connector.Record.Connector.ID, fixture.environment.Record.ID),
		} {
			read, getErr := fixture.store.Get(context.Background(), key)
			if getErr != nil || read.Entry == nil {
				t.Fatalf("pre-existing Backup authority %q = %#v, %v", key, read, getErr)
			}
			preexistingBackupRevisions[key] = read.Entry.ModRevision
		}
	}

	var releasePublication BlueprintReleasePublication
	releasePublicationID := ""
	if shape.releases != 0 {
		releasePublication, releasePublicationID = prepareEnvironmentBlueprintReleaseShape(
			t,
			fixture.store,
			&task,
			fixture.environment.Record.ID,
			projection,
			shape,
			releaseSourceMembers,
		)
		defer releasePublication.Clear()
	}
	if shape.scriptEditAfterPreparation {
		first := scriptCheckpointTestRecord(task.CreatedAt.Add(time.Millisecond))
		key := scriptSetScriptKey(fixture.environment.Record.ID, task.ID, first.ScriptID)
		read, err := fixture.store.Get(ctx, key)
		if err != nil || read.Entry == nil {
			t.Fatalf("read selected Script primary: %v", err)
		}
		script, err := decodeScriptRecord(read.Entry.Value)
		if err != nil {
			t.Fatal(err)
		}
		script.Desired.Body = "exit 0"
		script.Desired.Order++
		writeScriptContextPrimaryFixture(t, fixture.store, script)
	}

	marker := environmentBlueprintTestMarker(task, fixture.environment.Record.ID)
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatal(err)
	}
	claim := stageEnvironmentBlueprintForPublicationTest(
		t,
		hierarchy,
		0,
		environmentBlueprintTestRevision(fixture.environment.Record.ID, task, "services: {}\n"),
		projection,
		marker,
	)
	var detachWinner Versioned[AttachRecord]
	if shape.detachRetainedBeforeFinal {
		detaching, detachErr := BeginAttachDetaching(
			retainedAttachRevisions[0].Record,
			ids.NewAt(ids.KindTask, fixture.now, 8800),
		)
		if detachErr != nil {
			t.Fatalf("BeginAttachDetaching(retained target) error = %v", detachErr)
		}
		value, encodeErr := encodeAttachRecord(detaching)
		if encodeErr != nil {
			t.Fatalf("encodeAttachRecord(detaching target) error = %v", encodeErr)
		}
		detachResult, detachErr := fixture.store.Transact(
			ctx,
			[]Condition{
				{Key: attachKey(detaching.ID), ModRevision: retainedAttachRevisions[0].Revision},
				{Key: deletionTombstoneKey("attach", detaching.ID)},
			},
			[]Mutation{{Type: MutationPut, Key: attachKey(detaching.ID), Value: value}},
		)
		clear(value)
		if detachErr != nil || !detachResult.Succeeded {
			t.Fatalf("commit retained target detach = %#v, %v", detachResult, detachErr)
		}
		detachWinner = Versioned[AttachRecord]{
			Record: detaching, Revision: detachResult.Revision, ReadRevision: detachResult.Revision,
		}
	}
	if shape.tombstoneRetainedBeforeFinal {
		key := deletionTombstoneKey("attach", retainedAttachRevisions[0].Record.ID)
		result, tombstoneErr := fixture.store.Transact(
			ctx,
			[]Condition{{Key: key}},
			[]Mutation{{Type: MutationPut, Key: key, Value: []byte("deleting")}},
		)
		if tombstoneErr != nil || !result.Succeeded {
			t.Fatalf("commit retained target deletion tombstone = %#v, %v", result, tombstoneErr)
		}
	}
	zones, services, routes := environmentBlueprintTopologyChanges(t, projection)
	audit := &environmentBlueprintAtomicAuditStore{
		hierarchyStore: fixture.store,
		failFinal:      failFinal,
	}
	final, err := newEnvironmentBlueprintRepository(audit, audit)
	if err != nil {
		t.Fatal(err)
	}
	result, publishErr := final.PublishEnvironmentBlueprintDesiredRevision(
		ctx,
		netip.MustParsePrefix("10.240.0.0/24"),
		fixture.environment.Record.NetworkPool,
		fixture.project,
		fixture.environment,
		0,
		claim,
		EnvironmentDesiredRevisionIdentity{
			EnvironmentID: fixture.environment.Record.ID,
			RevisionID:    task.ID,
		},
		projection,
		zones,
		services,
		routes,
		releaseGroupPreparation,
		ComponentTaskPreparation{},
		attachPreparation,
		backupPreparation,
		scriptPublication,
		releasePublication,
		BlueprintRequirementGate{},
		task,
		marker,
	)
	return environmentBlueprintAtomicPublication{
		store:                      fixture.store,
		audit:                      audit,
		result:                     result,
		environmentID:              fixture.environment.Record.ID,
		task:                       task,
		markerKey:                  markerKey,
		releasePublicationID:       releasePublicationID,
		activeScriptRevision:       activeScriptRevision,
		candidateAttachIDs:         candidateAttachIDs,
		newBackupSourceIDs:         newBackupSourceIDs,
		newBackupSources:           newBackupSources,
		volumeIDs:                  environmentBlueprintVolumeIDs(projection),
		retainedAttachRevisions:    retainedAttachRevisions,
		retainedGrantRelations:     retainedGrantRelations,
		preexistingBackupRevisions: preexistingBackupRevisions,
		detachWinner:               detachWinner,
	}, publishErr
}
