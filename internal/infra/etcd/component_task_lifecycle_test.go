package etcd

import (
	"bytes"
	"context"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testenvironmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testnetworkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	testrecordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testzones "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

func TestComponentTaskIntentCodecIsStrictAndCanonical(t *testing.T) {
	// Rationale: restart-safe Component promotion depends on one immutable,
	// strictly decoded candidate ordered independently of request map order.
	t.Parallel()
	now := taskJournalTime()
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1001)
	caddy := componentTaskLifecycleRecords(t, environmentID, now, true)
	tunnelCurrent, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1010), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(tunnel current) error = %v", err)
	}
	tunnelCandidate, err := testcomponents.NewRecord(core.Component{
		ID: tunnelCurrent.Desired.ID, Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindEdgeCloudflare, Enabled: true,
		Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
			ZoneIDs:  []string{ids.NewAt(ids.KindNetwork, now, 1012)},
			SecretID: ids.NewAt(ids.KindSecret, now, 1011),
		}},
		GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1012)},
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(tunnel candidate) error = %v", err)
	}
	intent, err := testenvironmentchanges.NewComponentTaskIntent(
		ids.NewAt(ids.KindTask, now, 1013),
		environmentID,
		[]testenvironmentchanges.ComponentTaskCandidate{
			{CurrentRevision: 7, Current: tunnelCurrent, Candidate: tunnelCandidate},
			{CurrentRevision: 6, Current: caddy.current, Candidate: caddy.candidate},
		},
		now,
	)
	if err != nil {
		t.Fatalf("NewComponentTaskIntent() error = %v", err)
	}
	if intent.Candidates[0].Current.Desired.ID > intent.Candidates[1].Current.Desired.ID {
		t.Fatal("NewComponentTaskIntent() did not sort candidates")
	}
	encoded, err := testenvironmentchanges.EncodeComponentTaskIntent(intent)
	if err != nil {
		t.Fatalf("encodeComponentTaskIntent() error = %v", err)
	}
	decoded, err := testenvironmentchanges.DecodeComponentTaskIntent(encoded)
	if err != nil || !reflect.DeepEqual(decoded, intent) {
		t.Fatalf("decodeComponentTaskIntent() = %#v, %v", decoded, err)
	}
	corrupt := bytes.Replace(encoded, []byte(`"status":`), []byte(`"unknown":0,"status":`), 1)
	if _, err := testenvironmentchanges.DecodeComponentTaskIntent(corrupt); err == nil {
		t.Fatal("decodeComponentTaskIntent() accepted an unknown field")
	}
}

func TestComponentTaskIntentAcceptsEnabledHealthRepair(t *testing.T) {
	t.Parallel()
	now := taskJournalTime().Add(time.Minute)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1051)
	record, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, now, 1052), Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{ids.NewAt(ids.KindNetwork, now, 1053)},
		}},
		GeneratedServices: []string{ids.NewAt(ids.KindService, now, 1054)}, PinnedIPv4: "10.40.12.2",
	})
	if err != nil {
		t.Fatalf("NewComponentRecord() error = %v", err)
	}
	if _, err := testenvironmentchanges.NewComponentTaskIntent(
		ids.NewAt(ids.KindTask, now, 1055),
		environmentID,
		[]testenvironmentchanges.ComponentTaskCandidate{{CurrentRevision: 1, Current: record, Candidate: record}},
		now,
	); err != nil {
		t.Fatalf("NewComponentTaskIntent(health repair) error = %v", err)
	}
}

func TestTaskAcknowledgementPromotesComponentCandidateAndMovesReservation(t *testing.T) {
	// Rationale: a successful Agent Task must publish Component state and drop
	// the superseded Caddy address in the same terminal transaction.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime()
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1101)
	task := validTaskRecord(now)
	task.Type = testtaskjournal.TaskUpdate
	task.Target = environmentID
	pinComponentTaskDesiredRevision(&task)
	createLifecycleTask(t, repository, task)
	records := componentTaskLifecycleRecords(t, environmentID, now, true)
	seedComponentTaskLifecycle(t, store, task, records)

	agentID := ids.NewAt(ids.KindAgent, now, 1102)
	if _, found, err := repository.ClaimNextTask(ctx, agentID, 3, now.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %v/%v", found, err)
	}
	terminalAt := now.Add(2 * time.Second)
	terminal, err := repository.AcknowledgeTask(
		ctx, agentID, 3, task.ID, taskAssignmentIDForTest(t, repository,
			task.ID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(), terminalAt)

	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask() = %#v, %v", terminal, err)
	}
	assertComponentTaskTerminalState(t, store, task, records, testtaskjournal.TaskStatusCompleted, true)
	if _, err := repository.AcknowledgeTask(
		ctx, agentID, 3, task.ID, taskAssignmentIDForTest(t, repository,
			task.ID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(), terminalAt); err != nil {
		t.Fatalf("AcknowledgeTask(replay) error = %v", err)
	}
}

func TestTaskTimeoutDiscardsComponentCandidateAndNewReservation(t *testing.T) {
	// Rationale: an Agent timeout must retain the last active Component while
	// releasing only the address reserved for the unpromoted candidate.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(10 * time.Second)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1201)
	task := validTaskRecord(now)
	task.Type = testtaskjournal.TaskUpdate
	task.Target = environmentID
	pinComponentTaskDesiredRevision(&task)
	createLifecycleTask(t, repository, task)
	records := componentTaskLifecycleRecords(t, environmentID, now, false)
	seedComponentTaskLifecycle(t, store, task, records)

	agentID := ids.NewAt(ids.KindAgent, now, 1202)
	if _, found, err := repository.ClaimNextTask(ctx, agentID, 9, now.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %v/%v", found, err)
	}
	count, err := repository.TimeoutAgentAssignments(ctx, agentID, 9, 1, now.Add(24*time.Hour))
	if err != nil || count != 1 {
		t.Fatalf("TimeoutAgentAssignments() count/error = %d/%v", count, err)
	}
	assertComponentTaskTerminalState(t, store, task, records, testtaskjournal.TaskStatusTimedOut, false)
}

func TestPendingTaskAbortDiscardsComponentCandidateAndNewReservation(t *testing.T) {
	// Rationale: aborting before Agent assignment must release the unpublished
	// Component address and fence in the same transaction as the Task abort.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(20 * time.Second)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1251)
	task := validTaskRecord(now)
	task.Type = testtaskjournal.TaskUpdate
	task.Target = environmentID
	pinComponentTaskDesiredRevision(&task)
	createLifecycleTask(t, repository, task)
	records := componentTaskLifecycleRecords(t, environmentID, now, true)
	seedComponentTaskLifecycle(t, store, task, records)

	terminalAt := now.Add(time.Second)
	terminal, err := repository.AbortPendingTask(ctx, task.ID, terminalAt)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusAborted {
		t.Fatalf("AbortPendingTask() = %#v, %v", terminal, err)
	}
	assertComponentTaskTerminalState(t, store, task, records, testtaskjournal.TaskStatusAborted, false)
	if _, err := repository.AbortPendingTask(ctx, task.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask(replay) error = %v", err)
	}
}

func TestRetryTaskReacquiresExactComponentCandidateReservation(t *testing.T) {
	// Rationale: retry preserves the source plan hash, so it must publish the
	// same Component candidate and reacquire its exact pinned address.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(30 * time.Second)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1261)
	source := validTaskRecord(now)
	source.Type = testtaskjournal.TaskUpdate
	source.Target = environmentID
	pinComponentTaskDesiredRevision(&source)
	createLifecycleTask(t, repository, source)
	records := componentTaskLifecycleRecords(t, environmentID, now, true)
	seedComponentTaskLifecycle(t, store, source, records)
	seedComponentRetryProjection(t, store, source, records)
	terminalAt := now.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, source.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}

	retryID := ids.NewAt(ids.KindTask, terminalAt.Add(time.Second), 1262)
	marker := pendingRetryMarker(source, retryID, terminalAt.Add(time.Second), "component-retry-key-0001")
	result, err := repository.RetryTask(ctx, source.ID, retryID, testtaskjournal.TaskActorOperator, marker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	outcome, _, conflict, err := result.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask() outcome/conflict/error = %v/%v/%v", outcome, conflict, err)
	}
	registry := readComponentTaskRegistry(t, store, records.candidateZone)
	if registry.Reservations[records.candidate.Desired.ID] != records.candidate.Runtime.PinnedIPv4 {
		t.Fatal("Component retry did not reacquire the exact candidate address")
	}
	intentValue, err := store.Get(ctx, testenvironmentchanges.ComponentTaskIntentKey(retryID))
	if err != nil || intentValue.Entry == nil {
		t.Fatalf("Get(retry Component intent) = %#v, %v", intentValue, err)
	}
	intent, err := testenvironmentchanges.DecodeComponentTaskIntent(intentValue.Entry.Value)
	if err != nil || intent.Status != testtaskjournal.TaskStatusPending || intent.TaskID != retryID ||
		!intent.CreatedAt.Equal(marker.CreatedAt) || len(intent.Candidates) != 1 ||
		intent.Candidates[0].CurrentRevision <= 0 ||
		!reflect.DeepEqual(intent.Candidates[0].Current, records.current) ||
		!reflect.DeepEqual(intent.Candidates[0].Candidate, records.candidate) {
		t.Fatalf("retry Component intent = %#v, %v", intent, err)
	}
}

func TestRetryTaskRejectsTakenComponentCandidateReservation(t *testing.T) {
	// Rationale: a retry cannot silently allocate another address because that
	// would execute bytes different from the retained source plan.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(40 * time.Second)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1271)
	source := validTaskRecord(now)
	source.Type = testtaskjournal.TaskUpdate
	source.Target = environmentID
	pinComponentTaskDesiredRevision(&source)
	createLifecycleTask(t, repository, source)
	records := componentTaskLifecycleRecords(t, environmentID, now, true)
	seedComponentTaskLifecycle(t, store, source, records)
	seedComponentRetryProjection(t, store, source, records)
	terminalAt := now.Add(time.Second)
	if _, err := repository.AbortPendingTask(ctx, source.ID, terminalAt); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	registry := testnetworkreservations.ComponentAddressRegistry{Reservations: map[string]string{
		ids.NewAt(ids.KindComponent, now, 1272): records.candidate.Runtime.PinnedIPv4,
	}}
	value, err := testnetworkreservations.EncodeComponentAddressRegistry(records.candidateZone, registry)
	if err != nil {
		t.Fatalf("encodeComponentAddressRegistry() error = %v", err)
	}
	registryKey := testnetworkreservations.ComponentAddressRegistryKey(records.candidateZone.Desired.ID)
	stored, err := store.Get(ctx, registryKey)
	if err != nil || stored.Entry == nil {
		t.Fatalf("Get(Component registry) = %#v, %v", stored, err)
	}
	transaction, err := store.Transact(ctx, []testkeyvalue.Condition{{
		Key: registryKey, ModRevision: stored.Entry.ModRevision,
	}}, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: registryKey, Value: value}})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("replace Component registry = %#v, %v", transaction, err)
	}

	retryID := ids.NewAt(ids.KindTask, terminalAt.Add(time.Second), 1273)
	marker := pendingRetryMarker(source, retryID, terminalAt.Add(time.Second), "component-retry-key-0002")
	if _, err := repository.RetryTask(ctx, source.ID, retryID, testtaskjournal.TaskActorOperator, marker); err == nil {
		t.Fatal("RetryTask() accepted a candidate address reserved by another Component")
	}
}

func seedComponentRetryProjection(
	t *testing.T,
	store *memoryTaskStore,
	task TaskRecord,
	records componentTaskLifecycleFixture,
) {
	t.Helper()
	tunnel, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, task.CreatedAt, 1281), Owner: core.ComponentOwnerEnvironment,
		OwnerID: task.Target, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(tunnel) error = %v", err)
	}
	components := []testcomponents.Record{records.candidate, tunnel}
	sort.Slice(components, func(left int, right int) bool {
		return components[left].Desired.Kind < components[right].Desired.Kind
	})
	zones := []testenvironmentprojection.EnvironmentZoneProjection{
		{EnvironmentID: task.Target, Desired: records.candidateZone.Desired},
		{EnvironmentID: task.Target, Desired: records.currentZone.Desired},
	}
	desiredService := serviceRecordTestDesired()
	desiredService.ID = records.candidate.Runtime.GeneratedServices[0]
	desiredService.Name = "caddy"
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID:    task.Target,
		RevisionID:       ids.NewAt(ids.KindTask, task.CreatedAt, 1282),
		RenderGeneration: uint64(task.RenderGeneration),
		DesiredZones:     zones,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: task.Target,
			Desired:       desiredService,
		}},
		Components: components,
	})
	value, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
	if err != nil {
		t.Fatalf("encodeEnvironmentComposeProjection() error = %v", err)
	}
	seedTaskRepositoryValue(
		t,
		store,
		testenvironmentprojection.EnvironmentComposeProjectionStorageKey(task.Target),
		value,
	)
}

type componentTaskLifecycleFixture struct {
	current       testcomponents.Record
	candidate     testcomponents.Record
	currentZone   testzones.Record
	candidateZone testzones.Record
}

func componentTaskLifecycleRecords(
	t *testing.T,
	environmentID string,
	now time.Time,
	currentEnabled bool,
) componentTaskLifecycleFixture {
	t.Helper()
	componentID := ids.NewAt(ids.KindComponent, now, 1301)
	serviceID := ids.NewAt(ids.KindService, now, 1302)
	currentZone, err := testzones.NewRecord(environmentID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1303), Name: "current", Subnet: "10.40.10.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord(current) error = %v", err)
	}
	candidateZone, err := testzones.NewRecord(environmentID, core.Zone{
		ID: ids.NewAt(ids.KindNetwork, now, 1304), Name: "candidate", Subnet: "10.40.11.0/24",
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
	})
	if err != nil {
		t.Fatalf("NewZoneRecord(candidate) error = %v", err)
	}
	currentComponent := core.Component{
		ID: componentID, Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy,
	}
	if currentEnabled {
		currentComponent.Enabled = true
		currentComponent.Config = core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{currentZone.Desired.ID},
		}}
		currentComponent.GeneratedServices = []string{serviceID}
		currentComponent.PinnedIPv4 = "10.40.10.2"
		currentComponent.Healthy = true
	}
	current, err := testcomponents.NewRecord(currentComponent)
	if err != nil {
		t.Fatalf("NewComponentRecord(current) error = %v", err)
	}
	candidate, err := testcomponents.NewRecord(core.Component{
		ID: componentID, Owner: core.ComponentOwnerEnvironment,
		OwnerID: environmentID, Kind: core.ComponentKindIngressCaddy, Enabled: true,
		Config: core.ComponentConfig{Caddy: &core.CaddyComponentConfig{
			ZoneIDs: []string{candidateZone.Desired.ID},
		}},
		GeneratedServices: []string{serviceID}, PinnedIPv4: "10.40.11.2",
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(candidate) error = %v", err)
	}
	return componentTaskLifecycleFixture{
		current: current, candidate: candidate, currentZone: currentZone, candidateZone: candidateZone,
	}
}

func seedComponentTaskLifecycle(
	t *testing.T,
	store *memoryTaskStore,
	task TaskRecord,
	records componentTaskLifecycleFixture,
) {
	t.Helper()
	projectID := ids.NewAt(ids.KindProject, task.CreatedAt, 1290)
	environment := testhierarchy.EnvironmentRecord{
		ID: task.Target, ProjectID: projectID, Name: "production", NetworkPool: "10.40.0.0/16",
		VolumeDir:         "/var/lib/groundplane/vol/platform/" + projectID + "/" + task.Target,
		ProvisioningState: testhierarchy.EnvironmentProvisioningReady, CreateTaskID: task.ID, CreatedAt: task.CreatedAt,
	}
	environmentBytes, err := testhierarchy.EncodeEnvironment(environment)
	if err != nil {
		t.Fatalf("encodeEnvironment() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, testhierarchy.EnvironmentKey(task.Target), environmentBytes)
	seedComponentTaskDesiredProjection(t, store, task, records)
	currentBytes, err := testcomponents.EncodeRecord(records.current)
	if err != nil {
		t.Fatalf("encodeComponentRecord() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, testcomponents.RecordKey(records.current.Desired.ID), currentBytes)
	stored, err := store.Get(context.Background(), testcomponents.RecordKey(records.current.Desired.ID))
	if err != nil || stored.Entry == nil {
		t.Fatalf("Get(Component) = %#v, %v", stored, err)
	}
	if records.current.Desired.Enabled {
		registry := testnetworkreservations.ComponentAddressRegistry{Reservations: map[string]string{
			records.current.Desired.ID: records.current.Runtime.PinnedIPv4,
		}}
		value, encodeErr := testnetworkreservations.EncodeComponentAddressRegistry(records.currentZone, registry)
		if encodeErr != nil {
			t.Fatalf("encodeComponentAddressRegistry(current) error = %v", encodeErr)
		}
		seedTaskRepositoryValue(
			t,
			store,
			testnetworkreservations.ComponentAddressRegistryKey(records.currentZone.Desired.ID),
			value,
		)
	}
	candidateRegistry := testnetworkreservations.ComponentAddressRegistry{Reservations: map[string]string{
		records.candidate.Desired.ID: records.candidate.Runtime.PinnedIPv4,
	}}
	candidateRegistryBytes, err := testnetworkreservations.EncodeComponentAddressRegistry(
		records.candidateZone,
		candidateRegistry,
	)
	if err != nil {
		t.Fatalf("encodeComponentAddressRegistry(candidate) error = %v", err)
	}
	seedTaskRepositoryValue(
		t,
		store,
		testnetworkreservations.ComponentAddressRegistryKey(records.candidateZone.Desired.ID),
		candidateRegistryBytes,
	)
	intent, err := testenvironmentchanges.NewComponentTaskIntent(
		task.ID,
		task.Target,
		[]testenvironmentchanges.ComponentTaskCandidate{{
			CurrentRevision: stored.Entry.ModRevision,
			Current:         records.current, Candidate: records.candidate,
		}},
		task.CreatedAt,
	)
	if err != nil {
		t.Fatalf("NewComponentTaskIntent() error = %v", err)
	}
	intentBytes, err := testenvironmentchanges.EncodeComponentTaskIntent(intent)
	if err != nil {
		t.Fatalf("encodeComponentTaskIntent() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, testenvironmentchanges.ComponentTaskIntentKey(task.ID), intentBytes)
	seedTaskRepositoryValue(
		t,
		store,
		testenvironmentchanges.ComponentTaskActiveEnvironmentKey(task.Target),
		[]byte(task.ID),
	)
}

func pinComponentTaskDesiredRevision(task *TaskRecord) {
	if task.Params == nil {
		task.Params = make(map[string]string)
	}
	task.Params[testblueprints.EnvironmentDesiredRevisionParam] = task.ID
}

func seedComponentTaskDesiredProjection(
	t *testing.T,
	store *memoryTaskStore,
	task TaskRecord,
	records componentTaskLifecycleFixture,
) {
	t.Helper()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	zones := []testenvironmentprojection.EnvironmentZoneProjection{
		{EnvironmentID: task.Target, Desired: records.candidateZone.Desired},
		{EnvironmentID: task.Target, Desired: records.currentZone.Desired},
	}
	tunnel, err := testcomponents.NewRecord(core.Component{
		ID: ids.NewAt(ids.KindComponent, task.CreatedAt, 1281), Owner: core.ComponentOwnerEnvironment,
		OwnerID: task.Target, Kind: core.ComponentKindEdgeCloudflare,
	})
	if err != nil {
		t.Fatalf("NewComponentRecord(tunnel) error = %v", err)
	}
	components := []testcomponents.Record{records.candidate, tunnel}
	sort.Slice(components, func(left int, right int) bool {
		return components[left].Desired.Kind < components[right].Desired.Kind
	})
	desiredService := serviceRecordTestDesired()
	desiredService.ID = records.candidate.Runtime.GeneratedServices[0]
	desiredService.Name = "caddy"
	projection := withTestEnvironmentComposeArtifact(testenvironmentprojection.EnvironmentComposeProjection{
		EnvironmentID:    task.Target,
		RevisionID:       task.Params[testblueprints.EnvironmentDesiredRevisionParam],
		RenderGeneration: uint64(task.RenderGeneration),
		DesiredZones:     zones,
		DesiredServices: []testservices.EnvironmentServiceProjection{{
			EnvironmentID: task.Target,
			Desired:       desiredService,
		}},
		Components: components,
	})
	stageEnvironmentBlueprintForPublicationTest(
		t,
		hierarchy,
		0,
		environmentBlueprintTestRevision(task.Target, task, "services: {}\n"),
		projection,
		environmentBlueprintTestMarker(task, task.Target),
	)
	headValue, err := testidempotency.EncodeTaskReference(task.Params[testblueprints.EnvironmentDesiredRevisionParam])
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, testblueprints.EnvironmentBlueprintHeadKey(task.Target), headValue)
}

func assertComponentTaskTerminalState(
	t *testing.T,
	store *memoryTaskStore,
	task TaskRecord,
	records componentTaskLifecycleFixture,
	status testtaskjournal.TaskStatus,
	promoted bool,
) {
	t.Helper()
	componentValue, err := store.Get(context.Background(), testcomponents.RecordKey(records.current.Desired.ID))
	if err != nil || componentValue.Entry == nil {
		t.Fatalf("Get(Component) = %#v, %v", componentValue, err)
	}
	component, err := testcomponents.DecodeRecord(componentValue.Entry.Value)
	if err != nil {
		t.Fatalf("decodeComponentRecord() error = %v", err)
	}
	want := records.current
	if promoted {
		want = records.candidate
		want.Runtime.Healthy = want.Desired.Enabled
	}
	if !reflect.DeepEqual(component, want) {
		t.Fatalf("active Component = %#v, want %#v", component, want)
	}
	currentRegistry := readComponentTaskRegistry(t, store, records.currentZone)
	candidateRegistry := readComponentTaskRegistry(t, store, records.candidateZone)
	if promoted {
		if _, retained := currentRegistry.Reservations[records.current.Desired.ID]; retained {
			t.Fatal("successful Component promotion retained the previous address")
		}
		if candidateRegistry.Reservations[records.candidate.Desired.ID] != records.candidate.Runtime.PinnedIPv4 {
			t.Fatal("successful Component promotion lost the candidate address")
		}
	} else {
		if records.current.Desired.Enabled &&
			currentRegistry.Reservations[records.current.Desired.ID] != records.current.Runtime.PinnedIPv4 {
			t.Fatal("failed Component promotion lost the active address")
		}
		if _, retained := candidateRegistry.Reservations[records.candidate.Desired.ID]; retained {
			t.Fatal("failed Component promotion retained the candidate address")
		}
	}
	intentValue, err := store.Get(context.Background(), testenvironmentchanges.ComponentTaskIntentKey(task.ID))
	if err != nil || intentValue.Entry == nil {
		t.Fatalf("Get(Component intent) = %#v, %v", intentValue, err)
	}
	intent, err := testenvironmentchanges.DecodeComponentTaskIntent(intentValue.Entry.Value)
	if err != nil || intent.Status != status || intent.TerminalAt == nil {
		t.Fatalf("terminal Component intent = %#v, %v", intent, err)
	}
	active, err := store.Get(
		context.Background(),
		testenvironmentchanges.ComponentTaskActiveEnvironmentKey(task.Target),
	)
	if err != nil || active.Entry != nil {
		t.Fatalf("active Component intent index = %#v, %v", active, err)
	}
}

func readComponentTaskRegistry(
	t *testing.T,
	store *memoryTaskStore,
	zone testzones.Record,
) testnetworkreservations.ComponentAddressRegistry {
	t.Helper()
	result, err := store.Get(context.Background(), testnetworkreservations.ComponentAddressRegistryKey(zone.Desired.ID))
	if err != nil {
		t.Fatalf("Get(Component registry) error = %v", err)
	}
	if result.Entry == nil {
		return testnetworkreservations.ComponentAddressRegistry{Reservations: map[string]string{}}
	}
	registry, err := testrecordcodec.Decode[testnetworkreservations.ComponentAddressRegistry](
		result.Entry.Value,
		"component_address_registry",
	)
	if err != nil {
		t.Fatalf("decode Component registry error = %v", err)
	}
	return registry
}

func TestComponentTaskAcknowledgementProjectionRootConditionOwnership(t *testing.T) {
	environmentID := "environment_01M1DRGWFJ7W02MY83A4XZ4R4F"
	tests := []struct {
		name          string
		status        testtaskjournal.TaskStatus
		materializes  string
		wantCondition bool
	}{
		{
			name:         "successful materialization delegates condition",
			status:       testtaskjournal.TaskStatusCompleted,
			materializes: environmentID,
		},
		{
			name:          "successful non-materialization retains condition",
			status:        testtaskjournal.TaskStatusCompleted,
			wantCondition: true,
		},
		{
			name:          "failed materialization retains condition",
			status:        testtaskjournal.TaskStatusFailed,
			materializes:  environmentID,
			wantCondition: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			task := TaskRecord{
				Params: map[string]string{testtaskjournal.TaskMaterializationEnvironmentParam: test.materializes},
			}
			got := componentTaskAcknowledgementRequiresBlueprintRootCondition(task, test.status, environmentID)
			if got != test.wantCondition {
				t.Fatalf(
					"componentTaskAcknowledgementRequiresBlueprintRootCondition() = %t, want %t",
					got,
					test.wantCondition,
				)
			}
		})
	}
}
