package etcd

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestPlatformComponentTaskClassifierRejectsActiveFence(t *testing.T) {
	t.Parallel()
	records, err := DefaultPlatformComponents(false)
	if err != nil {
		t.Fatal(err)
	}
	componentID := records[0].Desired.ID
	current := Versioned[ComponentRecord]{Record: records[0], Revision: 7}
	indexes := &GetManyResult{Values: []*KeyValue{
		{Value: []byte(componentID), ModRevision: 8},
		{Value: []byte(componentID), ModRevision: 9},
	}}
	values := []*KeyValue{
		{ModRevision: 7},
		{Value: []byte(componentID), ModRevision: 8},
		{Value: []byte(componentID), ModRevision: 9},
		nil, nil, nil, nil, nil,
		{Value: []byte(ids.New(ids.KindTask)), ModRevision: 10},
	}
	if err := classifyPlatformComponentTaskConflict(
		current,
		indexes,
		ids.New(ids.KindOperation),
	)(10, values); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("active-fence classification error = %v, want state conflict", err)
	}
}

func TestPlatformDNSResolverTaskAttemptIncludesOperatorMutation(t *testing.T) {
	t.Parallel()
	task := newPlatformDNSResolverTask(
		"cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		time.Date(2026, time.August, 30, 1, 0, 0, 0, time.UTC),
	)
	task.Actor = TaskActorOperator
	delete(task.Params, TaskAutomaticReconcileParam)
	if !isPlatformDNSResolverTaskAttempt(task) {
		t.Fatal("operator Component mutation is not a resolver Task attempt")
	}
	task.Actor = TaskActorSystem
	if isPlatformDNSResolverTaskAttempt(task) {
		t.Fatal("unmarked system Task was accepted as resolver reconciliation")
	}
}

func TestPlatformDNSResolverTaskFencePublishesAndReplacesActiveTask(t *testing.T) {
	ctx := context.Background()
	store := newMemoryHierarchyStore()
	components, err := newComponentRepository(store)
	if err != nil {
		t.Fatalf("newComponentRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	records, err := DefaultPlatformComponents(false)
	if err != nil {
		t.Fatalf("DefaultPlatformComponents() error = %v", err)
	}
	now := time.Date(2026, time.August, 30, 2, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, now, 1)
	records[0], err = SetComponentRuntime(records[0], []string{serviceID}, "10.200.0.2", true)
	if err != nil {
		t.Fatalf("SetComponentRuntime() error = %v", err)
	}
	current, err := components.CreatePlatformComponent(ctx, records[0])
	if err != nil {
		t.Fatalf("CreatePlatformComponent() error = %v", err)
	}
	projection, err := NewHostResolutionProjectionRecord(current.ReadRevision, nil)
	if err != nil {
		t.Fatalf("NewHostResolutionProjectionRecord() error = %v", err)
	}
	projectionValue, err := encodeHostResolutionProjectionRecord(projection)
	if err != nil {
		t.Fatalf("encodeHostResolutionProjectionRecord() error = %v", err)
	}
	if result, err := store.Transact(
		ctx,
		nil,
		[]Mutation{{Type: MutationPut, Key: hostResolutionProjectionKey, Value: projectionValue}},
	); err != nil ||
		!result.Succeeded {
		t.Fatalf("seed projection transaction = %#v, %v", result, err)
	}
	_ = projectionValue
	digest, err := PlatformComponentDesiredDigest(current.Record)
	if err != nil {
		t.Fatalf("PlatformComponentDesiredDigest() error = %v", err)
	}
	preparedTasks := make(map[string]TaskRecord)
	preparedInputs := make(map[string]PlatformComponentTaskRenderInput)
	tasks.platformResolverTaskPreparer = func(_ context.Context, component Versioned[ComponentRecord], input HostResolutionProjectionRecord, task TaskRecord, observation *ComponentObservationRecord) (PlatformComponentTaskRenderInput, error) {
		prepared := PlatformComponentTaskRenderInput{
			PlanID:                      task.PlanID,
			TaskID:                      task.ID,
			ComponentID:                 component.Record.Desired.ID,
			DesiredSHA256:               digest,
			BaselineGeneration:          1,
			BaselineSHA256:              strings.Repeat("a", 64),
			HostResolutionInputRevision: input.InputRevision,
			HostResolutionSHA256:        input.InputSHA256,
			Config:                      *component.Record.Desired.Config.CoreDNS,
			GeneratedServiceID:          serviceID,
			DefinitionSHA256:            strings.Repeat("b", 64),
			CatalogSHA256:               strings.Repeat("c", 64),
			ActionID:                    "activate-config",
			ArtifactID:                  ids.NewAt(ids.KindConfig, now, 2),
			ComposeArtifactID:           ids.NewAt(ids.KindConfig, now, 3),
			ComposeArtifact: testPlatformComponentComposeArtifact(
				ids.NewAt(ids.KindConfig, now, 3),
				serviceID,
				strings.Repeat("9", 64),
			),
			ArtifactSHA256:      strings.Repeat("d", 64),
			OwnershipPlanID:     task.PlanID,
			OwnershipGeneration: 1,
			ImageRepository:     "coredns/coredns",
			ImageIndexDigest:    strings.Repeat("7", 64),
			ImageConfigDigest:   strings.Repeat("9", 64),
			ImageChildDigest: strings.Repeat(
				"8",
				64,
			),
			ImageReference:      "coredns/coredns@sha256:" + strings.Repeat("8", 64),
			ImageOS:             "linux",
			ImageArchitecture:   "amd64",
			ArtifactLength:      1,
			PlanSHA256:          strings.Repeat("e", 64),
			ExecutionPlanSHA256: strings.Repeat("f", 64),
		}
		prepared.EnsureService = len(component.Record.Runtime.GeneratedServices) == 0 ||
			!component.Record.Runtime.Healthy
		if observation != nil {
			prepared.PriorObservationRevision = observation.Revision
			prepared.PredecessorTaskID = observation.TaskID
			prepared.ExpectedPreviousArtifactSHA256 = observation.CorefileSHA256
			if observation.DNSResolverProof != nil {
				prepared.ExpectedPreviousArtifactID = observation.DNSResolverProof.ArtifactID
				prepared.ExpectedPreviousGeneration = observation.DNSResolverProof.RenderGeneration
			}
			if prepared.EnsureService && observation.Enabled {
				prepared.RollbackComposeArtifact = observation.ComposeArtifact
			}
		}
		preparedTasks[task.ID] = cloneTaskRecord(task)
		preparedInputs[task.ID] = clonePlatformComponentTaskRenderInput(prepared)
		return prepared, nil
	}
	operatorTask := newPlatformDNSResolverTask(current.Record.Desired.ID, now.Add(-time.Second))
	operatorTask.Actor = TaskActorOperator
	operatorTask.IdempotencyKey = "platform-component-abort-0001"
	delete(operatorTask.Params, TaskAutomaticReconcileParam)
	operatorInput, err := tasks.platformResolverTaskPreparer(
		ctx, current, projection, operatorTask, nil,
	)
	if err != nil {
		t.Fatalf("prepare operator resolver Task = %v", err)
	}
	operatorTask.PlanHash = operatorInput.ExecutionPlanSHA256
	operatorDesired, err := ProjectComponentRecord(current.Record)
	if err != nil {
		t.Fatalf("ProjectComponentRecord(operator resolver) = %v", err)
	}
	marker := pendingTaskMarker(operatorTask)
	marker.Locator.ScopeKind = IdempotencyScopePlatform
	marker.Locator.ScopeID = "-"
	marker.Locator.Route = "/components/{id}/update"
	published, err := tasks.ReplacePlatformComponentDesiredWithTask(
		ctx, current, operatorDesired, operatorTask, operatorInput, marker,
	)
	if err != nil {
		t.Fatalf("publish operator resolver Task = %v", err)
	}
	if outcome, _, conflict, classifyErr := published.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("publish operator resolver Task outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	aborted, err := tasks.AbortPendingTask(ctx, operatorTask.ID, now)
	if err != nil || aborted.Record.Status != TaskStatusAborted {
		t.Fatalf("AbortPendingTask(operator resolver) = %#v, %v", aborted, err)
	}
	if activeRead, readErr := store.Get(ctx, platformComponentTaskActiveKey(current.Record.Desired.ID)); readErr != nil ||
		activeRead.Entry != nil {
		t.Fatalf("aborted operator resolver active fence = %#v, %v", activeRead, readErr)
	}
	if replay, replayErr := tasks.AbortPendingTask(ctx, operatorTask.ID, now.Add(time.Second)); replayErr != nil ||
		replay.Revision != aborted.Revision {
		t.Fatalf("AbortPendingTask(operator resolver replay) = %#v, %v", replay, replayErr)
	}
	current, err = components.GetComponent(ctx, current.Record.Desired.ID)
	if err != nil {
		t.Fatalf("GetComponent(after operator abort) error = %v", err)
	}
	first := newPlatformDNSResolverTask(current.Record.Desired.ID, now)
	firstChange, err := tasks.preparePlatformDNSResolverTaskContribution(
		ctx, current, projection, first, nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatalf("prepare first resolver Task = %v", err)
	}
	if result, err := store.Transact(
		ctx,
		firstChange.conditions,
		firstChange.mutations,
	); err != nil ||
		!result.Succeeded {
		t.Fatalf("first resolver Task transaction = %#v, %v", result, err)
	}
	clearHostResolutionReconciliationChange(firstChange)
	activeEntry, err := store.Get(ctx, platformComponentTaskActiveKey(current.Record.Desired.ID))
	if err != nil || activeEntry.Entry == nil {
		t.Fatalf("active resolver Task = %#v, %v", activeEntry, err)
	}
	active := KeyValue{
		Key:         activeEntry.Entry.Key,
		Value:       append([]byte(nil), activeEntry.Entry.Value...),
		ModRevision: activeEntry.Entry.ModRevision,
	}
	firstRenderRead, err := store.Get(ctx, platformComponentTaskRenderInputKey(first.PlanID))
	if err != nil || firstRenderRead.Entry == nil {
		t.Fatalf("predecessor resolver render input = %#v, %v", firstRenderRead, err)
	}
	firstInput, err := decodePlatformComponentTaskRenderInput(firstRenderRead.Entry.Value)
	if err != nil {
		t.Fatalf("decode predecessor resolver render input: %v", err)
	}
	firstTerminal := cloneTaskRecord(first)
	firstTerminal.Status = TaskStatusCompleted
	firstResult := platformDNSProof(firstInput, uint64(first.RenderGeneration), now.Add(time.Second))
	firstTerminal.Result = &firstResult
	second := newPlatformDNSResolverTask(current.Record.Desired.ID, now.Add(time.Second))
	secondChange, err := tasks.preparePlatformDNSResolverTaskContribution(
		ctx,
		current,
		projection,
		second,
		&active,
		&firstTerminal,
		nil,
		nil,
	)
	if err != nil {
		t.Fatalf("prepare successor resolver Task = %v", err)
	}
	if result, err := store.Transact(
		ctx,
		secondChange.conditions,
		secondChange.mutations,
	); err != nil ||
		!result.Succeeded {
		t.Fatalf("successor resolver Task transaction = %#v, %v", result, err)
	}
	clearHostResolutionReconciliationChange(secondChange)
	updated, err := store.Get(ctx, platformComponentTaskActiveKey(current.Record.Desired.ID))
	if err != nil || updated.Entry == nil || string(updated.Entry.Value) != second.ID {
		t.Fatalf("active resolver successor = %#v, %v", updated, err)
	}
	if firstRead, err := store.Get(ctx, taskKey(first.ID)); err != nil || firstRead.Entry == nil {
		t.Fatalf("first resolver Task disappeared = %#v, %v", firstRead, err)
	}
	secondRead, err := store.Get(ctx, taskKey(second.ID))
	if err != nil || secondRead.Entry == nil {
		t.Fatalf("successor resolver Task missing = %#v, %v", secondRead, err)
	}
	secondRecord, err := decodeTaskRecord(secondRead.Entry.Value)
	if err != nil || len(secondRecord.Steps) != 2 || secondRecord.RetryOf != "" ||
		secondRecord.OperationID == first.OperationID || secondRecord.PlanID == first.PlanID {
		t.Fatalf("automatic resolver Task steps = %#v, %v", secondRecord.Steps, err)
	}
	renderRead, err := store.Get(ctx, platformComponentTaskRenderInputKey(second.PlanID))
	if err != nil || renderRead.Entry == nil {
		t.Fatalf("successor resolver render input = %#v, %v", renderRead, err)
	}
	secondInput, err := decodePlatformComponentTaskRenderInput(renderRead.Entry.Value)
	if err != nil {
		t.Fatalf("decode successor resolver render input: %v", err)
	}
	if secondInput.PriorObservationModRevision != 0 || secondInput.PriorObservationRevision != 1 ||
		secondInput.PredecessorTaskID != first.ID || secondInput.ExpectedPreviousArtifactSHA256 != firstInput.ArtifactSHA256 {
		t.Fatalf("same-transaction successor prior fence = %#v", secondInput)
	}
	preparedTask := preparedTasks[second.ID]
	preparedInput := preparedInputs[second.ID]
	if len(preparedTask.Steps) != len(secondRecord.Steps) {
		t.Fatalf("successor prepared/persisted step counts = %d/%d", len(preparedTask.Steps), len(secondRecord.Steps))
	}
	for index := range secondRecord.Steps {
		if preparedTask.Steps[index].ID != secondRecord.Steps[index].ID {
			t.Fatalf("successor prepared/persisted step %d = %s/%s",
				index, preparedTask.Steps[index].ID, secondRecord.Steps[index].ID)
		}
	}
	if preparedInput.ExpectedPreviousArtifactSHA256 != secondInput.ExpectedPreviousArtifactSHA256 ||
		preparedInput.ExpectedPreviousArtifactID != secondInput.ExpectedPreviousArtifactID ||
		preparedInput.ExpectedPreviousGeneration != secondInput.ExpectedPreviousGeneration {
		t.Fatalf("successor prepared/persisted predecessor authority = %#v/%#v", preparedInput, secondInput)
	}

	// Rationale: the predecessor observation is published in the same terminal
	// transaction that plans the successor, so the successor must fence by that
	// immutable predecessor lineage rather than the pre-transaction revision.
	observedAt := now.Add(2 * time.Second)
	proofResult := platformDNSProof(secondInput, uint64(secondRecord.RenderGeneration), observedAt)
	agentID := ids.NewAt(ids.KindAgent, now, 8)
	predecessorObservation := ComponentObservationRecord{
		ComponentID: secondInput.ComponentID, ServiceID: secondInput.GeneratedServiceID,
		PlanID: secondInput.OwnershipPlanID, ComposeArtifactID: secondInput.ComposeArtifactID,
		Enabled: true, Healthy: true, DesiredGeneration: uint64(current.Revision),
		RenderGeneration: uint64(secondRecord.RenderGeneration), AgentID: agentID, AgentGeneration: 1,
		BaselineGeneration: secondInput.BaselineGeneration, OwnershipGeneration: secondInput.OwnershipGeneration,
		CorefileSHA256: secondInput.ArtifactSHA256, InputSHA256: secondInput.HostResolutionSHA256,
		ObservedAt: observedAt, TaskID: first.ID, StepID: ids.NewAt(ids.KindStep, now, 9), Revision: 1,
		DNSResolverProof: proofResult.DNSResolverCandidateObservation,
		ComposeArtifact: testPlatformComponentComposeArtifact(
			secondInput.ComposeArtifactID,
			secondInput.GeneratedServiceID,
			secondInput.ImageConfigDigest,
		),
	}
	observationValue, err := encodeComponentObservation(predecessorObservation)
	if err != nil {
		t.Fatalf("encode predecessor observation: %v", err)
	}
	seedObservation, err := store.Transact(ctx, nil, []Mutation{{
		Type: MutationPut, Key: componentObservationKey(secondInput.ComponentID), Value: observationValue,
	}})
	if err != nil || !seedObservation.Succeeded {
		t.Fatalf("seed predecessor observation = %#v, %v", seedObservation, err)
	}
	finishedAt := observedAt.Add(time.Second)
	secondRecord.FinishedAt = &finishedAt
	secondRecord.TerminalAssignment = &TaskTerminalAssignmentRecord{
		AssignmentID: ids.NewAt(ids.KindAssignment, now, 10), AgentID: agentID, AgentGeneration: 1,
	}
	terminalResult := platformDNSProof(secondInput, uint64(secondRecord.RenderGeneration), finishedAt)
	ackChange, err := tasks.preparePlatformComponentTaskAcknowledgement(
		ctx, secondRecord, TaskStatusCompleted, &terminalResult, seedObservation.Revision,
	)
	if err != nil || !ackChange.applies {
		t.Fatalf("successor acknowledgement after predecessor replacement = %#v, %v", ackChange, err)
	}
	clearPlatformComponentTaskChange(ackChange)

	wrongFence, err := store.Transact(ctx, []Condition{{
		Key: platformComponentTaskActiveKey(current.Record.Desired.ID), ModRevision: updated.Entry.ModRevision,
	}}, []Mutation{{
		Type: MutationPut, Key: platformComponentTaskActiveKey(current.Record.Desired.ID), Value: []byte(first.ID),
	}})
	if err != nil || !wrongFence.Succeeded {
		t.Fatalf("replace active fence = %#v, %v", wrongFence, err)
	}
	if _, err := tasks.preparePlatformComponentTaskAcknowledgement(
		ctx, secondRecord, TaskStatusCompleted, &terminalResult, wrongFence.Revision,
	); err == nil {
		t.Fatal("automatic resolver acknowledgement accepted mismatched active-fence ownership")
	}
	restoredFence, err := store.Transact(ctx, []Condition{{
		Key: platformComponentTaskActiveKey(current.Record.Desired.ID), ModRevision: wrongFence.Revision,
	}}, []Mutation{{
		Type: MutationPut, Key: platformComponentTaskActiveKey(current.Record.Desired.ID), Value: []byte(second.ID),
	}})
	if err != nil || !restoredFence.Succeeded {
		t.Fatalf("restore active fence = %#v, %v", restoredFence, err)
	}
	updated.Entry.ModRevision = restoredFence.Revision
	if _, err := store.Transact(ctx, []Condition{{
		Key: platformComponentTaskActiveKey(current.Record.Desired.ID), ModRevision: updated.Entry.ModRevision,
	}}, []Mutation{{Type: MutationDelete, Key: platformComponentTaskActiveKey(current.Record.Desired.ID)}}); err != nil {
		t.Fatalf("clear terminal resolver fence: %v", err)
	}
	sourceRead, err := store.Get(ctx, taskKey(second.ID))
	if err != nil || sourceRead.Entry == nil {
		t.Fatalf("retry source resolver Task = %#v, %v", sourceRead, err)
	}
	source, err := decodeTaskRecord(sourceRead.Entry.Value)
	if err != nil {
		t.Fatalf("decode retry source resolver Task: %v", err)
	}
	source.Actor = TaskActorOperator
	delete(source.Params, TaskAutomaticReconcileParam)
	retry := cloneTaskRecord(source)
	retry.ID = ids.NewAt(ids.KindTask, now, 4)
	retry.RetryOf = source.ID
	retry.Actor = TaskActorOperator
	retryChange, err := tasks.preparePlatformDNSResolverTaskRetry(ctx, Versioned[TaskRecord]{
		Record: source, Revision: sourceRead.Entry.ModRevision, ReadRevision: sourceRead.ReadRevision,
	}, retry)
	if err != nil || !retryChange.applies {
		t.Fatalf("prepare resolver retry = %#v, %v", retryChange, err)
	}
	if len(retry.Steps) != 2 {
		t.Fatalf("resolver retry steps = %#v", retry.Steps)
	}
	if result, err := store.Transact(
		ctx,
		retryChange.conditions,
		retryChange.mutations,
	); err != nil ||
		!result.Succeeded {
		t.Fatalf("resolver retry fence transaction = %#v, %v", result, err)
	}
	clearHostResolutionReconciliationChange(retryChange)
	retried, err := store.Get(ctx, platformComponentTaskActiveKey(current.Record.Desired.ID))
	if err != nil || retried.Entry == nil || string(retried.Entry.Value) != retry.ID {
		t.Fatalf("active resolver retry = %#v, %v", retried, err)
	}
}
