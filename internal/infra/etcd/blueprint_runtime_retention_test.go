package etcd

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SeedRetainedRollback uses real staging codecs to model the durable native
// deploy B/3 and successful rollback C/2 heads without advancing Blueprint A.
func (fixture *ExecutedArtifactFixture) SeedRetainedRollback(
	t *testing.T,
	original ReleaseRenderInput,
	originalIntent domain.Intent,
) (ReleaseRenderInput, domain.Intent) {
	t.Helper()
	ctx := context.Background()
	priorID := original.ReleaseID
	var result ReleaseRenderInput
	var intent domain.Intent
	for index, replicas := range []uint32{3, 2} {
		result = cloneReleaseRenderInput(original)
		result.ReleaseID, result.PlanID, result.ArtifactID = ids.New(
			ids.KindDeployment,
		), ids.New(
			ids.KindPlan,
		), ids.New(
			ids.KindConfig,
		)
		result.CandidateWorkload.ReplicaCount = replicas
		if len(result.ProxyPorts) > 0 {
			config, err := domain.RenderProxyConfig(
				result.ServiceName,
				result.ReleaseID,
				result.CandidateTarget,
				result.ProxyGeneration,
				result.ProxyPorts,
			)
			if err != nil {
				t.Fatal(err)
			}
			result.ProxyConfigDigest = hex.EncodeToString(config.SHA256[:])
		}
		intent = originalIntent
		intent.ID, intent.OperationID, intent.OriginatingTaskID = result.ReleaseID, ids.New(
			ids.KindOperation,
		), ids.New(
			ids.KindTask,
		)
		intent.RenderInputID, intent.CandidateWorkload = result.ArtifactID, result.CandidateWorkload
		intent.PriorServingReleaseID, intent.OperationKind = priorID, domain.OperationDeploy
		if index == 1 {
			intent.OperationKind, intent.RollbackSourceReleaseID = domain.OperationRollback, original.ReleaseID
		}
		raw, err := EncodeReleaseRenderInput(result)
		if err != nil {
			t.Fatal(err)
		}
		intent.RenderInputDigest, err = domain.Digest(json.RawMessage(raw))
		if err != nil {
			t.Fatal(err)
		}
		publication := ids.NewULID()
		manifest, err := fixture.Ledger.Stage(
			ctx,
			ReleaseStage{
				PublicationID: publication,
				OperationID:   intent.OperationID,
				CreatedAt:     intent.CreatedAt,
				Members: []ReleaseStageMember{
					{
						Intent:      intent,
						RenderInput: raw,
						Checkpoint: domain.Checkpoint{
							ReleaseID: intent.ID,
							State:     domain.StatePending,
							UpdatedAt: intent.CreatedAt,
						},
					},
				},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		marker, _ := encodeReleaseRecord(
			"release-publication",
			ReleasePublicationMarker{
				PublicationID:  publication,
				OperationID:    intent.OperationID,
				ManifestDigest: manifest.Record.Digest,
				PublishedAt:    intent.CreatedAt,
			},
		)
		indexBytes, _ := json.Marshal(releaseServiceIndexValue{Schema: 1, PublicationID: publication})
		projection, _ := encodeReleaseRecord(
			"service-release-projection",
			domain.ServiceProjection{
				EnvironmentID:              intent.EnvironmentID,
				ServiceID:                  intent.ServiceID,
				ServingReleaseID:           intent.ID,
				CurrentSuccessfulReleaseID: intent.ID,
			},
		)
		head, _ := encodeReleaseRecord(
			"release-operation",
			ReleaseOperationHead{
				OperationID:   intent.OperationID,
				PublicationID: publication,
				EnvironmentID: intent.EnvironmentID,
				LatestTaskID:  intent.OriginatingTaskID,
				State:         domain.StateCompleted,
				Attempts: []domain.Attempt{
					{ID: intent.OriginatingTaskID, TaskID: intent.OriginatingTaskID, StartedAt: intent.CreatedAt},
				},
			},
		)
		_, err = fixture.store.Transact(
			ctx,
			nil,
			[]Mutation{
				{Type: MutationPut, Key: releasePublicationKey(publication), Value: marker},
				{
					Type:  MutationPut,
					Key:   releaseServiceIndexKey(intent.EnvironmentID, intent.ServiceID, intent.ID),
					Value: indexBytes,
				},
				{Type: MutationPut, Key: releaseProjectionKey(intent.ServiceID), Value: projection},
				{Type: MutationPut, Key: releaseOperationKey(intent.OperationID), Value: head},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		priorID = intent.ID
	}
	return result, intent
}

func (fixture *ExecutedArtifactFixture) AssertRetainedSourceRaces(
	t *testing.T,
	render ReleaseRenderInput,
	mixed *agentpb.ComposeArtifact,
) {
	t.Helper()
	ctx := context.Background()
	for _, kind := range []string{"intent", "render", "projection", "applied"} {
		for _, prune := range []bool{false, true} {
			scope, err := fixture.Ledger.LoadPlanningScope(ctx, render.EnvironmentID)
			if err != nil {
				t.Fatal(err)
			}
			planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{render.ServiceID})
			if err != nil {
				t.Fatal(err)
			}
			applied, _, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			serving, err := fixture.Ledger.ResolveServing(
				ctx,
				render.EnvironmentID,
				render.ServiceID,
				scope.ReadRevision,
			)
			if err != nil {
				t.Fatal(err)
			}
			current, err := fixture.Ledger.GetReleaseRenderInputAt(ctx, render.ReleaseID, scope.ReadRevision)
			if err != nil {
				t.Fatal(err)
			}
			release := ServiceLifecycleRelease{
				ServingReleaseID:   render.ReleaseID,
				ProjectionRevision: serving.ProjectionRevision,
				IntentRevision:     serving.IntentRevision,
				RenderRevision:     current.Revision,
				Current:            current.Record,
			}
			source := BlueprintRetainedRuntimeSource{Planning: planning[0], Release: &release, Intent: &serving}
			task := fixture.Task(t, 1450)
			publication, err := fixture.Ledger.PrepareBlueprintRuntimeRetention(
				ctx,
				BlueprintReleasePublication{},
				task,
				applied,
				[]BlueprintRetainedRuntimeSource{source},
				mixed,
			)
			if err != nil {
				t.Fatal(err)
			}
			key := map[string]string{"intent": releaseIntentStagingKey("", render.ReleaseID), "render": releaseRenderInputStagingKey("", render.ReleaseID), "projection": releaseProjectionKey(render.ServiceID), "applied": environmentComposeProjectionKey(render.EnvironmentID)}[kind]
			before, err := fixture.store.Get(ctx, key)
			if err != nil || before.Entry == nil {
				t.Fatalf("read %s: %v", kind, err)
			}
			mutation := Mutation{Type: MutationPut, Key: key, Value: before.Entry.Value}
			if prune {
				mutation.Type = MutationDelete
				mutation.Value = nil
			}
			if _, err := fixture.store.Transact(ctx, nil, []Mutation{mutation}); err != nil {
				t.Fatal(err)
			}
			result, err := fixture.store.Transact(ctx, publication.conditions, publication.mutations)
			if err != nil || result.Succeeded {
				t.Fatalf("%s prune=%v escaped retained source CAS: %v", kind, prune, err)
			}
			if _, err := fixture.store.Put(ctx, key, before.Entry.Value); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// Rationale: publication must atomically reject a replaced or pruned inactive
// render, including all mutations already carried by its candidate fragment.
func (fixture *ExecutedArtifactFixture) AssertRetainedInactiveRenderRaces(
	t *testing.T,
	prior, current ReleaseRenderInput,
	mixed *agentpb.ComposeArtifact,
) {
	t.Helper()
	ctx := context.Background()
	for _, render := range []ReleaseRenderInput{prior, current} {
		raw, err := EncodeReleaseRenderInput(render)
		if err != nil {
			t.Fatal(err)
		}
		value, err := encodeReleaseRecord("release-render-input", json.RawMessage(raw))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.store.Put(ctx, releaseRenderInputStagingKey("", render.ReleaseID), value); err != nil {
			t.Fatal(err)
		}
	}
	serving, err := fixture.Ledger.ResolveServing(ctx, current.EnvironmentID, current.ServiceID, 0)
	if err != nil {
		t.Fatal(err)
	}
	serving.Intent.Strategy, serving.Intent.Slot, serving.Intent.CandidateWorkload = current.Strategy, current.Slot, current.CandidateWorkload
	serving.Intent.PriorServingReleaseID = prior.ReleaseID
	raw, err := EncodeReleaseRenderInput(current)
	if err != nil {
		t.Fatal(err)
	}
	serving.Intent.RenderInputDigest, err = domain.Digest(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	value, err := encodeReleaseRecord("release-intent", serving.Intent)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.Put(ctx, releaseIntentStagingKey("", current.ReleaseID), value); err != nil {
		t.Fatal(err)
	}
	for _, prune := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "inactive-render-replaced", true: "inactive-render-pruned"}[prune],
			func(t *testing.T) {
				scope, err := fixture.Ledger.LoadPlanningScope(ctx, current.EnvironmentID)
				if err != nil {
					t.Fatal(err)
				}
				planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{current.ServiceID})
				if err != nil {
					t.Fatal(err)
				}
				applied, _, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
				if err != nil {
					t.Fatal(err)
				}
				serving, err := fixture.Ledger.ResolveServing(
					ctx,
					current.EnvironmentID,
					current.ServiceID,
					scope.ReadRevision,
				)
				if err != nil {
					t.Fatal(err)
				}
				active, err := fixture.Ledger.GetReleaseRenderInputAt(ctx, current.ReleaseID, scope.ReadRevision)
				if err != nil {
					t.Fatal(err)
				}
				inactive, err := fixture.Ledger.GetReleaseRenderInputAt(ctx, prior.ReleaseID, scope.ReadRevision)
				if err != nil {
					t.Fatal(err)
				}
				release := ServiceLifecycleRelease{
					ServingReleaseID:            current.ReleaseID,
					ProjectionRevision:          serving.ProjectionRevision,
					IntentRevision:              serving.IntentRevision,
					RenderRevision:              active.Revision,
					Current:                     active.Record,
					PriorServingReleaseID:       prior.ReleaseID,
					RetainedPrior:               &inactive.Record,
					RetainedPriorRenderRevision: inactive.Revision,
				}
				task := fixture.Task(t, 1480)
				task.Params[TaskReleasePublicationParam] = ids.NewULID()
				firstKey, secondKey := "/v1/test/retained-first/"+task.ID, "/v1/test/retained-second/"+task.ID
				candidate, err := newBlueprintReleasePublication(
					blueprintReleasePublicationInput{
						EnvironmentID: task.Target,
						OperationID:   task.OperationID,
						Conditions:    []Condition{{Key: firstKey}, {Key: secondKey}},
						Mutations: []Mutation{
							{Type: MutationPut, Key: firstKey, Value: []byte("first")},
							{Type: MutationPut, Key: secondKey, Value: []byte("second")},
						},
					},
				)
				if err != nil {
					t.Fatal(err)
				}
				publication, err := fixture.Ledger.PrepareBlueprintRuntimeRetention(
					ctx,
					candidate,
					task,
					applied,
					[]BlueprintRetainedRuntimeSource{{Planning: planning[0], Release: &release, Intent: &serving}},
					mixed,
				)
				if err != nil {
					t.Fatal(err)
				}
				ready, err := fixture.store.Transact(ctx, publication.conditions, nil)
				if err != nil || !ready.Succeeded {
					t.Fatalf("valid BG publication was not ready: %v", err)
				}
				key := releaseRenderInputStagingKey("", prior.ReleaseID)
				before, err := fixture.store.Get(ctx, key)
				if err != nil || before.Entry == nil {
					t.Fatalf("inactive source absent: %v", err)
				}
				mutation := Mutation{Type: MutationPut, Key: key, Value: before.Entry.Value}
				if prune {
					mutation.Type = MutationDelete
					mutation.Value = nil
				}
				if _, err := fixture.store.Transact(ctx, nil, []Mutation{mutation}); err != nil {
					t.Fatal(err)
				}
				result, err := fixture.store.Transact(ctx, publication.conditions, publication.mutations)
				if err != nil || result.Succeeded {
					t.Fatalf("inactive source change published: %v", err)
				}
				for _, target := range []string{firstKey, secondKey} {
					read, err := fixture.store.Get(ctx, target)
					if err != nil || read.Entry != nil {
						t.Fatalf("partial publication at %s: %v", target, err)
					}
				}
				if _, err := fixture.store.Put(ctx, key, before.Entry.Value); err != nil {
					t.Fatal(err)
				}
			},
		)
	}
}

func TestBlueprintRuntimeRetentionExtendsCandidateCompareSet(t *testing.T) {
	for _, mutate := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged", true: "absent-becomes-present"}[mutate], func(t *testing.T) {
			ctx := context.Background()
			fixture := NewExecutedArtifactFixture(t)
			first := fixture.Task(t, 1300)
			projection := environmentBlueprintTestProjection(fixture.Environment.Record.ID, first, 1)
			projection.DesiredZones, projection.DesiredRoutes, projection.Volumes = nil, nil, nil
			projection.DesiredServices[0].Desired.Zones = nil
			artifact := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
				t.Fatal(err)
			}
			artifact.Volumes = nil
			projection.ComposeArtifact, _ = proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
			fixture.Publish(t, first, projection, BlueprintReleasePublication{})
			scope, err := fixture.Ledger.LoadPlanningScope(ctx, fixture.Environment.Record.ID)
			if err != nil {
				t.Fatal(err)
			}
			planning, err := fixture.Ledger.LoadPlanningServices(
				ctx,
				scope,
				[]string{projection.DesiredServices[0].Desired.ID},
			)
			if err != nil {
				t.Fatal(err)
			}
			captured, _, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			task := fixture.Task(t, 1301)
			task.Params[TaskReleasePublicationParam] = ids.NewULID()
			candidateKey := "/v1/test/candidate/" + task.ID
			candidate, err := newBlueprintReleasePublication(
				blueprintReleasePublicationInput{EnvironmentID: task.Target, OperationID: task.OperationID,
					Conditions: []Condition{
						{Key: candidateKey},
					}, Mutations: []Mutation{{Type: MutationPut, Key: candidateKey, Value: []byte("candidate")}}},
			)
			if err != nil {
				t.Fatal(err)
			}
			guarded, err := fixture.Ledger.PrepareBlueprintRuntimeRetention(
				ctx,
				candidate,
				task,
				captured,
				[]BlueprintRetainedRuntimeSource{{Planning: planning[0]}},
				artifact,
			)
			if err != nil {
				t.Fatal(err)
			}
			if guarded.retained != nil || len(candidate.conditions) != 1 || len(guarded.conditions) != 3 ||
				len(guarded.mutations) != 1 {
				t.Fatal("candidate authority was replaced or aliased")
			}
			if err := guarded.validate(task.Target, task); err != nil {
				t.Fatal(err)
			}
			withoutParam := task
			withoutParam.Params = nil
			if guarded.validate(task.Target, withoutParam) == nil {
				t.Fatal("candidate Task parameter requirement was removed")
			}
			if mutate {
				if _, err := fixture.store.Put(ctx, releaseProjectionKey(planning[0].Service.Record.Desired.ID), []byte("present")); err != nil {
					t.Fatal(err)
				}
			}
			result, err := fixture.store.Transact(ctx, guarded.conditions, guarded.mutations)
			if err != nil || result.Succeeded == mutate {
				t.Fatalf("candidate compare outcome: %#v %v", result, err)
			}
		})
	}
}

// Rationale: source-only authority must reach the actual owning publisher and
// compare both acknowledged artifact and absent Release projection atomically.
func TestBlueprintRuntimeRetentionActualPublisherSources(t *testing.T) {
	for _, mutation := range []string{"none", "applied", "applied-existing", "absent-release", "tampered-artifact", "tampered-conditions", "absent-authority"} {
		t.Run(mutation, func(t *testing.T) {
			ctx := context.Background()
			fixture := NewExecutedArtifactFixture(t)
			first := fixture.Task(t, 1100)
			projection := environmentBlueprintTestProjection(fixture.Environment.Record.ID, first, 1)
			projection.DesiredZones, projection.DesiredRoutes, projection.Volumes = nil, nil, nil
			projection.DesiredServices[0].Desired.Zones = nil
			artifact := &agentpb.ComposeArtifact{}
			if err := proto.Unmarshal(projection.ComposeArtifact, artifact); err != nil {
				t.Fatal(err)
			}
			artifact.Volumes = nil
			projection.ComposeArtifact, _ = proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
			fixture.Publish(t, first, projection, BlueprintReleasePublication{})
			if mutation == "applied-existing" {
				value, err := encodeEnvironmentComposeProjection(projection)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.Put(ctx, environmentComposeProjectionKey(fixture.Environment.Record.ID), value); err != nil {
					t.Fatal(err)
				}
			}
			scope, err := fixture.Ledger.LoadPlanningScope(ctx, fixture.Environment.Record.ID)
			if err != nil {
				t.Fatal(err)
			}
			planning, err := fixture.Ledger.LoadPlanningServices(
				ctx,
				scope,
				[]string{projection.DesiredServices[0].Desired.ID},
			)
			if err != nil {
				t.Fatal(err)
			}
			captured, _, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
			if err != nil {
				t.Fatal(err)
			}
			next := fixture.Task(t, 1101)
			next.RenderGeneration = 2
			projection.RevisionID, projection.RenderGeneration = next.ID, 2
			publication, err := fixture.Ledger.PrepareBlueprintRuntimeRetention(
				ctx,
				BlueprintReleasePublication{},
				next,
				captured,
				[]BlueprintRetainedRuntimeSource{{Planning: planning[0]}},
				artifact,
			)
			if err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "applied", "applied-existing":
				value, err := encodeEnvironmentComposeProjection(projection)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.Put(ctx, environmentComposeProjectionKey(fixture.Environment.Record.ID), value); err != nil {
					t.Fatal(err)
				}
			case "absent-release":
				if _, err := fixture.store.Put(ctx, releaseProjectionKey(planning[0].Service.Record.Desired.ID), []byte("changed")); err != nil {
					t.Fatal(err)
				}
			case "tampered-artifact":
				publication.retained.mixedSHA[0] ^= 1
			case "tampered-conditions":
				publication.conditions = publication.conditions[:1]
			case "absent-authority":
				publication = BlueprintReleasePublication{}
				artifact.Services[0].Role = agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON
				artifact.Services[0].ExpectedLabels = []*agentpb.LabelPair{
					{Key: "com.groundplane.plan-id", Value: first.PlanID},
				}
				projection.ComposeArtifact, _ = proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
			}
			result, err := fixture.tryPublish(t, next, projection, publication)
			if mutation == "tampered-artifact" || mutation == "tampered-conditions" || mutation == "absent-authority" {
				if !isKind(err, errs.KindValidationFailed) {
					t.Fatalf("tampered authority accepted: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			outcome, _, conflict, classifyErr := result.Classify()
			if mutation == "none" {
				if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
					t.Fatalf("valid publication rejected: %v %v %v", outcome, conflict, classifyErr)
				}
			} else if classifyErr != nil || !isKind(conflict, errs.KindStateConflict) || outcome == IdempotencyKnownApplied {
				t.Fatalf("changed source accepted: %v %v %v", outcome, conflict, classifyErr)
			}
		})
	}
}

func TestBlueprintRuntimeRetentionAuthorityDetectorUsesPhysicalRoles(t *testing.T) {
	task := TaskRecord{PlanID: "current", RenderGeneration: 2}
	for _, role := range []agentpb.ComposeServiceRole{
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED,
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT,
		agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
	} {
		t.Run(role.String(), func(t *testing.T) {
			artifact := &agentpb.ComposeArtifact{
				Services: []*agentpb.ComposeService{
					{
						Role: role,
						ExpectedLabels: []*agentpb.LabelPair{
							{Key: "com.groundplane.plan-id", Value: "prior"},
							{Key: "com.groundplane.render-generation", Value: "1"},
						},
					},
				},
			}
			value, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
			if err != nil {
				t.Fatal(err)
			}
			err = (BlueprintReleasePublication{}).validateRetainedRuntime(
				task,
				EnvironmentComposeProjection{ComposeArtifact: value},
			)
			if role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
				if err != nil {
					t.Fatalf("roleless desired artifact misclassified: %v", err)
				}
			} else if !isKind(err, errs.KindValidationFailed) || !strings.Contains(err.Error(), "source authority is absent") {
				t.Fatalf("physical authority absence not rejected: %v", err)
			}
		})
	}
}
