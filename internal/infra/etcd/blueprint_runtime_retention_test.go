package etcd

import (
	"context"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

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
			task.Params[testreleaserender.TaskReleasePublicationParam] = ids.NewULID()
			candidateKey := "/v1/test/candidate/" + task.ID
			candidate, err := newBlueprintReleasePublication(
				blueprintReleasePublicationInput{EnvironmentID: task.Target, OperationID: task.OperationID,
					Conditions: []testkeyvalue.Condition{
						{Key: candidateKey},
					}, Mutations: []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: candidateKey, Value: []byte("candidate")}}},
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
				if _, err := fixture.store.Put(ctx, testreleases.ReleaseProjectionKey(planning[0].Service.Record.Desired.ID), []byte("present")); err != nil {
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

// SVC-15/BP-04: Rationale: source-only authority must reach the actual owning publisher and
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
				value, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.Put(ctx, testenvironmentprojection.EnvironmentComposeProjectionStorageKey(fixture.Environment.Record.ID), value); err != nil {
					t.Fatal(err)
				}
				seedTestRuntimeConfigurationHead(
					t,
					fixture.store.memoryHierarchyStore,
					fixture.Environment.Record.ID,
					projection.RenderGeneration,
				)
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
				value, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(projection)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := fixture.store.Put(ctx, testenvironmentprojection.EnvironmentComposeProjectionStorageKey(fixture.Environment.Record.ID), value); err != nil {
					t.Fatal(err)
				}
				seedTestRuntimeConfigurationHead(
					t,
					fixture.store.memoryHierarchyStore,
					fixture.Environment.Record.ID,
					projection.RenderGeneration,
				)
			case "absent-release":
				if _, err := fixture.store.Put(ctx, testreleases.ReleaseProjectionKey(planning[0].Service.Record.Desired.ID), []byte("changed")); err != nil {
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
				task, testenvironmentprojection.EnvironmentComposeProjection{ComposeArtifact: value},
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
