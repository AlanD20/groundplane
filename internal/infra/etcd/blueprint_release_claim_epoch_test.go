package etcd

import (
	bytes "bytes"
	context "context"
	sha256 "crypto/sha256"
	errors "errors"
	executionplan "github.com/AlanD20/groundplane/internal/common/executionplan"
	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
	agentpb "github.com/AlanD20/groundplane/proto/agentpb"
	proto "google.golang.org/protobuf/proto"
	strings "strings"
	testing "testing"
	time "time"
)

// Rationale: an older pending reconciliation may lawfully advance the applied projection and
// Environment epoch after candidate publication, while an unrelated epoch rewrite remains drift.
func TestBlueprintCandidateClaimEpochAcceptsOnlyValidatedAppliedPredecessor(t *testing.T) {
	for _, test := range []struct {
		name                      string
		advanceAppliedPredecessor bool
		wantClaim                 bool
	}{
		{name: "applied predecessor and epoch", advanceAppliedPredecessor: true, wantClaim: true},
		{name: "epoch only", wantClaim: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newMemoryTaskStore()
			repository, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 9, 4, 18, 0, 0, 0, time.UTC)
			tenantID := ids.NewAt(ids.KindTenant, now, 801)
			projectID := ids.NewAt(ids.KindProject, now, 802)
			environmentID := ids.NewAt(ids.KindEnvironment, now, 803)
			initialTaskID := ids.NewAt(ids.KindTask, now, 804)
			intermediateTaskID := ids.NewAt(ids.KindTask, now, 805)
			publicationID := ids.NewULID()

			initialProjection := withTestEnvironmentComposeArtifact(
				testenvironmentprojection.EnvironmentComposeProjection{
					EnvironmentID: environmentID, RevisionID: initialTaskID, RenderGeneration: 1,
				},
			)
			initialValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(initialProjection)
			if err != nil {
				t.Fatal(err)
			}
			initial, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{{
				Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID), Value: initialValue,
			}})
			if err != nil || !initial.Succeeded {
				t.Fatalf("seed lower applied projection = %#v, %v", initial, err)
			}

			task := materializationLifecycleTask(now.Add(time.Second), environmentID, 3)
			task.Owner = testtaskjournal.TaskOwner{
				WorkspaceType: testtaskjournal.TaskWorkspaceTenant, TenantID: tenantID,
				ProjectID: projectID, EnvironmentID: environmentID,
			}
			task.Params[testreleaserender.TaskReleasePublicationParam] = publicationID
			task.Params[testblueprints.EnvironmentDesiredRevisionParam] = task.ID
			artifactID := ids.NewAt(ids.KindConfig, now, 807)
			serviceID := ids.NewAt(ids.KindService, now, 808)
			releaseID := ids.NewAt(ids.KindDeployment, now, 809)
			probeStepID := ids.NewAt(ids.KindStep, now, 810)
			compensateStepID := ids.NewAt(ids.KindStep, now, 811)
			task.Params[testtaskjournal.TaskComposeArtifactParam] = artifactID
			task.Steps = append(
				task.Steps,
				testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: probeStepID},
				testtaskjournal.TaskStepRecord{Kind: testtaskjournal.TaskStepOperation, ID: compensateStepID},
			)
			procedure, err := executionplan.BuildCandidateReleaseProcedure(executionplan.CandidateReleaseProcedureInput{
				Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
				Members: []executionplan.CandidateReleaseMemberInput{{
					ServiceID: serviceID, CandidateReleaseID: releaseID, CandidateArtifactID: artifactID,
					ForwardStepIDs: []string{task.Steps[0].ID},
					ServingPredecessor: &executionplan.ServingPredecessorInput{
						ProbeStepID: probeStepID, CompensateStepID: compensateStepID,
					},
					CandidateAbsence: &executionplan.CandidateAbsenceInput{
						ComposeProjectName: "gp-" + environmentID,
						ProbeStepID:        probeStepID, CompensateStepID: compensateStepID,
						Services: []executionplan.CandidateServiceIdentity{
							{ServiceID: serviceID, ReleaseID: releaseID},
						},
					},
				}},
			})
			if err != nil {
				t.Fatal(err)
			}
			procedureBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(procedure)
			if err != nil {
				t.Fatal(err)
			}
			descriptor := executionplan.CandidateReleaseDescriptor{
				PlanID: task.PlanID, PlanHash: bytes.Repeat([]byte{0xaa}, sha256.Size),
				Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, ProcedureBytes: procedureBytes,
			}
			manifest := testreleases.ReleaseStagedManifest{
				PublicationID: publicationID, OperationID: task.OperationID, CreatedAt: now,
				Members: []testreleases.ReleaseStagedMemberRef{{
					ReleaseID: releaseID, ServiceID: serviceID,
					IntentDigest: strings.Repeat("b", 64), RenderDigest: strings.Repeat("c", 64),
					CheckpointDigest: strings.Repeat("d", 64),
				}},
			}
			manifest.Digest, err = blueprintCandidateManifestDigest(manifest)
			if err != nil {
				t.Fatal(err)
			}
			seedBlueprintRequirementTask(t, store, task)

			markerValue, err := testreleases.EncodeReleaseRecord(
				"release-publication",
				testreleases.ReleasePublicationMarker{
					PublicationID: publicationID, OperationID: task.OperationID,
					ManifestDigest: manifest.Digest, CandidateReleaseDescriptor: descriptor, PublishedAt: now,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			manifestValue, err := testreleases.EncodeReleaseRecord("release-staged-manifest", manifest)
			if err != nil {
				t.Fatal(err)
			}
			epochValue, err := testbackupruntime.EncodeEnvironmentMutationEpochRecord(
				testbackupruntime.EnvironmentMutationEpochRecord{
					EnvironmentID: environmentID,
				},
			)
			if err != nil {
				t.Fatal(err)
			}
			published, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
				{
					Type:  testkeyvalue.MutationPut,
					Key:   testreleases.ReleasePublicationKey(publicationID),
					Value: markerValue,
				},
				{
					Type:  testkeyvalue.MutationPut,
					Key:   testreleases.ReleaseManifestStagingKey(publicationID),
					Value: manifestValue,
				},
				{
					Type:  testkeyvalue.MutationPut,
					Key:   testhierarchy.EnvironmentMutationEpochKey(environmentID),
					Value: epochValue,
				},
			})
			if err != nil || !published.Succeeded {
				t.Fatalf("publish pending Blueprint candidate = %#v, %v", published, err)
			}

			intermediateProjection := initialProjection
			intermediateProjection.RevisionID = intermediateTaskID
			intermediateProjection.RenderGeneration = 2
			intermediateValue, err := testenvironmentprojection.EncodeEnvironmentComposeProjectionStorage(
				intermediateProjection,
			)
			if err != nil {
				t.Fatal(err)
			}
			mutations := []testkeyvalue.Mutation{{
				Type: testkeyvalue.MutationPut, Key: testhierarchy.EnvironmentMutationEpochKey(environmentID), Value: epochValue,
			}}
			if test.advanceAppliedPredecessor {
				mutations = append(mutations, testkeyvalue.Mutation{
					Type: testkeyvalue.MutationPut, Key: testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID), Value: intermediateValue,
				})
			}
			advanced, err := store.Transact(ctx, nil, mutations)
			if err != nil || !advanced.Succeeded || advanced.Revision == published.Revision {
				t.Fatalf("advance intermediate applied state = %#v, %v", advanced, err)
			}

			claim, found, claimErr := repository.ClaimNextTask(
				ctx, ids.NewAt(ids.KindAgent, now, 806), 1, now.Add(2*time.Second),
			)
			if !test.wantClaim {
				if claimErr == nil || found || !errors.Is(claimErr, errs.New(errs.KindStateConflict, "")) {
					t.Fatalf("claim after epoch-only rewrite = %#v, %t, %v", claim, found, claimErr)
				}
				pending, getErr := repository.GetTask(ctx, task.ID)
				if getErr != nil || pending.Record.Status != testtaskjournal.TaskStatusPending {
					t.Fatalf("rejected Blueprint claim = %#v, %v", pending, getErr)
				}
				return
			}
			if claimErr != nil || !found || claim.Task.Record.ID != task.ID {
				t.Fatalf("claim after applied predecessor advance = %#v, %t, %v", claim, found, claimErr)
			}
			authority, err := store.GetMany(
				ctx,
				testkeyvalue.GetManyRequest{
					Keys: []string{
						testtaskjournal.TaskMaterializationWriterKey(environmentID),
						testhierarchy.EnvironmentMutationEpochKey(environmentID),
						testenvironmentprojection.EnvironmentComposeProjectionStorageKey(environmentID),
					},
				},
			)
			if err != nil || authority.Values[0] == nil || authority.Values[1] == nil || authority.Values[2] == nil ||
				authority.Values[0].ModRevision != claim.Assignment.Revision ||
				authority.Values[1].ModRevision != claim.Assignment.Revision ||
				authority.Values[2].ModRevision != advanced.Revision {
				t.Fatalf("claimed Blueprint authority = %#v, %v", authority, err)
			}
			writer, err := decodeTaskMaterializationWriter(authority.Values[0].Value)
			if err != nil || writer.BlueprintAppliedPredecessor == nil ||
				*writer.BlueprintAppliedPredecessor != (taskMaterializationAppliedPredecessor{
					Present: true, KeyRevision: advanced.Revision,
					RevisionID: intermediateTaskID, RenderGeneration: 2,
				}) {
				t.Fatalf("claimed applied predecessor writer = %#v, %v", writer, err)
			}
		})
	}
}
