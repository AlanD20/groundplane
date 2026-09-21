package etcd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestBlueprintScriptCheckpointAuthorityRequiresExactPublishedOperation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	now := time.Date(2026, 9, 2, 20, 0, 0, 0, time.UTC)
	store := newMemoryTaskStore()
	repository := composeScriptRepository(store)
	execution := scriptCheckpointTestRecord(now)
	publicationID := ids.NewULID()
	task := releaseHookRetryTestTask(execution, execution.CurrentTaskID, now)
	task.Params[testreleaserender.TaskReleasePublicationParam] = publicationID
	task.Params[testtaskjournal.TaskMaterializationEnvironmentParam] = execution.EnvironmentID
	task.Params[testblueprints.EnvironmentDesiredRevisionParam] = task.ID
	tenantID := ids.NewAt(ids.KindTenant, now, 30)
	projectID := ids.NewAt(ids.KindProject, now, 31)
	task.Owner = testtaskjournal.TaskOwner{
		WorkspaceType: testtaskjournal.TaskWorkspaceTenant, TenantID: tenantID,
		ProjectID: projectID, EnvironmentID: execution.EnvironmentID,
	}
	intent := domain.Intent{
		ID: execution.ReleaseID, EnvironmentID: execution.EnvironmentID,
		ServiceID: execution.ServiceID, OperationID: execution.OperationID,
		OperationKind:    domain.OperationBlueprintApply,
		GroupOperationID: execution.OperationID, GroupMemberOrdinal: 1,
		CandidateWorkload: releaseTestWorkloadSeal("docker.io/library/nginx@sha256:" + strings.Repeat("d", 64)),
		Tag:               "stable", Strategy: domain.StrategyRecreate,
		OnFailure:         domain.OnFailureSwitchBack,
		RenderInputID:     ids.NewAt(ids.KindConfig, now, 32),
		RenderInputDigest: strings.Repeat("e", 64),
		CreatedAt:         now, Actor: "operator", OriginatingTaskID: task.ID,
		Workspace: domain.Workspace{
			Kind: domain.WorkspaceTenant, TenantID: tenantID,
			ProjectID: projectID, EnvironmentID: execution.EnvironmentID,
		},
	}
	if err := domain.ValidateIntent(intent); err != nil {
		t.Fatal(err)
	}
	intentDigest, _ := domain.Digest(intent)
	member := testreleases.ReleaseStagedMemberRef{
		ReleaseID: execution.ReleaseID, ServiceID: execution.ServiceID,
		IntentDigest: intentDigest, RenderDigest: strings.Repeat("b", 64),
		CheckpointDigest: strings.Repeat("c", 64),
	}
	manifest := testreleases.ReleaseStagedManifest{
		PublicationID: publicationID, OperationID: execution.OperationID,
		Members: []testreleases.ReleaseStagedMemberRef{member}, CreatedAt: now,
	}
	manifest.Digest, _ = blueprintCandidateManifestDigest(manifest)
	marker := testreleases.ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: execution.OperationID,
		ManifestDigest: manifest.Digest, PublishedAt: now,
	}
	manifestValue, err := testreleases.EncodeReleaseRecord("release-staged-manifest", manifest)
	if err != nil {
		t.Fatal(err)
	}
	markerValue, err := testreleases.EncodeReleaseRecord("release-publication", marker)
	if err != nil {
		t.Fatal(err)
	}
	intentValue, err := testreleases.EncodeReleaseRecord("release-intent", intent)
	if err != nil {
		t.Fatal(err)
	}
	seeded, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseManifestStagingKey(publicationID),
			Value: manifestValue,
		},
		{Type: testkeyvalue.MutationPut, Key: testreleases.ReleasePublicationKey(publicationID), Value: markerValue},
		{
			Type:  testkeyvalue.MutationPut,
			Key:   testreleases.ReleaseIntentStagingKey(publicationID, execution.ReleaseID),
			Value: intentValue,
		},
	})
	if err != nil || !seeded.Succeeded {
		t.Fatalf("seed Blueprint Script authority = %#v, %v", seeded, err)
	}
	if err := repository.validateBlueprintScriptExecutionAuthority(ctx, task, execution, seeded.Revision); err != nil {
		t.Fatalf("exact Blueprint Script authority error = %v", err)
	}
	ordinary := task
	ordinary.Params = map[string]string{testreleaserender.ReleaseHookStepExecutionParam(execution.StepID): execution.ID}
	if err := repository.validateBlueprintScriptExecutionAuthority(ctx, ordinary, execution, seeded.Revision); err == nil {
		t.Fatal("ordinary update gained Blueprint Script authority")
	}
	mismatched := execution
	mismatched.OperationID = ids.NewAt(ids.KindOperation, now, 99)
	if err := repository.validateBlueprintScriptExecutionAuthority(ctx, task, mismatched, seeded.Revision); err == nil {
		t.Fatal("mismatched operation gained Blueprint Script authority")
	}
}

func TestBlueprintCompensationRequiresExactStrategyPredecessorEvidence(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 20, 30, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, now, 1)
	priorReleaseID := ids.NewAt(ids.KindDeployment, now, 2)
	priorArtifactID := ids.NewAt(ids.KindConfig, now, 3)
	intent := domain.Intent{
		ServiceID: serviceID, Strategy: domain.StrategyRecreate,
		PriorServingReleaseID: priorReleaseID, PriorSuccessfulReleaseID: priorReleaseID,
	}
	render := testreleaserender.ReleaseRenderInput{
		ServiceID: serviceID, Strategy: domain.StrategyRecreate,
		PriorArtifactID: priorArtifactID, PriorTarget: domain.WorkloadSingleton,
	}
	exact := testtaskjournal.TaskResultRecord{RecreateEvidence: []testtaskjournal.TaskRecreateEvidence{{
		ServiceID: serviceID, ReleaseID: priorReleaseID, ArtifactID: priorArtifactID,
		Target: string(domain.WorkloadSingleton), Compensated: true,
	}}}
	if err := validateBlueprintCandidateCompensation(intent, render, exact); err != nil {
		t.Fatalf("exact recreate compensation error = %v", err)
	}
	for name, result := range map[string]testtaskjournal.TaskResultRecord{
		"missing": {},
		"wrong artifact": {RecreateEvidence: []testtaskjournal.TaskRecreateEvidence{{
			ServiceID: serviceID, ReleaseID: priorReleaseID,
			ArtifactID: ids.NewAt(ids.KindConfig, now, 4),
			Target:     string(domain.WorkloadSingleton), Compensated: true,
		}}},
		"wrong strategy": {ProxyEvidence: []testtaskjournal.TaskProxyEvidence{{
			ServiceID: serviceID, ReleaseID: priorReleaseID,
			Target: string(domain.WorkloadSingleton), Compensated: true,
		}}},
	} {
		if err := validateBlueprintCandidateCompensation(intent, render, result); !errors.Is(
			err, errs.New(errs.KindReleaseRecoveryRequired, ""),
		) {
			t.Errorf("%s error = %v, want release.recovery_required", name, err)
		}
	}

	intent.Strategy = domain.StrategyBlueGreen
	render.Strategy = domain.StrategyBlueGreen
	render.PriorTarget = domain.WorkloadBlue
	render.PriorProxyGeneration = 7
	render.PriorProxyDigest = strings.Repeat("d", 64)
	blueGreen := testtaskjournal.TaskResultRecord{ProxyEvidence: []testtaskjournal.TaskProxyEvidence{{
		ServiceID: serviceID, ReleaseID: priorReleaseID, Target: string(domain.WorkloadBlue),
		ProxyGeneration: 7, ConfigSHA256: render.PriorProxyDigest, Compensated: true,
	}}}
	if err := validateBlueprintCandidateCompensation(intent, render, blueGreen); err != nil {
		t.Fatalf("exact blue-green compensation error = %v", err)
	}
}

func TestBlueprintCandidateManifestAcceptsDocumentedMemberLimitAndRejectsDrift(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 2, 21, 0, 0, 0, time.UTC)
	publicationID := ids.NewULID()
	operationID := ids.NewAt(ids.KindOperation, now, 1)
	task := TaskRecord{
		OperationID: operationID,
		Params:      map[string]string{testreleaserender.TaskReleasePublicationParam: publicationID},
	}
	manifest := testreleases.ReleaseStagedManifest{
		PublicationID: publicationID, OperationID: operationID, CreatedAt: now,
		Members: make([]testreleases.ReleaseStagedMemberRef, testreleases.MaximumReleasePublicationMembers),
	}
	for index := range manifest.Members {
		manifest.Members[index] = testreleases.ReleaseStagedMemberRef{
			ReleaseID:    ids.NewAt(ids.KindDeployment, now, int64(index+10)),
			ServiceID:    ids.NewAt(ids.KindService, now, int64(index+100)),
			IntentDigest: strings.Repeat("a", 64), RenderDigest: strings.Repeat("b", 64),
			CheckpointDigest: strings.Repeat("c", 64),
		}
	}
	manifest.Digest, _ = blueprintCandidateManifestDigest(manifest)
	marker := testreleases.ReleasePublicationMarker{
		PublicationID: publicationID, OperationID: operationID,
		ManifestDigest: manifest.Digest, PublishedAt: now,
	}
	if err := validateBlueprintCandidateManifest(task, marker, manifest); err != nil {
		t.Fatalf("documented member limit rejected = %v", err)
	}
	manifest.Members[0].ServiceID = ids.NewAt(ids.KindService, now, 999)
	if err := validateBlueprintCandidateManifest(task, marker, manifest); err == nil {
		t.Fatal("manifest membership drift was accepted")
	}
}
