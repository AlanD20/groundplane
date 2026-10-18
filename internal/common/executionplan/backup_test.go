package executionplan

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/backupsecret"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const (
	testBackupProjectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupEnvironmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupConnectorID   = "con_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupSourceID      = "spt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	testBackupPointID       = "rp_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: a private machine plan must preserve the captured Config source,
// immutable revisions, object target, encryption decision and absolute step
// authority without reconstructing any of them at assignment time.
func TestSealAndValidateBackupSourcePlan(t *testing.T) {
	plan := validBackupPlan(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(Backup) error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate(Backup) error = %v", err)
	}
}

// Rationale: the typed source and resource authority are one closed pair;
// accepting Config execution for a Volume resource would authorize the wrong
// compiled capture implementation.
func TestSealRejectsMismatchedBackupSourceResource(t *testing.T) {
	plan := validBackupPlan(t)
	step := plan.Steps[0].GetBackupStep()
	step.GetCapture().Resource.Kind = agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_VOLUME
	resealBackupExecutionStep(t, plan.Steps[0])
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(mismatched Backup source resource) error = nil")
	}
}

// Rationale: one consumer may appear only once in a step authority; duplicate
// identities could otherwise repeat stop/restart effects.
func TestSealRejectsDuplicateBackupConsumers(t *testing.T) {
	plan := validBackupPlan(t)
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	plan.BackupScope.Services = []*agentpb.BackupServiceFact{validBackupServiceFactFixture(serviceID)}
	plan.Steps[0].GetBackupStep().ConsumerServiceIds = []string{serviceID, serviceID}
	resealBackupExecutionStep(t, plan.Steps[0])
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(duplicate Backup consumers) error = nil")
	}
}

// Rationale: Config is the Environment-wide source and cannot name another
// Environment even when that target id is otherwise canonical.
func TestSealRejectsBackupConfigTargetOutsidePlanEnvironment(t *testing.T) {
	plan := validBackupPlan(t)
	capture := plan.Steps[0].GetBackupStep().GetCapture()
	capture.Resource.ResourceId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	capture.GetConfig().EnvironmentId = capture.Resource.ResourceId
	resealBackupExecutionStep(t, plan.Steps[0])
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(config target outside plan Environment) error = nil")
	}
}

// Rationale: the public recipient is represented only by its immutable digest;
// a malformed digest cannot enter a sealed age-encrypted capture authority.
func TestSealRejectsMalformedAgeRecipientDigest(t *testing.T) {
	plan := validBackupPlan(t)
	step := plan.Steps[0]
	step.GetBackupStep().GetCapture().Encryption.RecipientSha256 = bytes.Repeat([]byte{1}, sha256.Size-1)
	resealBackupExecutionStep(t, step)
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(malformed age recipient digest) error = nil")
	}
}

// Rationale: Connector and encryption controls are part of the sealed step
// digest; changing a captured policy revision after publication must invalidate
// the plan rather than silently alter the Agent's authority.
func TestSealRejectsMutatedBackupPolicyControls(t *testing.T) {
	plan := validBackupPlan(t)
	plan.Steps[0].GetBackupStep().GetCapture().Target.Connector.Connector.ModRevision++
	if _, err := Seal(plan); err == nil {
		t.Fatal("Seal(mutated Backup policy controls) error = nil")
	}
}

// Rationale: one internal prune Task may delete ordered points from different
// Connectors, while every object remains independently fenced by its own exact
// Connector authority and immutable discriminator.
func TestSealAndValidateBackupPrunePlanAcrossConnectors(t *testing.T) {
	plan := validBackupPrunePlan(t)
	sealed, err := Seal(plan)
	if err != nil {
		t.Fatalf("Seal(Backup prune) error = %v", err)
	}
	if _, err := Validate(sealed); err != nil {
		t.Fatalf("Validate(Backup prune) error = %v", err)
	}
	first := sealed.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector
	second := sealed.Steps[1].GetBackupStep().GetPrune().Objects[0].Object.Connector
	if first.ConnectorId == second.ConnectorId {
		t.Fatal("backup prune fixture did not exercise different Connectors")
	}
}

// Rationale: prune ordering, point uniqueness, immutable revisions, Connector
// decisions, exact object evidence, and bounded step identity all fail closed
// before an Agent can receive the plan.
func TestSealRejectsMalformedBackupPrunePlan(t *testing.T) {
	checks := []struct {
		name   string
		mutate func(*testing.T, *agentpb.ExecutionPlan)
	}{
		{name: "no steps", mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) { plan.Steps = nil }},
		{name: "too many steps", mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) {
			for len(plan.Steps) <= MaximumBackupPrunePoints {
				plan.Steps = append(plan.Steps, plan.Steps[0])
			}
		}},
		{
			name:   "render generation",
			mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) { plan.RenderGeneration = 1 },
		},
		{name: "wrong target", mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) {
			plan.TargetId = "env_01ARZ3NDEKTSV4RRFFQ69G5FAW"
		}},
		{name: "zero step timeout", mutate: func(_ *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].TimeoutSeconds = 0
		}},
		{name: "duplicate point", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			prune := plan.Steps[0].GetBackupStep().GetPrune()
			duplicate := proto.Clone(prune.Objects[0]).(*agentpb.BackupPruneObject)
			duplicate.Ordinal = 2
			prune.Objects = append(prune.Objects, duplicate)
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "zero retention revision", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().RetentionPolicy.ModRevision = 0
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "zero ordinal", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Ordinal = 0
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "bad endpoint", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector.CanonicalEndpointUrl += "?query=forbidden"
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "bad bucket", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Bucket = "INVALID"
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "bad prefix", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector.Prefix = "not-normalized"
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "empty region", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector.Region = ""
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "missing addressing decision", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.Connector.PathStyle = nil
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "wrong object key", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Object.ObjectKey = "another/object"
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "zero stored size", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			plan.Steps[0].GetBackupStep().GetPrune().Objects[0].Evidence.StoredSizeBytes = 0
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
		{name: "short stored digest", mutate: func(t *testing.T, plan *agentpb.ExecutionPlan) {
			object := plan.Steps[0].GetBackupStep().GetPrune().Objects[0]
			object.Evidence.StoredSha256 = object.Evidence.StoredSha256[:sha256.Size-1]
			resealBackupExecutionStep(t, plan.Steps[0])
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			plan := validBackupPrunePlan(t)
			check.mutate(t, plan)
			if _, err := Seal(plan); err == nil {
				t.Fatal("Seal(malformed Backup prune) error = nil")
			}
		})
	}
}

// Rationale: Backup capture and prune are different closed procedures. A plan
// cannot change the operation enum while retaining the other operation's step.
func TestSealRejectsBackupAndPruneStepOperationMismatch(t *testing.T) {
	prune := validBackupPrunePlan(t)
	prune.Operation = agentpb.PlanOperation_PLAN_OPERATION_BACKUP
	if _, err := Seal(prune); err == nil {
		t.Fatal("Seal(Backup operation with prune step) error = nil")
	}
	backup := validBackupPlan(t)
	backup.Operation = agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE
	if _, err := Seal(backup); err == nil {
		t.Fatal("Seal(Backup prune operation with capture step) error = nil")
	}
}

// Rationale: owned checkpoint bytes and an exact durable ACK prevent a caller
// mutation or unrelated receipt from releasing the next external effect.
func TestValidateBackupWorkCheckpointAndAck(t *testing.T) {
	request, input, identity := nativeBackupCheckpoint(t)
	validated, err := ValidateBackupWorkCheckpoint(request, input, identity)
	if err != nil {
		t.Fatal(err)
	}
	validated.Attempt.Input.Sha256[0]++
	if bytes.Equal(validated.Attempt.Input.Sha256, request.Attempt.Input.Sha256) {
		t.Fatal("validated checkpoint aliases captured input")
	}
	ack := nativeBackupCheckpointAck(t, request, input)
	if _, err := ValidateBackupWorkCheckpointAck(ack, request, input, identity); err != nil {
		t.Fatal(err)
	}
}

// Rationale: Config restoration names a Config generation, never a Task.
func TestBackupConfigCheckpointRequiresConfigIdentity(t *testing.T) {
	request, _, _ := nativeBackupCheckpoint(t)
	request.Checkpoint = &agentpb.BackupWorkCheckpointRequest_Config{Config: &agentpb.BackupConfigCheckpoint{
		Checkpoint: &agentpb.BackupConfigCheckpoint_TransferCompleted{
			TransferCompleted: &agentpb.BackupConfigTransferCompleted{
				RestoreGenerationId: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV", Content: testBackupConfigContent(),
				CommittedRecordCount: 4, ValueChainSha256: bytes.Repeat([]byte{26}, sha256.Size),
				TransferTranscriptSha256: bytes.Repeat([]byte{27}, sha256.Size), RenderGeneration: 1,
			},
		},
	}}
	if _, err := BackupWorkCheckpointDigest(request); err != nil {
		t.Fatal(err)
	}
	request.GetConfig().GetTransferCompleted().RestoreGenerationId = request.Attempt.TaskId
	if _, err := BackupWorkCheckpointDigest(request); err == nil {
		t.Fatal("Task accepted as Config generation")
	}
}

// Rationale: replay identity and immutable upload outcome must both affect the
// checkpoint digest, while identical typed reports remain replayable.
func TestBackupWorkCheckpointDigestBindsAttemptAndUploadOutcome(t *testing.T) {
	request, _, _ := nativeBackupCheckpoint(t)
	digest, err := BackupWorkCheckpointDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := BackupWorkCheckpointDigest(proto.CloneOf(request))
	if err != nil || !bytes.Equal(digest, repeated) {
		t.Fatalf("same report changed digest: %v", err)
	}
	other := proto.CloneOf(request)
	other.Attempt.AttemptId = "try_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	changed, err := BackupWorkCheckpointDigest(other)
	if err != nil || bytes.Equal(digest, changed) {
		t.Fatalf("digest failed to bind attempt: %v", err)
	}

	upload, _, _ := nativeBackupUploadCheckpoint(t)
	digest, err = BackupWorkCheckpointDigest(upload)
	if err != nil {
		t.Fatal(err)
	}
	upload.GetUploadCompleted().GetReturnedObject().GetEtag().Value = "different-etag"
	changed, err = BackupWorkCheckpointDigest(upload)
	if err != nil || bytes.Equal(digest, changed) {
		t.Fatalf("digest failed to bind uploaded object: %v", err)
	}
}

// Rationale: malformed orphan evidence cannot be accepted as a committed upload.
func TestValidateBackupWorkCheckpointRejectsMalformedUpload(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*agentpb.BackupWorkCheckpointRequest)
	}{
		{"nil payload", func(r *agentpb.BackupWorkCheckpointRequest) {
			r.Checkpoint = &agentpb.BackupWorkCheckpointRequest_UploadVerified{}
		}},
		{"bad point", func(r *agentpb.BackupWorkCheckpointRequest) { r.GetUploadCompleted().PointId = "rp_invalid" }},
		{
			"zero stored size",
			func(r *agentpb.BackupWorkCheckpointRequest) { r.GetUploadCompleted().Evidence.StoredSizeBytes = 0 },
		},
		{
			"short stored digest",
			func(r *agentpb.BackupWorkCheckpointRequest) { r.GetUploadCompleted().Evidence.StoredSha256 = []byte{1} },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, input, identity := nativeBackupUploadCheckpoint(t)
			if _, err := ValidateBackupWorkCheckpoint(request, input, identity); err != nil {
				t.Fatal(err)
			}
			test.mutate(request)
			if _, err := ValidateBackupWorkCheckpoint(request, input, identity); err == nil {
				t.Fatal("malformed upload accepted")
			}
		})
	}
}

// Rationale: a source report must bind its captured input, authenticated host,
// valid sequence and predecessor. Persisted next-sequence CAS is the repository's
// separate responsibility, not inferred by this resource validator.
func TestValidateBackupWorkCheckpointRejectsForeignOrMalformedReport(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*agentpb.BackupWorkCheckpointRequest)
	}{
		{"zero sequence", func(r *agentpb.BackupWorkCheckpointRequest) { r.CheckpointSequence = 0 }},
		{"uncaptured source", func(r *agentpb.BackupWorkCheckpointRequest) { r.SourceOrdinal = 1 }},
		{
			"wrong host",
			func(r *agentpb.BackupWorkCheckpointRequest) { r.Attempt.HostId = "host_01ARZ3NDEKTSV4RRFFQ69G5FAW" },
		},
		{"changed captured input", func(r *agentpb.BackupWorkCheckpointRequest) { r.Attempt.Input.Sha256[0]++ }},
		{"predecessor on first", func(r *agentpb.BackupWorkCheckpointRequest) {
			r.PrecedingCheckpoint = &agentpb.CheckpointFence{
				AuthorityDigest:      bytes.Repeat([]byte{1}, sha256.Size),
				DedupeKeyModRevision: 1,
			}
		}},
		{"foreign predecessor", func(r *agentpb.BackupWorkCheckpointRequest) {
			r.CheckpointSequence = 2
			r.PrecedingCheckpoint = &agentpb.CheckpointFence{
				AuthorityDigest:      bytes.Repeat([]byte{1}, sha256.Size),
				DedupeKeyModRevision: 1,
			}
		}},
		{"missing predecessor", func(r *agentpb.BackupWorkCheckpointRequest) { r.CheckpointSequence = 2 }},
		{"bad point", func(r *agentpb.BackupWorkCheckpointRequest) { r.GetArtifactPrepared().PointId = "rp_invalid" }},
		{"short artifact digest", func(r *agentpb.BackupWorkCheckpointRequest) {
			r.GetArtifactPrepared().Evidence.StoredSha256 = []byte{1}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, input, identity := nativeBackupCheckpoint(t)
			test.mutate(request)
			if _, err := ValidateBackupWorkCheckpoint(request, input, identity); err == nil {
				t.Fatal("foreign or malformed report accepted")
			}
		})
	}
}

// Rationale: an ACK may release only this exact report and source cursor;
// accepting a valid but unrelated receipt could skip an external effect.
func TestValidateBackupWorkCheckpointAckRejectsDifferentReceipt(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*agentpb.BackupWorkCheckpointAck)
	}{
		{
			"attempt",
			func(a *agentpb.BackupWorkCheckpointAck) { a.Attempt.AttemptId = "try_01ARZ3NDEKTSV4RRFFQ69G5FAW" },
		},
		{"sequence", func(a *agentpb.BackupWorkCheckpointAck) { a.CheckpointSequence++ }},
		{"source ordinal", func(a *agentpb.BackupWorkCheckpointAck) { a.SourceOrdinal++ }},
		{"request digest", func(a *agentpb.BackupWorkCheckpointAck) { a.RequestSha256[0]++ }},
		{"authority", func(a *agentpb.BackupWorkCheckpointAck) { a.Committed.AuthorityDigest[0]++ }},
		{"uncommitted", func(a *agentpb.BackupWorkCheckpointAck) { a.Committed.DedupeKeyModRevision = 0 }},
		{"resume cursor", func(a *agentpb.BackupWorkCheckpointAck) { a.Source.GetCapture().CheckpointSequence++ }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request, input, identity := nativeBackupCheckpoint(t)
			ack := nativeBackupCheckpointAck(t, request, input)
			if _, err := ValidateBackupWorkCheckpointAck(ack, request, input, identity); err != nil {
				t.Fatal(err)
			}
			test.mutate(ack)
			if _, err := ValidateBackupWorkCheckpointAck(ack, request, input, identity); err == nil {
				t.Fatal("different receipt accepted")
			}
		})
	}
}

func validBackupPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	step := sealBackupExecutionStep(t, &agentpb.BackupStepAuthority{
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", ExecutionId: "01ARZ3NDEKTSV4RRFFQ69G5FAV",
		StepDeadlineUnixNano: 1,
		Operation: &agentpb.BackupStepAuthority_Capture{Capture: &agentpb.BackupCaptureAuthority{
			PointId: testBackupPointID,
			Resource: &agentpb.BackupResourceIdentity{
				Kind:       agentpb.BackupResourceKind_BACKUP_RESOURCE_KIND_ENVIRONMENT,
				ResourceId: testBackupEnvironmentID, Resource: testBackupRevision(3, 3),
			},
			Target: testBackupTarget(testBackupConnectorID, testBackupSourceID, testBackupPointID, true),
			Encryption: &agentpb.BackupEncryptionAuthority{
				Kind:            agentpb.BackupEncryption_BACKUP_ENCRYPTION_AGE,
				SecretSlotId:    backupsecret.CurrentAgeIdentitySlotID,
				RecipientSha256: bytes.Repeat([]byte{4}, sha256.Size), SecretSlot: testBackupRevision(4, 4),
				KeyEra: backupUint64(2),
			},
			Source: &agentpb.BackupCaptureAuthority_Config{Config: &agentpb.BackupConfigCaptureAuthority{
				EnvironmentId: testBackupEnvironmentID, Content: testBackupConfigContent(),
				MetadataSnapshotRevision: 8, MetadataEntryCount: 1, MetadataProtoBytes: 128,
			}},
		}},
	})
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: testPlanID,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP, TargetId: testBackupEnvironmentID,
		BackupScope: testBackupScope(), Steps: []*agentpb.ExecutionStep{step},
	}
}

func validBackupPrunePlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	return &agentpb.ExecutionPlan{
		Schema: SchemaVersion, PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BACKUP_PRUNE, TargetId: testBackupEnvironmentID,
		BackupScope: testBackupScope(),
		Steps: []*agentpb.ExecutionStep{
			backupPruneStep(
				t,
				"step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				"01ARZ3NDEKTSV4RRFFQ69G5FAV",
				1,
				testBackupPointID,
				testBackupSourceID,
				testBackupConnectorID,
				"https://objects.example.test",
				"production/",
				true,
			),
			backupPruneStep(t, "step_01ARZ3NDEKTSV4RRFFQ69G5FAW", "01ARZ3NDEKTSV4RRFFQ69G5FAW", 2,
				"rp_01ARZ3NDEKTSV4RRFFQ69G5FAW", "spt_01ARZ3NDEKTSV4RRFFQ69G5FAW",
				"con_01ARZ3NDEKTSV4RRFFQ69G5FAW", "https://archive.example.test", "archive/", false),
		},
	}
}

func backupPruneStep(t *testing.T, stepID, executionID string, ordinal uint32, pointID, sourceID,
	connectorID, endpoint, prefix string, pathStyle bool,
) *agentpb.ExecutionStep {
	t.Helper()
	connector := testBackupConnector(connectorID, endpoint, prefix, pathStyle)
	digest := bytes.Repeat([]byte{byte(ordinal)}, sha256.Size)
	authority := &agentpb.BackupStepAuthority{
		StepId: stepID, ExecutionId: executionID, StepDeadlineUnixNano: 2,
		Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
			RetentionPolicy: testBackupRevision(20, 20),
			Objects: []*agentpb.BackupPruneObject{{
				Ordinal: ordinal, PointId: pointID, Point: testBackupRevision(int64(30+ordinal), byte(30+ordinal)),
				Evidence: &agentpb.BackupArtifactEvidence{
					SourceSizeBytes: 4096, SourceSha256: append([]byte(nil), digest...),
					StoredSizeBytes: 4096, StoredSha256: append([]byte(nil), digest...),
				},
				Object: &agentpb.BackupObjectIdentity{
					Connector: connector, Bucket: "groundplane-backups",
					ObjectKey:     prefix + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
					Discriminator: &agentpb.BackupObjectIdentity_Etag{Etag: &agentpb.BackupS3ETag{Value: "etag"}},
				},
				MetadataCount: 10, MetadataSha256: bytes.Repeat([]byte{9}, sha256.Size),
			}},
		}},
	}
	return sealBackupExecutionStep(t, authority)
}

func sealBackupExecutionStep(t *testing.T, authority *agentpb.BackupStepAuthority) *agentpb.ExecutionStep {
	t.Helper()
	sealed, err := SealBackupStepAuthority(authority)
	if err != nil {
		t.Fatalf("SealBackupStepAuthority() error = %v", err)
	}
	return &agentpb.ExecutionStep{
		StepId: sealed.StepId, TimeoutSeconds: MaximumBackupPruneStepTimeoutSeconds,
		Payload: &agentpb.ExecutionStep_BackupStep{BackupStep: sealed},
	}
}

func resealBackupExecutionStep(t *testing.T, step *agentpb.ExecutionStep) {
	t.Helper()
	authority := step.GetBackupStep()
	authority.StepDigest = nil
	sealed, err := SealBackupStepAuthority(authority)
	if err != nil {
		return
	}
	step.Payload = &agentpb.ExecutionStep_BackupStep{BackupStep: sealed}
}

func testBackupScope() *agentpb.BackupPlanScope {
	return &agentpb.BackupPlanScope{
		ProjectId: testBackupProjectID, Project: testBackupRevision(1, 1),
		EnvironmentId: testBackupEnvironmentID, Environment: testBackupRevision(2, 2), TaskAttempt: 1,
	}
}

func validBackupServiceFactFixture(serviceID string) *agentpb.BackupServiceFact {
	return &agentpb.BackupServiceFact{
		ServiceId: serviceID, CurrentName: "application", Service: testBackupRevision(5, 5),
		Compose: testBackupRevision(6, 6),
		PriorRuntimeIntent: &agentpb.BackupPriorRuntimeIntent{
			Kind:   agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING,
			Intent: testBackupRevision(7, 7),
		},
		RequiredLabelCount: 1, RequiredLabelsSha256: bytes.Repeat([]byte{8}, sha256.Size),
		LocalImageIdSha256: bytes.Repeat([]byte{9}, sha256.Size),
	}
}

func testBackupTarget(connectorID, sourceID, pointID string, pathStyle bool) *agentpb.BackupObjectTarget {
	connector := testBackupConnector(connectorID, "https://objects.example.test", "production/", pathStyle)
	return &agentpb.BackupObjectTarget{
		Connector: connector, Bucket: "groundplane-backups",
		ObjectKey: connector.Prefix + testBackupEnvironmentID + "/" + sourceID + "/" + pointID + "/artifact.bin",
	}
}

func testBackupConnector(connectorID, endpoint, prefix string, pathStyle bool) *agentpb.BackupConnectorAuthority {
	return &agentpb.BackupConnectorAuthority{
		ConnectorId: connectorID, Connector: testBackupRevision(10, 10),
		CanonicalEndpointUrl: endpoint, Region: "auto", PathStyle: backupBool(pathStyle), Prefix: prefix,
		AccessKeySlotId: backupsecret.AccessKeySlotID, SecretKeySlotId: backupsecret.SecretKeySlotID,
		AccessKeySlot: testBackupRevision(11, 11), SecretKeySlot: testBackupRevision(12, 12),
	}
}

func testBackupRevision(revision int64, fill byte) *agentpb.RevisionDigest {
	return &agentpb.RevisionDigest{ModRevision: revision, Sha256: bytes.Repeat([]byte{fill}, sha256.Size)}
}

func testBackupConfigContent() *agentpb.BackupConfigContentAuthority {
	return &agentpb.BackupConfigContentAuthority{
		ManifestSha256: bytes.Repeat([]byte{13}, sha256.Size), EntryCount: 1,
		TotalSelectedValueBytes: 32, ManifestSizeBytes: 128, SourceSizeBytes: 1536,
		MetadataSnapshotSha256: bytes.Repeat([]byte{14}, sha256.Size),
	}
}

func backupBool(value bool) *bool       { return &value }
func backupUint64(value uint64) *uint64 { return &value }
