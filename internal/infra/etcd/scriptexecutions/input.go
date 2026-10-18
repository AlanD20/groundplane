package scriptexecutions

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// The input digest excludes mutable attempt/checkpoint/source-retention state.
// A Retry keeps this exact operation input instead of copying an ExecutionPlan.
type capturedInput struct {
	TaskID              string `json:"task_id"`
	WorkItemID          string `json:"work_item_id"`
	OperationID         string `json:"operation_id"`
	PlanID              string `json:"plan_id"`
	ScriptSetGeneration string `json:"script_set_generation"`
	Snapshot            []byte `json:"snapshot"`
	Projection          []byte `json:"projection"`
	Run                 []byte `json:"run"`
	Body                []byte `json:"body"`
}

func capturedInputDigest(record ScriptExecutionRecord) (string, error) {
	value, err := recordcodec.Encode("script-work-input", capturedInput{
		TaskID: record.CurrentTaskID, WorkItemID: record.WorkItemID, OperationID: record.OperationID,
		PlanID: record.Input.ID, ScriptSetGeneration: record.ScriptSetGeneration,
		Snapshot: record.Snapshot, Projection: record.RunnerProjection, Run: record.Run, Body: record.BodyMetadata,
	})
	if err != nil {
		return "", err
	}
	defer clear(value)
	if len(value) > keyvalue.MaximumBytes {
		return "", errs.New(errs.KindValidationFailed, "Script input exceeds the storage size limit")
	}
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:]), nil
}

func validateCapturedInput(record ScriptExecutionRecord) error {
	if len(record.Snapshot) == 0 || len(record.RunnerProjection) == 0 || len(record.Run) == 0 || len(record.BodyMetadata) == 0 ||
		len(record.Snapshot)+len(record.RunnerProjection)+len(record.Run)+len(record.BodyMetadata) > keyvalue.MaximumBytes {
		return errs.New(errs.KindValidationFailed, "Script captured input is missing or too large")
	}
	var snapshot agentpb.ResolvedRunnerSnapshot
	var projection agentpb.ScriptRunnerProjection
	var run agentpb.RunScript
	var body agentpb.ScriptBodyArtifactMetadata
	decode := proto.UnmarshalOptions{DiscardUnknown: false}
	if decode.Unmarshal(record.Snapshot, &snapshot) != nil || decode.Unmarshal(record.RunnerProjection, &projection) != nil ||
		decode.Unmarshal(record.Run, &run) != nil || decode.Unmarshal(record.BodyMetadata, &body) != nil {
		return errs.New(errs.KindValidationFailed, "Script captured input is invalid")
	}
	snapshotDigest := sha256.Sum256(record.Snapshot)
	projectionDigest := sha256.Sum256(record.RunnerProjection)
	if snapshot.SnapshotId != record.SnapshotID || snapshot.ScriptExecutionId != record.ID ||
		snapshot.EnvironmentId != record.EnvironmentID || snapshot.ServiceId != record.ServiceID ||
		snapshot.ReleaseId != record.ReleaseID || snapshot.RenderGeneration != record.RenderGeneration ||
		projection.SnapshotId != record.SnapshotID || projection.Image != snapshot.LocalImageId ||
		run.ScriptExecutionId != record.ID || run.RunnerSnapshotId != record.SnapshotID ||
		run.ScriptId != record.ScriptID || run.ScriptGeneration != record.ScriptGeneration ||
		run.EnvironmentId != record.EnvironmentID || run.ServiceId != record.ServiceID ||
		run.ReleaseId != record.ReleaseID || run.RenderGeneration != record.RenderGeneration ||
		body.ScriptExecutionId != record.ID || body.ScriptId != record.ScriptID || body.Generation != record.ScriptGeneration ||
		body.Uid != projection.Uid || body.Gid != projection.Gid ||
		!bytes.Equal(run.BodySha256, body.Sha256) || hex.EncodeToString(body.Sha256) != record.BodySHA256 ||
		!bytes.Equal(run.ServiceDefinitionSha256, snapshot.ServiceDefinitionSha256) ||
		!bytes.Equal(run.RunnerSnapshotSha256, snapshotDigest[:]) || hex.EncodeToString(snapshotDigest[:]) != record.SnapshotSHA256 ||
		!bytes.Equal(snapshot.RunnerProjectionSha256, projectionDigest[:]) ||
		hex.EncodeToString(projectionDigest[:]) != record.RunnerProjectionSHA256 {
		return errs.New(errs.KindValidationFailed, "Script captured input identities or digests disagree")
	}
	digest, err := capturedInputDigest(record)
	if err != nil {
		return err
	}
	if digest != record.Input.SHA256 {
		return errs.New(errs.KindValidationFailed, "Script captured input digest changed")
	}
	return nil
}

func (record *ScriptExecutionRecord) Clear() {
	clear(record.Snapshot)
	clear(record.RunnerProjection)
	clear(record.Run)
	clear(record.BodyMetadata)
}
