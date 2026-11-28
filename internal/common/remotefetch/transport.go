// Package remotefetch is the closed, credential-free Agent image Fetch wire contract.
package remotefetch

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/common/workloadimage"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"github.com/distribution/reference"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const Kind = "remote-image-fetch"

// Reference admits an explicit digest from the supported public registries.
func Reference(value string) (string, error) {
	named, err := imagefetch.ParseRequested(value)
	if err != nil {
		return "", err
	}
	if _, pinned := named.(reference.Digested); !pinned {
		return "", invalid()
	}
	switch reference.Domain(named) {
	case "docker.io", "ghcr.io", "quay.io":
		return named.String(), nil
	default:
		return "", invalid()
	}
}

func ValidatePlan(plan imagefetch.Plan) error {
	ref, err := Reference(plan.Requested)
	if err != nil {
		return err
	}
	if ref != plan.Requested || ref != plan.Reference() {
		return errs.New(errs.KindValidationFailed, "Remote Fetch requires an exact platform manifest, not an index")
	}
	return plan.Validate()
}

func EncodeAssignment(attempt workexecution.Attempt, plan imagefetch.Plan) (*agentpb.RemoteImageAssignment, error) {
	document, digest, err := imagefetch.Encode(plan)
	if err != nil {
		return nil, err
	}
	if ValidatePlan(plan) != nil || attempt.Executor.Executor != workexecution.Agent || digest != attempt.Input.SHA256 {
		return nil, invalid()
	}
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.RemoteImageAssignment{Attempt: wire, HostId: attempt.Executor.HostID,
		TaskId: attempt.TaskID, Plan: document, PlanSha256: digest}, nil
}

func DecodeAssignment(message *agentpb.RemoteImageAssignment, identity workexecution.Identity,
) (workexecution.Attempt, imagefetch.Plan, error) {
	if !validMessage(message, 8192) || identity.Executor != workexecution.Agent || message.HostId != identity.HostID {
		return workexecution.Attempt{}, imagefetch.Plan{}, invalid()
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return workexecution.Attempt{}, imagefetch.Plan{}, err
	}
	if message.TaskId != attempt.TaskID || message.PlanSha256 != attempt.Input.SHA256 {
		return workexecution.Attempt{}, imagefetch.Plan{}, invalid()
	}
	plan, err := imagefetch.Decode(message.Plan, message.PlanSha256)
	if err != nil {
		return workexecution.Attempt{}, imagefetch.Plan{}, err
	}
	return attempt, plan, ValidatePlan(plan)
}

func EncodeReport(attempt workexecution.Attempt, localID string) (*agentpb.RemoteImageResultRequest, error) {
	if !validLocalID(localID) {
		return nil, invalid()
	}
	wire, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.RemoteImageResultRequest{Attempt: wire, LocalImageId: localID}, nil
}

func DecodeReport(message *agentpb.RemoteImageResultRequest, identity workexecution.Identity,
) (workexecution.Attempt, string, error) {
	if !validMessage(message, 4096) || !validLocalID(message.LocalImageId) {
		return workexecution.Attempt{}, "", invalid()
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	return attempt, message.LocalImageId, err
}

func ResultReference(attempt workexecution.Attempt, localID string) (workexecution.Input, error) {
	if attempt.Validate() != nil || !validLocalID(localID) {
		return workexecution.Input{}, invalid()
	}
	digest := sha256.Sum256([]byte(attempt.Input.SHA256 + ":" + localID))
	return workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, attempt.AssignedAt, attempt.ID, "remote-image-result"),
		SHA256: hex.EncodeToString(digest[:])}, nil
}

func EncodeAcknowledgement(attempt workexecution.Attempt) (*agentpb.RemoteImageResultAck, error) {
	if attempt.Validate() != nil || attempt.Result == nil || attempt.FinishedAt == nil {
		return nil, invalid()
	}
	receipt := attempt
	receipt.Result, receipt.FinishedAt, receipt.Outcome = nil, nil, ""
	identity, err := workexecution.EncodeAgentAttempt(receipt)
	if err != nil {
		return nil, err
	}
	digest, err := hex.DecodeString(attempt.Result.SHA256)
	if err != nil {
		return nil, err
	}
	ack := &agentpb.RemoteImageResultAck{
		Attempt: identity, Outcome: string(attempt.Outcome), FinishedAt: timestamppb.New(*attempt.FinishedAt),
		Evidence: &agentpb.WorkInputReference{InputId: attempt.Result.ID, Sha256: digest},
	}
	if attempt.StartedAt != nil {
		ack.StartedAt = timestamppb.New(*attempt.StartedAt)
	}
	return ack, nil
}

func validLocalID(value string) bool { return value == "" || workloadimage.LocalIDValid(value) }

func validMessage(message proto.Message, limit int) bool {
	return message.ProtoReflect().IsValid() && proto.Size(message) <= limit &&
		executionplan.RejectUnknown(message) == nil
}

func invalid() error {
	return errs.New(errs.KindValidationFailed, "Remote Fetch requires authenticated evidence for a pinned public image")
}
