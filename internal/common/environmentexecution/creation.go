// Package environmentexecution owns the closed machine contract for captured
// Environment directory provisioning, not desired state or filesystem access.
package environmentexecution

import (
	"crypto/sha256"
	"encoding/hex"
	"net/netip"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/environmentpath"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const MaximumInputBytes = 8192

func ValidateInput(input *agentpb.EnvironmentProvisionInput) error {
	if input == nil || proto.Size(input) > MaximumInputBytes || executionplan.RejectUnknown(input) != nil ||
		ids.Validate(ids.KindTask, input.TaskId) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemId) != nil ||
		ids.Validate(ids.KindHost, input.HostId) != nil || input.CreatedAt == nil || input.CreatedAt.CheckValid() != nil ||
		input.CreatedAt.AsTime().IsZero() || input.EnvironmentName == "" || len(input.EnvironmentName) > 256 || !utf8.ValidString(input.EnvironmentName) {
		return invalid("environment provisioning input is invalid")
	}
	scope, err := environmentpath.Parse(input.VolumeRoot, input.VolumeDirectory)
	if err != nil {
		return err
	}
	if scope.ProjectKind != environmentpath.ProjectKindTenant || scope.TenantID != input.TenantId ||
		scope.ProjectID != input.ProjectId || scope.EnvironmentID != input.EnvironmentId {
		return invalid("environment provisioning directory does not match its captured scope")
	}
	pool, err := netip.ParsePrefix(input.NetworkPool)
	if err != nil || !pool.Addr().Is4() || pool != pool.Masked() || pool.String() != input.NetworkPool {
		return invalid("environment provisioning network pool is invalid")
	}
	return nil
}

func Reference(planID string, input *agentpb.EnvironmentProvisionInput) (workexecution.Input, error) {
	if err := ValidateInput(input); err != nil {
		return workexecution.Input{}, err
	}
	raw, err := (proto.MarshalOptions{Deterministic: true}).Marshal(input)
	if err != nil {
		return workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(raw)
	ref := workexecution.Input{ID: planID, SHA256: hex.EncodeToString(digest[:])}
	if err := ref.Validate(); err != nil {
		return workexecution.Input{}, err
	}
	return ref, nil
}

type Input struct {
	Attempt        workexecution.Attempt
	Creation       *agentpb.EnvironmentProvisionInput
	RecoverySource *workexecution.Attempt
}

func (input Input) Validate() error {
	if input.Attempt.Validate() != nil || input.Attempt.Executor.Executor != workexecution.Agent || input.Attempt.FinishedAt != nil {
		return invalid("environment provisioning requires an active Agent attempt")
	}
	reference, err := Reference(input.Attempt.Input.ID, input.Creation)
	if err != nil {
		return err
	}
	if input.Attempt.TaskID != input.Creation.TaskId || input.Attempt.WorkItemID != input.Creation.WorkItemId ||
		input.Attempt.Executor.HostID != input.Creation.HostId || input.Attempt.Input != reference {
		return invalid("environment provisioning assignment differs from captured authority")
	}
	if input.Attempt.Direction == workexecution.Forward {
		if input.RecoverySource != nil {
			return invalid("forward provisioning cannot carry cleanup authority")
		}
		return nil
	}
	source := input.RecoverySource
	if input.Attempt.Direction != workexecution.Recovery || source == nil || source.Validate() != nil ||
		source.Direction != workexecution.Forward || source.TaskID != input.Attempt.TaskID || source.WorkItemID != input.Attempt.WorkItemID ||
		source.Input != input.Attempt.Input || source.Executor.HostID != input.Attempt.Executor.HostID || source.Number >= input.Attempt.Number ||
		source.FinishedAt == nil || source.Outcome == workexecution.Succeeded || source.Result == nil {
		return invalid("environment cleanup requires its captured failed forward attempt")
	}
	return nil
}

func DecodeAssignment(message *agentpb.EnvironmentProvisionAssignment, identity workexecution.Identity) (Input, error) {
	if message == nil || executionplan.RejectUnknown(message) != nil || proto.Size(message) > MaximumInputBytes+4096 {
		return Input{}, invalid("environment provisioning assignment is missing or oversized")
	}
	attempt, err := workexecution.DecodeAgentAttempt(message.Attempt, identity)
	if err != nil {
		return Input{}, err
	}
	input := Input{Attempt: attempt, Creation: proto.CloneOf(message.Input)}
	if message.RecoverySource != nil {
		source, err := DecodeRecoverySource(message.RecoverySource)
		if err != nil {
			return Input{}, err
		}
		input.RecoverySource = &source
	}
	if err := input.Validate(); err != nil {
		return Input{}, err
	}
	return input, nil
}

func invalid(message string) error { return errs.New(errs.KindValidationFailed, message) }
