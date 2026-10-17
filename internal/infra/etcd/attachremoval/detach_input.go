package attachremoval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

const maximumDetachBytes = 4 * executionplan.MaximumPlanBytes

// DetachInput owns the complete admission snapshot, not a saved old Task or
// ExecutionPlan. Consumer contains deterministic protobuf bytes so the stored
// record cannot accept a second JSON representation of the Agent contract.
// Exact hook input ciphertext and Secret pins live in WorkCapture, bound to
// this input's digest and atomically published with the Task and binding.
type DetachInput struct {
	TaskID     string         `json:"task_id"`
	WorkItemID string         `json:"work_item_id"`
	Snapshot   DetachSnapshot `json:"snapshot"`
	Consumer   []byte         `json:"consumer,omitempty"`
}

// HasHostEffects determines placement after capture. A plain binding without
// an applied consumer, credential or Detach hook settles in the Controller;
// it must not consume Agent capacity for a fabricated no-op command.
func (input DetachInput) HasHostEffects() bool {
	return len(input.Consumer) != 0 || NeedsAgent(input.Snapshot.Source)
}

func (input DetachInput) Reference(ctx context.Context, inputID string) (workexecution.Input, error) {
	raw, reference, err := captureDetachInput(ctx, inputID, input)
	clear(raw)
	return reference, err
}

// Claims serializes both credential work and the consumer's physical edge.
// Host-scoped claims keep a later remote-host implementation from conflating
// identically named Docker objects; control-plane claims protect stable IDs.
func (input DetachInput) Claims(hostID string) []workexecution.Claim {
	attach := input.Snapshot.Source.Attach
	resources := []string{attach.ID, attach.EnvironmentID, attach.ServiceID, attach.BackingServiceID, attach.BackingEnvironmentID}
	resources = append(resources, attach.GrantAttachIDs...)
	if !attach.OwnsCredential() {
		resources = append(resources, attach.CredentialAttachID)
	}
	claims := make([]workexecution.Claim, 0, len(resources)*2)
	for _, id := range resources {
		claims = append(claims, workexecution.Claim{ScopeID: id}, workexecution.Claim{HostID: hostID, ScopeID: id})
	}
	slices.SortFunc(claims, func(left, right workexecution.Claim) int {
		if order := strings.Compare(left.HostID, right.HostID); order != 0 {
			return order
		}
		return strings.Compare(left.ScopeID, right.ScopeID)
	})
	return slices.Compact(claims)
}

// NewDetachInput transfers snapshot ownership on success. On failure the caller
// retains it. The Agent descriptor is serialized into independent owned bytes.
func NewDetachInput(ctx context.Context, taskID, workID string, snapshot *DetachSnapshot,
	consumer *agentpb.AttachConsumerTransition,
) (DetachInput, error) {
	if snapshot == nil {
		return DetachInput{}, errs.New(errs.KindValidationFailed, "Detach snapshot is required")
	}
	input := DetachInput{TaskID: taskID, WorkItemID: workID, Snapshot: *snapshot}
	if consumer != nil {
		var err error
		input.Consumer, err = proto.MarshalOptions{Deterministic: true}.Marshal(consumer)
		if err != nil {
			return DetachInput{}, errs.Wrap(errs.KindValidationFailed, err)
		}
	}
	if err := input.Validate(ctx); err != nil {
		clear(input.Consumer)
		return DetachInput{}, err
	}
	*snapshot = DetachSnapshot{}
	return input, nil
}

func (input DetachInput) Validate(ctx context.Context) error {
	if ctx == nil || ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkItemID) != nil {
		return recordcodec.CorruptRecord()
	}
	if err := input.Snapshot.Validate(); err != nil {
		return err
	}
	source := input.Snapshot.Source
	if source.HookInputSet != nil || source.HookInputs != nil || source.HookOperationID != "" {
		return errs.New(errs.KindValidationFailed, "Detach admission input cannot contain unbound hook delivery inputs")
	}
	requiresConsumer := input.Snapshot.Runtime != nil && source.Attach.Status != "detached" && !input.Snapshot.KeepsConsumerNetwork()
	if requiresConsumer != (len(input.Consumer) != 0) {
		return errs.New(errs.KindValidationFailed, "Detach consumer work does not match its captured runtime and binding union")
	}
	if !requiresConsumer {
		return nil
	}
	consumer, err := input.ConsumerTransition(ctx)
	if err != nil {
		return err
	}
	defer clearConsumer(consumer)
	target := source.Attach
	baseline := &agentpb.ComposeArtifact{}
	if proto.Unmarshal(input.Snapshot.Runtime.Runtime.CurrentArtifact, baseline) != nil ||
		consumer.EnvironmentId != target.EnvironmentID || consumer.BackingEnvironmentId != target.BackingEnvironmentID ||
		consumer.ServiceId != target.ServiceID || consumer.NetworkId != target.BackingNetworkID || !proto.Equal(consumer.Baseline, baseline) {
		return errs.New(errs.KindValidationFailed, "Detach consumer source differs from the captured applied runtime")
	}
	return nil
}

func (input DetachInput) ConsumerTransition(ctx context.Context) (*agentpb.AttachConsumerTransition, error) {
	if len(input.Consumer) == 0 {
		return nil, nil
	}
	if len(input.Consumer) > executionplan.MaximumPlanBytes {
		return nil, recordcodec.CorruptRecord()
	}
	consumer := &agentpb.AttachConsumerTransition{}
	if proto.Unmarshal(input.Consumer, consumer) != nil {
		return nil, recordcodec.CorruptRecord()
	}
	if err := executionplan.ValidateAttachConsumerTransition(ctx, consumer); err != nil {
		clearConsumer(consumer)
		return nil, err
	}
	return consumer, nil
}

func captureDetachInput(ctx context.Context, inputID string, input DetachInput) ([]byte, workexecution.Input, error) {
	if ids.Validate(ids.KindPlan, inputID) != nil {
		return nil, workexecution.Input{}, errs.New(errs.KindValidationFailed, "Detach input identity is invalid")
	}
	if err := input.Validate(ctx); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := recordcodec.Encode("detach-work-input", input)
	if err != nil {
		return nil, workexecution.Input{}, err
	}
	if len(raw) > maximumDetachBytes {
		clear(raw)
		return nil, workexecution.Input{}, errs.New(errs.KindValidationFailed, "captured Detach input exceeds its storage bound")
	}
	digest := sha256.Sum256(raw)
	return raw, workexecution.Input{ID: inputID, SHA256: hex.EncodeToString(digest[:])}, nil
}

func decodeDetachInput(ctx context.Context, raw []byte) (DetachInput, error) {
	if len(raw) > maximumDetachBytes {
		return DetachInput{}, recordcodec.CorruptRecord()
	}
	input, err := recordcodec.Decode[DetachInput](raw, "detach-work-input")
	if err != nil {
		input.Clear()
		return DetachInput{}, err
	}
	if err := input.Validate(ctx); err != nil {
		input.Clear()
		return DetachInput{}, err
	}
	return input, nil
}

func (input *DetachInput) Clear() {
	if input != nil {
		input.Snapshot.Clear()
		clear(input.Consumer)
	}
}

func clearConsumer(consumer *agentpb.AttachConsumerTransition) {
	if consumer != nil {
		if consumer.Baseline != nil {
			clear(consumer.Baseline.CanonicalYaml)
		}
		if consumer.Candidate != nil {
			clear(consumer.Candidate.CanonicalYaml)
		}
	}
}
