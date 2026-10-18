// Package resolverexecution owns the captured platform DNS operation. It does
// not carry a generic step program or derive authority from mutable Task history.
package resolverexecution

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const MaximumInputBytes = 768 * 1024
const MaximumCorefileBytes = 96 * 1024
const MaximumReportBytes = 32 * 1024

// Configuration is immutable executable material. The Controller retains a
// separate applied copy when this input becomes collectible. Corefiles contain
// DNS configuration only; Secret values and arbitrary commands are not admitted.
type Configuration struct {
	PlanID            string                   `json:"plan_id"`
	Runtime           *agentpb.ComposeArtifact `json:"runtime"`
	Apply             *agentpb.ComponentApply  `json:"apply"`
	Observe           *agentpb.ComponentApply  `json:"observe"`
	Corefile          []byte                   `json:"corefile"`
	Generation        uint64                   `json:"generation"`
	RuntimeGeneration uint64                   `json:"runtime_generation"`
}

type Input struct {
	TaskID        string         `json:"task_id"`
	WorkID        string         `json:"work_id"`
	HostID        string         `json:"host_id"`
	OperationID   string         `json:"operation_id"`
	ComponentID   string         `json:"component_id"`
	Generation    uint64         `json:"generation"`
	CreatedAt     time.Time      `json:"created_at"`
	Deadline      time.Time      `json:"deadline"`
	Enabled       bool           `json:"enabled"`
	EnsureRuntime bool           `json:"ensure_runtime"`
	Candidate     *Configuration `json:"candidate,omitempty"`
	Previous      *Configuration `json:"previous,omitempty"`
}

func (input Input) Validate() error {
	if ids.Validate(ids.KindTask, input.TaskID) != nil || ids.Validate(ids.KindWorkItem, input.WorkID) != nil ||
		ids.Validate(ids.KindHost, input.HostID) != nil || ids.Validate(ids.KindOperation, input.OperationID) != nil ||
		ids.Validate(ids.KindComponent, input.ComponentID) != nil || input.Generation == 0 || !canonicalTime(input.CreatedAt) ||
		!canonicalTime(input.Deadline) || !input.Deadline.After(input.CreatedAt) ||
		input.Deadline.Sub(input.CreatedAt) > 24*time.Hour ||
		input.Enabled != (input.Candidate != nil) || !input.Enabled && (input.EnsureRuntime || input.Previous == nil) ||
		input.Enabled && !input.EnsureRuntime && input.Previous == nil {
		return invalid("DNS resolver input has invalid identity, timing or captured runtime selection")
	}
	if input.Candidate != nil && input.Candidate.Generation != input.Generation ||
		input.Previous != nil && input.Previous.Generation >= input.Generation {
		return invalid("DNS resolver input did not advance its captured configuration generation")
	}
	for _, configuration := range []*Configuration{input.Candidate, input.Previous} {
		if configuration != nil {
			if err := configuration.Validate(input.ComponentID); err != nil {
				return err
			}
		}
	}
	if input.Candidate != nil && input.Previous != nil {
		candidate, previous := input.Candidate, input.Previous
		if candidate.Generation <= previous.Generation || candidate.Runtime.Services[0].ServiceId != previous.Runtime.Services[0].ServiceId ||
			!input.EnsureRuntime && (!bytes.Equal(candidate.Runtime.YamlSha256, previous.Runtime.YamlSha256) ||
				candidate.Runtime.ArtifactId != previous.Runtime.ArtifactId) ||
			!bytes.Equal(candidate.Apply.ExpectedPreviousArtifactDigest, previous.Apply.ArtifactDigest) ||
			candidate.Apply.ExpectedPreviousArtifactId != previous.Apply.ArtifactId ||
			candidate.Apply.ExpectedPreviousGeneration != previous.Generation {
			return invalid("DNS resolver candidate differs from its captured predecessor")
		}
	} else if candidate := input.Candidate; candidate != nil && (len(candidate.Apply.ExpectedPreviousArtifactDigest) != 0 ||
		candidate.Apply.ExpectedPreviousArtifactId != "" || candidate.Apply.ExpectedPreviousGeneration != 0) {
		return invalid("first DNS resolver activation cannot name an absent predecessor")
	}
	return nil
}

func (configuration Configuration) Validate(componentID string) error {
	if configuration.Generation == 0 || configuration.RuntimeGeneration == 0 || configuration.RuntimeGeneration > configuration.Generation ||
		len(configuration.Corefile) == 0 || len(configuration.Corefile) > MaximumCorefileBytes ||
		configuration.Runtime == nil || configuration.Runtime.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM ||
		configuration.Runtime.OwnerId != "" || len(configuration.Runtime.Services) != 1 || configuration.Runtime.Services[0] == nil ||
		configuration.Runtime.Services[0].OwnerComponentId != componentID || configuration.Runtime.Services[0].ExpectedReplicas != 1 {
		return invalid("DNS resolver configuration requires one captured platform-owned container")
	}
	// Configuration reloads may retain the running Compose ownership generation.
	if err := executionplan.ValidateCapturedArtifact(configuration.PlanID, configuration.RuntimeGeneration, configuration.Runtime); err != nil {
		return err
	}
	digest := sha256.Sum256(configuration.Corefile)
	for index, action := range []*agentpb.ComponentApply{configuration.Apply, configuration.Observe} {
		if action == nil || executionplan.RejectUnknown(action) != nil || action.ComponentId != componentID ||
			action.Generation != configuration.Generation || ids.Validate(ids.KindConfig, action.ArtifactId) != nil ||
			len(action.DefinitionDigest) != sha256.Size || len(action.CatalogDigest) != sha256.Size ||
			!bytes.Equal(action.ArtifactDigest, digest[:]) || action.ActionId == "" || len(action.ActionId) > 128 ||
			action.ManagedConfigContent != (index == 0) {
			return invalid("DNS resolver configuration action differs from its captured Corefile")
		}
	}
	if configuration.Apply.ArtifactId != configuration.Observe.ArtifactId ||
		!bytes.Equal(configuration.Apply.DefinitionDigest, configuration.Observe.DefinitionDigest) ||
		!bytes.Equal(configuration.Apply.CatalogDigest, configuration.Observe.CatalogDigest) ||
		len(configuration.Observe.ExpectedPreviousArtifactDigest) != 0 || configuration.Observe.ExpectedPreviousArtifactId != "" ||
		configuration.Observe.ExpectedPreviousGeneration != 0 {
		return invalid("DNS resolver observation is not bound to its captured configuration")
	}
	return nil
}

func Capture(inputID string, input Input) ([]byte, workexecution.Input, error) {
	if err := input.Validate(); err != nil {
		return nil, workexecution.Input{}, err
	}
	raw, err := json.Marshal(input)
	if err != nil {
		return nil, workexecution.Input{}, errs.Wrap(errs.KindInternal, err)
	}
	canonical, err := jcs.Canonicalize(raw)
	if err != nil || len(canonical) > MaximumInputBytes {
		return nil, workexecution.Input{}, invalid("DNS resolver input exceeds its captured-input bound")
	}
	digest := sha256.Sum256(canonical)
	reference := workexecution.Input{ID: inputID, SHA256: hex.EncodeToString(digest[:])}
	if err := reference.Validate(); err != nil {
		return nil, workexecution.Input{}, err
	}
	return canonical, reference, nil
}

func Decode(inputID string, raw []byte) (Input, workexecution.Input, error) {
	if len(raw) == 0 || len(raw) > MaximumInputBytes {
		return Input{}, workexecution.Input{}, invalid("DNS resolver captured input is missing or oversized")
	}
	input, err := jcs.Decode[Input](raw)
	if err != nil {
		return Input{}, workexecution.Input{}, invalid("DNS resolver captured input cannot be decoded")
	}
	canonical, reference, err := Capture(inputID, input)
	if err != nil || !bytes.Equal(canonical, raw) {
		return Input{}, workexecution.Input{}, invalid("DNS resolver captured input is not canonical")
	}
	return input, reference, nil
}

func canonicalTime(at time.Time) bool { return !at.IsZero() && at.Location() == time.UTC }
func invalid(message string) error    { return errs.New(errs.KindValidationFailed, message) }
