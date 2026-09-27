package runnerallocation

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/netip"
	"slices"
)

type RunnerRuntimeStep string

const (
	StepEnsureIdentity RunnerRuntimeStep = "ensure_identity"
	StepEnsureNetwork  RunnerRuntimeStep = "ensure_network"
	StepEnsureEgress   RunnerRuntimeStep = "ensure_egress"
	StepStartDaemon    RunnerRuntimeStep = "start_daemon"
	StepStartRunner    RunnerRuntimeStep = "start_runner"
	StepStopRunner     RunnerRuntimeStep = "stop_runner"
	StepStopDaemon     RunnerRuntimeStep = "stop_daemon"
	StepRemoveNetwork  RunnerRuntimeStep = "remove_network"
	StepRemoveEgress   RunnerRuntimeStep = "remove_egress"
	StepRemoveIdentity RunnerRuntimeStep = "remove_identity"
)

type RunnerRuntimeIdentity struct {
	User        string
	Group       string
	UID         uint32
	GID         uint32
	SubUIDStart uint32
	SubUIDCount uint32
	SubGIDStart uint32
	SubGIDCount uint32
}

type RunnerRuntimePaths struct {
	SlotRoot   string
	RunnerHome string
	WorkRoot   string
	DataRoot   string
	RawSocket  string
}

type RunnerRuntimeNetwork struct {
	Name          string
	BridgeName    string
	RunnerPool    netip.Prefix
	Subnet        netip.Prefix
	Gateway       netip.Addr
	RunnerAddress netip.Addr
}

type RunnerRuntimeEgress struct {
	SourceUID          uint32
	DeniedCIDRs        []netip.Prefix
	ControllerEndpoint netip.AddrPort
}

type RunnerContainer struct {
	Name               string
	ImageRef           string
	User               string
	Privileged         bool
	ReadOnlyRootFS     bool
	CapAdd             []string
	CapDrop            []string
	SecurityOptions    []string
	NetworkName        string
	NetworkAddress     netip.Addr
	DockerSocketSource string
	DockerSocketTarget string
	GitHubURL          string
	RunnerName         string
	Labels             []string
}

type RuntimePlan struct {
	RunnerID         string
	TenantID         string
	OwnerKind        string
	OwnerID          string
	RuntimeEpoch     uint64
	AllocationConfig RunnerAllocationConfig
	Allocation       RunnerHostAllocationRecord
	Identity         RunnerRuntimeIdentity
	Paths            RunnerRuntimePaths
	Network          RunnerRuntimeNetwork
	Egress           RunnerRuntimeEgress
	Container        RunnerContainer
}

func (plan RuntimePlan) Digest() string {
	encoded, err := json.Marshal(plan)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(digest[:])
}

// Clone keeps the journal's sealed inputs independent from caller-owned slices.
func (plan RuntimePlan) Clone() RuntimePlan {
	plan.Egress.DeniedCIDRs = slices.Clone(plan.Egress.DeniedCIDRs)
	plan.Container.CapAdd = slices.Clone(plan.Container.CapAdd)
	plan.Container.CapDrop = slices.Clone(plan.Container.CapDrop)
	plan.Container.SecurityOptions = slices.Clone(plan.Container.SecurityOptions)
	plan.Container.Labels = slices.Clone(plan.Container.Labels)
	return plan
}

type RunnerRuntimeEvidence struct {
	ContainerID  string `json:"container_id"`
	DaemonNonce  string `json:"daemon_nonce"`
	SocketDevice uint64 `json:"socket_device"`
	SocketInode  uint64 `json:"socket_inode"`
}

type RunnerRuntimeEffectState string

const (
	RuntimeEffectApplied RunnerRuntimeEffectState = "applied"
	RuntimeEffectAbsent  RunnerRuntimeEffectState = "absent"
)

type RunnerRuntimeStepEvidence struct {
	Step          RunnerRuntimeStep        `json:"step"`
	State         RunnerRuntimeEffectState `json:"state"`
	Ownership     *RunnerRuntimeEvidence   `json:"ownership,omitempty"`
	ReceiptSHA256 string                   `json:"receipt_sha256,omitempty"`
}

func (evidence RunnerRuntimeStepEvidence) Valid(operation RuntimeOperation, expected RunnerRuntimeStep) bool {
	if evidence.Step != expected {
		return false
	}
	if operation == RuntimeOperationCreate {
		if evidence.State != RuntimeEffectApplied || evidence.ReceiptSHA256 != "" {
			return false
		}
		if expected == StepStartRunner {
			return evidence.Ownership != nil && evidence.Ownership.Valid()
		}
		return evidence.Ownership == nil
	}
	return operation == RuntimeOperationRemove && evidence.State == RuntimeEffectAbsent &&
		evidence.Ownership == nil && validRuntimeDigest(evidence.ReceiptSHA256)
}

type RuntimeOperation string

const (
	RuntimeOperationCreate RuntimeOperation = "create"
	RuntimeOperationRemove RuntimeOperation = "remove"
)

type RuntimeStatus string

const (
	RuntimeStatusRunning RuntimeStatus = "running"
	RuntimeStatusFailed  RuntimeStatus = "failed"
	RuntimeStatusReady   RuntimeStatus = "ready"
	RuntimeStatusRemoved RuntimeStatus = "removed"
)

type RunnerRuntimeAttempt struct {
	TaskID         string
	Executor       string
	OwnershipNonce string
	Plan           RuntimePlan
}

type RunnerRuntimeProgress struct {
	Plan              RuntimePlan                 `json:"plan"`
	BootID            string                      `json:"boot_id"`
	RunnerID          string                      `json:"runner_id"`
	TaskID            string                      `json:"task_id"`
	Executor          string                      `json:"executor"`
	PlanDigest        string                      `json:"plan_digest"`
	IdentityDigest    string                      `json:"identity_digest"`
	RuntimeEpoch      uint64                      `json:"runtime_epoch"`
	OwnershipNonce    string                      `json:"ownership_nonce"`
	Operation         RuntimeOperation            `json:"operation"`
	NextStep          int                         `json:"next_step"`
	ActiveStep        *RunnerRuntimeStep          `json:"active_step,omitempty"`
	Status            RuntimeStatus               `json:"status"`
	Revision          int64                       `json:"revision"`
	PredecessorSHA256 string                      `json:"predecessor_sha256,omitempty"`
	Evidence          *RunnerRuntimeEvidence      `json:"evidence,omitempty"`
	CleanupReceipts   []RunnerRuntimeStepEvidence `json:"cleanup_receipts,omitempty"`
	HeadRevision      int64                       `json:"-"`
}

func (evidence RunnerRuntimeEvidence) Valid() bool {
	return validRuntimeHex(evidence.ContainerID) && validRuntimeHex(evidence.DaemonNonce) &&
		evidence.SocketDevice != 0 && evidence.SocketInode != 0
}

func validRuntimeHex(value string) bool {
	if len(value) != 64 {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' && character < 'a' || character > 'f' {
			return false
		}
	}
	return true
}

func validRuntimeDigest(value string) bool {
	return len(value) == len("sha256:")+64 && value[:len("sha256:")] == "sha256:" &&
		validRuntimeHex(value[len("sha256:"):])
}

func (plan RuntimePlan) IdentityDigest() string {
	plan.RuntimeEpoch = 0
	return plan.Digest()
}

func CreationRuntimeSteps() []RunnerRuntimeStep {
	return []RunnerRuntimeStep{
		StepEnsureIdentity, StepEnsureNetwork, StepEnsureEgress,
		StepStartDaemon, StepStartRunner,
	}
}

func RemovalRuntimeSteps() []RunnerRuntimeStep {
	return []RunnerRuntimeStep{
		StepStopRunner, StepStopDaemon, StepRemoveNetwork,
		StepRemoveIdentity, StepRemoveEgress,
	}
}
