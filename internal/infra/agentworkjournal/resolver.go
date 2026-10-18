package agentworkjournal

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/dnsproof"
	"github.com/AlanD20/groundplane/internal/common/resolverexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ResolverRecord keeps only the compact cursor, one actual serving proof and
// delivery receipts. Captured Corefiles and Compose YAML are not journal history.
type ResolverRecord struct {
	Layout        resolverexecution.Layout                `json:"layout"`
	Progress      *resolverexecution.Progress             `json:"progress,omitempty"`
	Acknowledged  bool                                    `json:"acknowledged"`
	Verification  *resolverexecution.Progress             `json:"verification,omitempty"`
	Proof         *agentpb.DNSResolverObservationEvidence `json:"proof,omitempty"`
	FailureReason resolverexecution.FailureReason         `json:"failure_reason,omitempty"`
	Failure       []byte                                  `json:"failure,omitempty"`
	Source        *resolverexecution.Recovery             `json:"source,omitempty"`
	Recovery      *resolverexecution.RecoveryReport       `json:"recovery,omitempty"`
	Receipt       *workexecution.Attempt                  `json:"receipt,omitempty"`
}

func (record attemptRecord) validateResolver() error {
	entry := record.Resolver
	if entry == nil || record.Attempt.Validate() != nil || record.Attempt.FinishedAt != nil ||
		record.Attempt.Executor.Executor != workexecution.Agent || entry.Layout.Validate(record.Attempt) != nil ||
		record.UpdatedAt.IsZero() || record.UpdatedAt.Before(record.Attempt.AssignedAt) {
		return invalid()
	}
	for _, present := range []bool{record.BackingRecovery != nil, record.Backing != nil, record.EnvironmentProvision != nil,
		record.Backup != nil, record.RemovalRecovery != nil, record.LifecycleRecovery != nil, record.Lifecycle != nil,
		record.AttachProvisionRecovery != nil, record.AttachProvisionFailure != nil, record.AttachProvision != nil,
		record.EntryRecovery != nil, record.EntryConfiguration != nil, record.ComponentRecovery != nil, record.Component != nil,
		record.NetworkRecovery != nil, record.Network != nil, record.VolumeRecovery != nil, record.Volume != nil,
		record.AttachRecovery != nil, record.AttachFailure != nil, record.AttachRemoval != nil, record.EnvironmentFailure != nil,
		record.EnvironmentRemoval != nil, record.ScriptFailure != nil, record.ScriptRecovery != nil, len(record.Script) != 0,
		record.Release != nil, record.Effect != nil, record.Completion != nil, record.Failure != nil, record.Recovery != nil, record.ScriptCompletion != nil} {
		if present {
			return invalid()
		}
	}
	if entry.Source != nil {
		return record.validateResolverRecovery()
	}
	if record.Attempt.Direction != workexecution.Forward || entry.Recovery != nil {
		return invalid()
	}
	if entry.Progress != nil {
		if entry.Progress.Validate(record.Attempt, entry.Layout) != nil || entry.Progress.Sequence == 0 ||
			record.Sequence != entry.Progress.Sequence || !record.UpdatedAt.Equal(entry.Progress.UpdatedAt) {
			return invalid()
		}
	} else if record.Sequence != 0 || entry.Acknowledged || entry.Verification != nil || entry.Proof != nil || entry.FailureReason == "" {
		return invalid()
	}
	if entry.Proof != nil {
		proof := entry.Proof
		if !entry.Layout.Enabled || dnsproof.Verify(proof) != nil || proof.ComponentId != entry.Layout.ComponentID ||
			hex.EncodeToString(proof.ArtifactSha256) != entry.Layout.ConfigurationSHA256 || proof.ObservedAt == nil ||
			proof.ObservedAt.CheckValid() != nil || record.Attempt.StartedAt == nil || proof.ObservedAt.AsTime().Before(*record.Attempt.StartedAt) ||
			entry.Progress == nil || entry.Progress.Sequence < 1 ||
			entry.Progress.Stage != resolverexecution.VerifyDNS && entry.Verification == nil {
			return invalid()
		}
	}
	if verification := entry.Verification; verification != nil {
		if verification.Validate(record.Attempt, entry.Layout) != nil || verification.InFlight || verification.Last == nil ||
			verification.Last.Stage != resolverexecution.VerifyDNS || entry.Proof == nil ||
			verification.Last.ServingProofSHA256 != hex.EncodeToString(entry.Proof.ProofSha256) ||
			!verification.Last.At.Equal(entry.Proof.ObservedAt.AsTime()) || entry.Progress == nil ||
			verification.Sequence > entry.Progress.Sequence || verification.UpdatedAt.After(entry.Progress.UpdatedAt) {
			return invalid()
		}
	}
	if entry.FailureReason != "" {
		if entry.FailureReason != resolverexecution.ExecutionFailed && entry.FailureReason != resolverexecution.Interrupted &&
			entry.FailureReason != resolverexecution.DeadlineExpired || entry.Progress != nil && entry.Progress.Stage == resolverexecution.Complete {
			return invalid()
		}
		if receipt := entry.Receipt; receipt != nil {
			if !sameResolverTerminal(record.Attempt, *receipt) || receipt.Outcome == workexecution.Succeeded {
				return invalid()
			}
			failure, err := resolverexecution.DecodeFailure(*receipt, entry.Layout, entry.Failure, *receipt.Result)
			if err != nil || failure.Reason != entry.FailureReason {
				return invalid()
			}
		} else if len(entry.Failure) != 0 {
			return invalid()
		}
	} else if len(entry.Failure) != 0 {
		return invalid()
	} else if receipt := entry.Receipt; receipt != nil {
		if !sameResolverTerminal(record.Attempt, *receipt) || entry.Progress == nil || !entry.Acknowledged ||
			entry.Progress.Stage != resolverexecution.Complete || entry.Layout.Enabled && (entry.Verification == nil || entry.Proof == nil) {
			return invalid()
		}
		evidence, err := resolverexecution.CompletionDigest(*receipt, *entry.Progress, entry.Proof)
		if err != nil || evidence != *receipt.Result {
			return invalid()
		}
	}
	return nil
}

func (record attemptRecord) validateResolverRecovery() error {
	entry := record.Resolver
	if record.Attempt.StartedAt == nil || entry.Source.ValidateFor(record.Attempt, entry.Layout) != nil ||
		record.Sequence != 0 || entry.Progress != nil || entry.Acknowledged || entry.Verification != nil || entry.Proof != nil ||
		entry.FailureReason != "" || len(entry.Failure) != 0 || record.UpdatedAt.Before(*record.Attempt.StartedAt) {
		return invalid()
	}
	if entry.Recovery == nil {
		if entry.Receipt != nil {
			return invalid()
		}
		return nil
	}
	if entry.Recovery.ValidateBoundary(record.Attempt, *entry.Source, entry.Layout) != nil ||
		!record.UpdatedAt.Equal(entry.Recovery.ObservedAt) {
		return invalid()
	}
	if receipt := entry.Receipt; receipt != nil {
		outcome := workexecution.Failed
		if entry.Recovery.Status == resolverexecution.RecoveryComplete {
			outcome = workexecution.Succeeded
		}
		evidence, err := resolverexecution.RecoveryDigest(record.Attempt.ID, *entry.Recovery)
		if err != nil || !sameResolverTerminal(record.Attempt, *receipt) || evidence != *receipt.Result ||
			receipt.Outcome != outcome || receipt.FinishedAt.Before(record.UpdatedAt) {
			return invalid()
		}
	}
	return nil
}

func sameResolverTerminal(active, terminal workexecution.Attempt) bool {
	if terminal.Validate() != nil || !workexecution.SameAssignment(active, terminal) || terminal.FinishedAt == nil || terminal.Result == nil ||
		(active.StartedAt == nil) != (terminal.StartedAt == nil) {
		return false
	}
	return active.StartedAt == nil || active.StartedAt.Equal(*terminal.StartedAt)
}
