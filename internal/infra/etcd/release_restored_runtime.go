package etcd

import (
	"bytes"
	"time"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

func restoredReleaseRuntimeMutation(task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	recovery releaseRecoveryAcknowledgement, intent domain.Intent, value *etcdstore.KeyValue, at time.Time,
) (etcdstore.Mutation, error) {
	var current serviceruntimerecord.Record
	if value != nil {
		var err error
		current, err = releases.DecodeReleaseRecord[serviceruntimerecord.Record](
			value.Value,
			"service-acknowledged-runtime",
		)
		if err != nil || serviceruntimerecord.Validate(current) != nil ||
			current.EnvironmentID != task.Owner.EnvironmentID ||
			current.Runtime.ServiceID != intent.ServiceID ||
			current.Runtime.ReleaseID != intent.ID && current.Runtime.ReleaseID != intent.PriorServingReleaseID {
			return etcdstore.Mutation{}, releases.CorruptReleaseRecord()
		}
	}
	key := serviceruntimerecord.Key(intent.ServiceID)
	for index, witness := range assignment.RestorationAuthority.NativePredecessors {
		if witness.ServiceID != intent.ServiceID {
			continue
		}
		candidate := assignment.RestorationAuthority.Candidates[index]
		if candidate.Target == taskassignments.ReleaseRestorationCandidateAbsence {
			if intent.PriorServingReleaseID != "" || len(witness.CurrentArtifact) != 0 {
				return etcdstore.Mutation{}, releases.CorruptReleaseRecord()
			}
			if value == nil {
				return etcdstore.Mutation{}, nil
			}
			return etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key}, nil
		}
		if intent.PriorServingReleaseID == "" {
			return etcdstore.Mutation{}, releases.CorruptReleaseRecord()
		}
		if value != nil && current.Runtime.ReleaseID == intent.PriorServingReleaseID &&
			bytes.Equal(current.Runtime.CurrentArtifact, witness.CurrentArtifact) &&
			bytes.Equal(current.Runtime.RetainedPriorArtifact, witness.RetainedPriorArtifact) {
			return etcdstore.Mutation{}, nil
		}
		target := "singleton"
		if proxy, ok := releaseProxyEvidence(recovery.result, intent.ServiceID); ok {
			target = proxy.Target
		}
		digest, err := domain.Digest(recovery.result)
		if err != nil || len(recovery.record.RecoveryStepIDs) == 0 {
			return etcdstore.Mutation{}, releases.CorruptReleaseRecord()
		}
		record, err := serviceruntimerecord.Restore(
			task.Owner.EnvironmentID,
			intent.ServiceID,
			intent.PriorServingReleaseID,
			target,
			witness.CurrentArtifact,
			witness.RetainedPriorArtifact,
			serviceruntimerecord.Acknowledgement{
				TaskID: task.ID, PlanID: task.PlanID, PlanHash: task.PlanHash,
				StepID:  recovery.record.RecoveryStepIDs[len(recovery.record.RecoveryStepIDs)-1],
				AgentID: assignment.AgentID, AssignmentID: assignment.AssignmentID, ExecutionEpoch: assignment.ExecutionEpoch,
				RenderGeneration: uint64(task.RenderGeneration), EffectDigest: digest, AcknowledgedAt: at,
			},
		)
		if err != nil {
			return etcdstore.Mutation{}, err
		}
		if task.Configuration != nil {
			record.Configuration = task.Configuration.Prior
		}
		if err := serviceruntimerecord.Validate(record); err != nil {
			return etcdstore.Mutation{}, err
		}
		encoded, err := releases.EncodeReleaseRecord("service-acknowledged-runtime", record)
		return etcdstore.Mutation{Type: etcdstore.MutationPut, Key: key, Value: encoded}, err
	}
	return etcdstore.Mutation{}, releases.CorruptReleaseRecord()
}
