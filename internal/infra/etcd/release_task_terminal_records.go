package etcd

import (
	"encoding/hex"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func decodeReleaseProjection(value *etcdstore.KeyValue, environmentID, serviceID string) (domain.ServiceProjection, error) {
	if value == nil {
		return domain.ServiceProjection{}, nil
	}
	projection, err := releases.DecodeReleaseRecord[domain.ServiceProjection](value.Value, "service-release-projection")
	if err != nil || projection.EnvironmentID != environmentID || projection.ServiceID != serviceID ||
		projection.ActiveOperationID != "" {
		return domain.ServiceProjection{}, releases.CorruptReleaseRecord()
	}
	return projection, nil
}

// releaseTerminalRecordMutations encodes the indivisible member checkpoint,
// history, retention and serving projection before considering its batch size.
func releaseTerminalRecordMutations(
	keys []string,
	checkpoint domain.Checkpoint,
	terminal domain.TerminalSummary,
	retention domain.RollbackMaterial,
	projection domain.ServiceProjection,
	writeProjection bool,
) ([]etcdstore.Mutation, error) {
	mutations := make([]etcdstore.Mutation, 0, 4)
	checkpointValue, err := releases.EncodeReleaseRecord("release-checkpoint", checkpoint)
	if err != nil {
		return nil, err
	}
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[1], Value: checkpointValue})
	terminalValue, err := releases.EncodeReleaseRecord("release-terminal-summary", terminal)
	if err != nil {
		clearMutations(mutations)
		return nil, err
	}
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[3], Value: terminalValue})
	retentionValue, err := releases.EncodeReleaseRecord("release-retention", retention)
	if err != nil {
		clearMutations(mutations)
		return nil, err
	}
	mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[4], Value: retentionValue})
	if writeProjection {
		projectionValue, err := releases.EncodeReleaseRecord("service-release-projection", projection)
		if err != nil {
			clearMutations(mutations)
			return nil, err
		}
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: keys[2], Value: projectionValue})
	}
	return mutations, nil
}

func releaseAcknowledgedRuntime(
	marker releases.ReleasePublicationMarker,
	serviceID string,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	effect domain.EffectEvidence,
	result taskjournal.TaskResultRecord,
) ([]byte, error) {
	if marker.CandidateReleaseDescriptor.PlanID != task.PlanID ||
		hex.EncodeToString(marker.CandidateReleaseDescriptor.PlanHash) != task.PlanHash ||
		result.ExecutionEpoch != assignment.ExecutionEpoch {
		return nil, errs.New(errs.KindStateConflict, "release runtime source does not match acknowledged Task")
	}
	for _, candidate := range marker.PreparedRuntimes {
		if candidate.ReleaseID != effect.ObservedReleaseID || candidate.ServiceID != serviceID {
			continue
		}
		proxy, hasProxy := releaseProxyEvidence(result, candidate.ServiceID)
		recreate, hasRecreate := releaseRecreateEvidence(result, candidate.ServiceID)
		if hasProxy == hasRecreate || proxy.Compensated || recreate.Compensated {
			return nil, errs.New(errs.KindStateConflict, "release runtime requires uncompensated serving evidence")
		}
		observation := serviceruntimerecord.Observation{
			ReleaseID: proxy.ReleaseID, Target: proxy.Target,
			Proxy: &serviceruntimerecord.ProxyObservation{
				ProxyGeneration: proxy.ProxyGeneration,
				ProxyConfigHash: proxy.ConfigSHA256,
			},
		}
		if hasRecreate {
			observation = serviceruntimerecord.Observation{
				ReleaseID:          recreate.ReleaseID,
				Target:             recreate.Target,
				RecreateArtifactID: recreate.ArtifactID,
			}
		}
		record, err := serviceruntimerecord.AcknowledgeRelease(task.Owner.EnvironmentID, candidate,
			serviceruntimerecord.Acknowledgement{
				TaskID: task.ID, PlanID: task.PlanID, PlanHash: task.PlanHash, StepID: effect.StepID,
				AgentID: effect.AgentID, AssignmentID: assignment.AssignmentID,
				ExecutionEpoch: assignment.ExecutionEpoch, EffectDigest: effect.EffectDigest,
				RenderGeneration: uint64(task.RenderGeneration),
				AcknowledgedAt:   effect.AcknowledgedAt,
			}, observation)
		if err != nil {
			return nil, err
		}
		record.Configuration, err = taskRuntimeConfiguration(task)
		if err != nil {
			return nil, err
		}
		if err := serviceruntimerecord.Validate(record); err != nil {
			return nil, err
		}
		return releases.EncodeReleaseRecord("service-acknowledged-runtime", record)
	}
	return nil, errs.New(errs.KindStateConflict, "release prepared runtime is missing for acknowledged member")
}
