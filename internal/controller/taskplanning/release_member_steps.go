package taskplanning

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const releaseMemberStepTimeoutSeconds = uint32(5 * 60)

func buildReleaseMemberSteps(
	task etcd.TaskRecord,
	input etcd.ReleaseTaskRenderInput,
	priorArtifacts map[string]*agentpb.ComposeArtifact,
) ([]*agentpb.ExecutionStep, error) {
	first := input.Members[0].Render
	steps := make([]*agentpb.ExecutionStep, 0, len(task.Steps))
	for index, member := range input.Members {
		base := index * 5
		applyID, healthID, switchID := task.Steps[base].ID, task.Steps[base+1].ID, task.Steps[base+2].ID
		probeID, compensateID := task.Steps[base+3].ID, task.Steps[base+4].ID
		if ids.Validate(ids.KindStep, applyID) != nil || ids.Validate(ids.KindStep, healthID) != nil ||
			ids.Validate(ids.KindStep, switchID) != nil || ids.Validate(ids.KindStep, probeID) != nil ||
			ids.Validate(ids.KindStep, compensateID) != nil {
			return nil, errs.New(errs.KindInternal, "release Task step identity is invalid")
		}
		priorReleaseID := member.Intent.PriorServingReleaseID
		if member.Render.Strategy == domain.StrategyRecreate {
			if priorArtifacts[member.Render.ServiceID] == nil {
				if priorReleaseID != "" || member.Render.PriorArtifactID != "" {
					return nil, errs.New(errs.KindInternal, "recreate prior artifact is missing")
				}
				steps = append(
					steps,
					&agentpb.ExecutionStep{
						StepId:         applyID,
						TimeoutSeconds: releaseMemberStepTimeoutSeconds,
						Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
						Payload: &agentpb.ExecutionStep_ComposeApply{
							ComposeApply: &agentpb.ComposeApply{
								ArtifactId:     first.ArtifactID,
								ServiceIds:     []string{member.Render.ServiceID},
								ForceRecreate:  true,
								NoDependencies: true,
							},
						},
					},
					&agentpb.ExecutionStep{
						StepId:             healthID,
						TimeoutSeconds:     releaseMemberStepTimeoutSeconds,
						Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
						PrerequisiteStepId: applyID,
						Payload: &agentpb.ExecutionStep_WaitHealthy{
							WaitHealthy: &agentpb.WaitHealthy{
								ArtifactId: first.ArtifactID,
								ServiceIds: []string{member.Render.ServiceID},
							},
						},
					},
					&agentpb.ExecutionStep{
						StepId:             switchID,
						TimeoutSeconds:     releaseMemberStepTimeoutSeconds,
						Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
						PrerequisiteStepId: healthID,
						Payload: &agentpb.ExecutionStep_ServiceRecreateAcknowledge{
							ServiceRecreateAcknowledge: &agentpb.ServiceRecreateAcknowledge{
								ArtifactId: first.ArtifactID,
								ServiceId:  member.Render.ServiceID,
								ReleaseId:  member.Intent.ID,
							},
						},
					},
					&agentpb.ExecutionStep{
						StepId:         probeID,
						TimeoutSeconds: releaseMemberStepTimeoutSeconds,
						Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
						Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
							CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
								CandidateArtifactId: first.ArtifactID,
								ServiceId:           member.Render.ServiceID,
								CandidateReleaseId:  member.Intent.ID,
							},
						},
					},
					&agentpb.ExecutionStep{
						StepId:             compensateID,
						TimeoutSeconds:     releaseMemberStepTimeoutSeconds,
						Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
						PrerequisiteStepId: applyID,
						Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
							CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
								CandidateArtifactId: first.ArtifactID,
								ServiceId:           member.Render.ServiceID,
								CandidateReleaseId:  member.Intent.ID,
							},
						},
					},
				)
				if index > 0 {
					steps[len(steps)-5].PrerequisiteStepId = task.Steps[base-3].ID
				}
				continue
			}
			steps = append(
				steps,
				&agentpb.ExecutionStep{
					StepId:         applyID,
					TimeoutSeconds: releaseMemberStepTimeoutSeconds,
					Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
					Payload: &agentpb.ExecutionStep_ComposeRemove{
						ComposeRemove: &agentpb.ComposeRemove{
							ArtifactId: member.Render.PriorArtifactID,
							ServiceIds: []string{member.Render.ServiceID},
						},
					},
				},
				&agentpb.ExecutionStep{
					StepId:             healthID,
					TimeoutSeconds:     releaseMemberStepTimeoutSeconds,
					Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
					PrerequisiteStepId: applyID,
					Payload: &agentpb.ExecutionStep_ComposeApply{
						ComposeApply: &agentpb.ComposeApply{
							ArtifactId:     first.ArtifactID,
							ServiceIds:     []string{member.Render.ServiceID},
							ForceRecreate:  true,
							NoDependencies: true,
						},
					},
				},
				&agentpb.ExecutionStep{
					StepId:             switchID,
					TimeoutSeconds:     releaseMemberStepTimeoutSeconds,
					Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
					PrerequisiteStepId: healthID,
					Payload: &agentpb.ExecutionStep_ServiceRecreateAcknowledge{
						ServiceRecreateAcknowledge: &agentpb.ServiceRecreateAcknowledge{
							ArtifactId: first.ArtifactID,
							ServiceId:  member.Render.ServiceID,
							ReleaseId:  member.Intent.ID,
						},
					},
				},
				&agentpb.ExecutionStep{
					StepId:         probeID,
					TimeoutSeconds: releaseMemberStepTimeoutSeconds,
					Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
					Payload: &agentpb.ExecutionStep_ServiceRecreateProbe{
						ServiceRecreateProbe: &agentpb.ServiceRecreateProbe{
							CandidateArtifactId: first.ArtifactID,
							PriorArtifactId:     member.Render.PriorArtifactID,
							ServiceId:           member.Render.ServiceID,
							CandidateReleaseId:  member.Intent.ID,
							PriorReleaseId:      priorReleaseID,
						},
					},
				},
				&agentpb.ExecutionStep{
					StepId:             compensateID,
					TimeoutSeconds:     releaseMemberStepTimeoutSeconds,
					Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
					PrerequisiteStepId: healthID,
					Payload: &agentpb.ExecutionStep_ServiceRecreateCompensate{
						ServiceRecreateCompensate: &agentpb.ServiceRecreateCompensate{
							ArtifactId:          member.Render.PriorArtifactID,
							CandidateArtifactId: first.ArtifactID,
							ServiceId:           member.Render.ServiceID,
							CandidateReleaseId:  member.Intent.ID,
							PriorReleaseId:      priorReleaseID,
							PriorTarget:         string(member.Render.PriorTarget),
							Enabled:             input.Operation.FailurePolicy == domain.OnFailureSwitchBack,
						},
					},
				},
			)
			if index > 0 {
				steps[len(steps)-5].PrerequisiteStepId = task.Steps[base-3].ID
			}
			continue
		}
		candidateConfig, err := domain.RenderProxyConfig(
			member.Render.ServiceName,
			member.Intent.ID,
			member.Render.CandidateTarget,
			member.Render.ProxyGeneration,
			member.Render.ProxyPorts,
		)
		if err != nil {
			return nil, err
		}
		steps = append(steps,
			&agentpb.ExecutionStep{
				StepId: applyID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ComposeWorkloadApply{
					ComposeWorkloadApply: &agentpb.ComposeWorkloadApply{
						ArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, Target: string(member.Render.CandidateTarget),
						EnsureProxy: member.Render.PriorStrategy == domain.StrategyRecreate,
					},
				},
			},
			&agentpb.ExecutionStep{
				StepId: healthID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_WaitWorkloadHealthy{WaitWorkloadHealthy: &agentpb.WaitWorkloadHealthy{
					ArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, Target: string(member.Render.CandidateTarget),
				}},
			},
			&agentpb.ExecutionStep{
				StepId: switchID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ServiceProxySwitch{ServiceProxySwitch: &agentpb.ServiceProxySwitch{
					CandidateArtifactId: first.ArtifactID, PriorArtifactId: member.Render.PriorArtifactID,
					ServiceId:  member.Render.ServiceID,
					FromTarget: string(member.Render.PriorTarget), ToTarget: string(member.Render.CandidateTarget),
					ProxyGeneration: member.Render.ProxyGeneration, ConfigJson: candidateConfig.JSON,
					ConfigSha256: candidateConfig.SHA256[:], ReleaseId: member.Intent.ID,
				}},
			},
		)
		if priorReleaseID == "" {
			if member.Render.PriorWorkload != nil || member.Render.PriorArtifactID != "" ||
				member.Render.PriorProxyGeneration != 0 || member.Render.PriorProxyDigest != "" {
				return nil, errs.New(errs.KindInternal, "first blue-green release has unexpected predecessor authority")
			}
			steps = append(
				steps,
				&agentpb.ExecutionStep{StepId: probeID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
					Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
					Payload: &agentpb.ExecutionStep_CandidateRestorationProbe{
						CandidateRestorationProbe: &agentpb.CandidateRestorationProbe{
							CandidateArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID,
						},
					}},
				&agentpb.ExecutionStep{
					StepId:             compensateID,
					TimeoutSeconds:     releaseMemberStepTimeoutSeconds,
					PrerequisiteStepId: applyID,
					Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
					Payload: &agentpb.ExecutionStep_CandidateRestorationCompensate{
						CandidateRestorationCompensate: &agentpb.CandidateRestorationCompensate{
							CandidateArtifactId: first.ArtifactID, ServiceId: member.Render.ServiceID, CandidateReleaseId: member.Intent.ID,
						},
					},
				},
			)
			if index > 0 {
				steps[len(steps)-5].PrerequisiteStepId = task.Steps[base-3].ID
			}
			continue
		}
		priorConfig, err := releasePriorProxyConfig(member, priorArtifacts[member.Render.ServiceID])
		if err != nil {
			return nil, err
		}
		steps = append(steps,
			&agentpb.ExecutionStep{
				StepId: probeID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
				Payload: &agentpb.ExecutionStep_ServiceProxyProbe{ServiceProxyProbe: &agentpb.ServiceProxyProbe{
					CandidateArtifactId: first.ArtifactID, PriorArtifactId: member.Render.PriorArtifactID,
					ServiceId: member.Render.ServiceID,
					ExpectedTarget: string(
						member.Render.PriorTarget,
					), ProxyGeneration: member.Render.PriorProxyGeneration,
					ConfigJson: priorConfig.JSON, ConfigSha256: priorConfig.SHA256[:], ReleaseId: priorReleaseID,
					AlternateTarget: string(
						member.Render.CandidateTarget,
					), AlternateProxyGeneration: member.Render.ProxyGeneration,
					AlternateConfigJson: candidateConfig.JSON, AlternateConfigSha256: candidateConfig.SHA256[:],
					AlternateReleaseId: member.Intent.ID,
				}},
			},
			&agentpb.ExecutionStep{
				StepId: compensateID, TimeoutSeconds: releaseMemberStepTimeoutSeconds,
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
				PrerequisiteStepId: applyID,
				Payload: &agentpb.ExecutionStep_ServiceProxyCompensate{
					ServiceProxyCompensate: &agentpb.ServiceProxyCompensate{
						CandidateArtifactId: first.ArtifactID, PriorArtifactId: member.Render.PriorArtifactID,
						ServiceId: member.Render.ServiceID,
						CandidateTarget: string(
							member.Render.CandidateTarget,
						), PriorTarget: string(member.Render.PriorTarget),
						ProxyGeneration: member.Render.PriorProxyGeneration, ConfigJson: priorConfig.JSON,
						ConfigSha256: priorConfig.SHA256[:], PriorReleaseId: priorReleaseID,
						Enabled: input.Operation.FailurePolicy == domain.OnFailureSwitchBack,
					},
				},
			},
		)
		if index > 0 {
			steps[len(steps)-5].PrerequisiteStepId = task.Steps[base-3].ID
		}
	}
	bindRetainedOrdinaryRecovery(input.Members, steps)
	return steps, nil
}
