package controller

import (
	"strconv"
	"strings"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// projectScriptRunnerContext keeps inherited consumer projection separate from
// the explicit minimal configuration; ambient fields never enter that branch.
func projectScriptRunnerContext(
	input ManualScriptPlanInput, service, desired composetypes.ServiceConfig,
) (*agentpb.ScriptRunnerProjection, error) {
	if err := input.Preparation.validateSources(input.Sources); err != nil {
		return nil, err
	}
	if explicit := input.Preparation.explicit; explicit != nil {
		uid, gid, err := parseScriptNumericUser(explicit.Context.User)
		if err != nil {
			return nil, err
		}
		mounts := make([]*agentpb.ScriptRunnerMount, 0, len(explicit.Context.Volumes))
		for _, grant := range explicit.Context.Volumes {
			source := existingScriptSourceAuthority(input.Sources.DesiredProjection.Revision)
			if input.Candidate != nil {
				source = blueprintScriptStagedSourceAuthority(input.Candidate, input.Candidate.ProjectionSHA256)
			}
			mounts = append(mounts, &agentpb.ScriptRunnerMount{
				SourceId: grant.VolumeId, Source: source,
				RenderedMount: &agentpb.ScriptMount{
					Type: "volume", Source: "gp_vol_" + strings.ToLower(grant.VolumeId), Target: grant.Target,
					ReadOnly: grant.ReadOnly, VolumeNoCopy: true,
				},
			})
		}
		return projectScriptRunner(input, composetypes.ServiceConfig{
			Image: input.Preparation.image.LocalImageID, WorkingDir: "/",
		}, uid, gid, nil, mounts, input.Preparation.bindings)
	}
	if err := validateScriptServiceDisposition(service); err != nil {
		return nil, err
	}
	uid, gid, err := parseScriptNumericUser(service.User)
	if err != nil {
		return nil, err
	}
	networks, err := projectScriptNetworks(service, input.Sources, input.Candidate)
	if err != nil {
		return nil, err
	}
	mounts, err := projectScriptMounts(desired, input.Sources, input.Candidate)
	if err != nil {
		return nil, err
	}
	return projectScriptRunner(input, service, uid, gid, networks, mounts, input.Preparation.bindings)
}

func validateScriptServiceDisposition(service composetypes.ServiceConfig) error {
	if service.Configs != nil || service.EnvFiles != nil || service.Secrets != nil {
		return errs.New(
			errs.KindValidationFailed,
			"Script runner Entry artifacts must be resolved through the private assignment channel",
		)
	}
	if field := scriptServiceUnsupportedField(service); field != "" {
		return unsupportedScriptServiceField(field)
	}
	return nil
}

func unsupportedScriptServiceField(name string) error {
	return errs.Newf(errs.KindValidationFailed, "Script runner does not support Compose service field %s", name)
}

func scriptServiceUnsupportedField(service composetypes.ServiceConfig) string {
	switch {
	case service.Annotations != nil:
		return "Annotations"
	case service.Build != nil:
		return "Build"
	case service.Develop != nil:
		return "Develop"
	case service.CapAdd != nil:
		return "CapAdd"
	case service.CgroupParent != "":
		return "CgroupParent"
	case service.Cgroup != "":
		return "Cgroup"
	case service.CredentialSpec != nil:
		return "CredentialSpec"
	case service.DeviceCgroupRules != nil:
		return "DeviceCgroupRules"
	case service.Devices != nil:
		return "Devices"
	case service.Dockerfile != "":
		return "Dockerfile"
	case service.DomainName != "":
		return "DomainName"
	case service.Provider != nil:
		return "Provider"
	case service.Extends != nil:
		return "Extends"
	case service.ExternalLinks != nil:
		return "ExternalLinks"
	case service.Gpus != nil:
		return "Gpus"
	case service.Hostname != "":
		return "Hostname"
	case service.Ipc != "":
		return "Ipc"
	case service.MacAddress != "":
		return "MacAddress"
	case service.Models != nil:
		return "Models"
	case service.LabelFiles != nil:
		return "LabelFiles"
	case service.Links != nil:
		return "Links"
	case service.Net != "":
		return "Net"
	case service.NetworkMode != "":
		return "NetworkMode"
	case service.Pid != "":
		return "Pid"
	case service.Privileged:
		return "Privileged"
	case service.UserNSMode != "":
		return "UserNSMode"
	case service.Uts != "":
		return "Uts"
	case service.UseAPISocket:
		return "UseAPISocket"
	case service.VolumeDriver != "":
		return "VolumeDriver"
	case service.VolumesFrom != nil:
		return "VolumesFrom"
	case service.PreStart != nil:
		return "PreStart"
	case service.PostStart != nil:
		return "PostStart"
	case service.PreStop != nil:
		return "PreStop"
	case service.Extensions != nil:
		return "Extensions"
	default:
		return ""
	}
}

func parseScriptNumericUser(value string) (uint32, uint32, error) {
	if value == "" {
		return 0, 0, nil
	}
	parts := strings.Split(value, ":")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, 0, errs.New(errs.KindValidationFailed, "Script runner user must be an explicit numeric uid:gid")
	}
	uid, err := strconv.ParseUint(parts[0], 10, 32)
	if err != nil {
		return 0, 0, errs.New(errs.KindValidationFailed, "Script runner uid is invalid")
	}
	gid, err := strconv.ParseUint(parts[1], 10, 32)
	if err != nil {
		return 0, 0, errs.New(errs.KindValidationFailed, "Script runner gid is invalid")
	}
	return uint32(uid), uint32(gid), nil
}
