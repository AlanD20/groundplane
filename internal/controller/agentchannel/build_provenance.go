package agentchannel

import (
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func readyBuildProvenance(value *agentpb.BuildProvenance) (version.BuildProvenance, error) {
	if value == nil {
		return version.BuildProvenance{}, nil
	}
	source := version.BuildProvenance{Kind: value.GetKind(), Commit: value.GetCommit()}
	if executionplan.RejectUnknown(value) != nil || !source.Known() || source.Validate() != nil {
		return version.BuildProvenance{}, errs.New(errs.KindValidationFailed, "agent Ready build provenance is invalid")
	}
	return source, nil
}
