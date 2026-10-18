package serviceruntimerecord

import (
	"context"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeconfiguration"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// ConfigurationFiles selects only files referenced by this acknowledged runtime,
// including shared files and retained release members, from the operation's
// captured file snapshot. File updates can be newer than the last deployment.
// The caller authenticates that snapshot's immutable reference before selection.
func ConfigurationFiles(ctx context.Context, record Record, snapshot runtimeconfiguration.Snapshot) ([]taskmaterialization.Record, error) {
	if err := Validate(record); err != nil {
		return nil, err
	}
	if snapshot.EnvironmentID != record.EnvironmentID || snapshot.Generation == 0 {
		return nil, errs.New(errs.KindStateConflict, "Runtime configuration selection requires its captured snapshot")
	}
	selected := make(map[string]bool)
	for _, raw := range [][]byte{record.Runtime.CurrentArtifact, record.Runtime.RetainedPriorArtifact} {
		if len(raw) == 0 {
			continue
		}
		artifact := &agentpb.ComposeArtifact{}
		if err := proto.Unmarshal(raw, artifact); err != nil {
			return nil, errs.Wrap(errs.KindStateConflict, err)
		}
		uses, err := executionplan.RuntimeConfigurationFiles(ctx, artifact)
		if err != nil {
			return nil, err
		}
		for _, use := range uses {
			found := false
			for _, file := range snapshot.Files {
				if file.Destination != use.Destination && !(use.Bind && strings.HasPrefix(file.Destination, use.Destination+"/")) {
					continue
				}
				if file.Source.Kind == taskmaterialization.SourceRemoval {
					continue
				}
				if file.EnvironmentID != record.EnvironmentID || taskmaterialization.ValidateRecord(file, snapshot.Generation) != nil {
					return nil, errs.New(errs.KindStateConflict, "Runtime configuration contains an invalid file source")
				}
				selected[file.Destination], found = true, true
			}
			if !use.Bind && !found {
				return nil, errs.New(errs.KindStateConflict, "Captured runtime file source is unavailable")
			}
		}
	}
	files := make([]taskmaterialization.Record, 0, len(selected))
	for _, file := range snapshot.Files {
		if selected[file.Destination] {
			files = append(files, file)
		}
	}
	return taskmaterialization.Clone(files), nil
}
