package taskplanning

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	localagentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/controller/workloadseal"
	"github.com/AlanD20/groundplane/internal/core"
	release "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type scriptPreparationAgentReader interface {
	GetSingleton(context.Context) (etcdstore.Versioned[localagentrecord.LocalAgentRecord], error)
}

// ScriptRunnerPreparation is private source-bound authority, never caller-supplied
// image text or Entry bindings. It contains metadata and hashes, not Entry values.
type ScriptRunnerPreparation struct {
	sourceDigest [sha256.Size]byte
	image        release.WorkloadSeal
	explicit     *agentpb.ScriptExplicitExecutionAuthority
	bindings     []*agentpb.ScriptRunnerEntryBinding
}

type ScriptRunnerPreparationService struct {
	artifacts *ScriptArtifactService
	agents    scriptPreparationAgentReader
	images    workloadseal.Resolver
}

func NewScriptRunnerPreparationService(
	artifacts *ScriptArtifactService, agents scriptPreparationAgentReader, images workloadseal.Resolver,
) (*ScriptRunnerPreparationService, error) {
	if artifacts == nil || agents == nil || images == nil {
		return nil, errs.New(errs.KindInternal, "Script runner preparation dependencies are required")
	}
	return &ScriptRunnerPreparationService{artifacts: artifacts, agents: agents, images: images}, nil
}

func (service *ScriptRunnerPreparationService) Prepare(
	ctx context.Context, sources etcd.ScriptExecutionSources,
) (ScriptRunnerPreparation, error) {
	if ctx == nil || service == nil || service.artifacts == nil || service.agents == nil || service.images == nil {
		return ScriptRunnerPreparation{}, errs.New(errs.KindInternal, "Script runner preparation is not configured")
	}
	entries, err := selectedScriptEntries(sources)
	if err != nil {
		return ScriptRunnerPreparation{}, err
	}
	explicit, err := scriptExplicitAuthority(sources)
	if err != nil {
		return ScriptRunnerPreparation{}, err
	}
	digest, err := scriptPreparationSourceDigest(sources, entries, explicit)
	if err != nil {
		return ScriptRunnerPreparation{}, err
	}
	prepared := ScriptRunnerPreparation{sourceDigest: digest, explicit: explicit}
	if explicit != nil {
		agent, err := service.agents.GetSingleton(ctx)
		if err != nil {
			return ScriptRunnerPreparation{}, err
		}
		seals, err := workloadseal.Resolve(ctx, service.images, agent.Record.ID, []workloadseal.Selection{{
			Requested: &workloadseal.Requested{Reference: explicit.Context.ImageReference, Replicas: 1},
		}})
		if err != nil {
			return ScriptRunnerPreparation{}, err
		}
		prepared.image = seals[0]
	}
	prepared.bindings, err = service.artifacts.buildScriptEntryBindings(ctx, sources, entries)
	if err != nil {
		return ScriptRunnerPreparation{}, err
	}
	return prepared, nil
}

func (prepared ScriptRunnerPreparation) validateSources(sources etcd.ScriptExecutionSources) error {
	entries, err := selectedScriptEntries(sources)
	if err != nil {
		return err
	}
	explicit, err := scriptExplicitAuthority(sources)
	if err != nil {
		return err
	}
	// An inherited runner without Entries needs no side-effect preparation.
	if prepared.sourceDigest == ([sha256.Size]byte{}) && explicit == nil && len(entries) == 0 {
		return nil
	}
	digest, err := scriptPreparationSourceDigest(sources, entries, explicit)
	if err != nil {
		return err
	}
	if digest != prepared.sourceDigest {
		return errs.New(errs.KindStateConflict, "Script runner sources changed after preparation")
	}
	return nil
}

func scriptExplicitAuthority(sources etcd.ScriptExecutionSources) (*agentpb.ScriptExplicitExecutionAuthority, error) {
	desired := sources.Script.Record.Desired.Execution
	if desired == nil || desired.Mode == core.ScriptExecutionInherited {
		return nil, nil
	}
	if sources.Revision <= 0 || sources.Script.Revision <= 0 || sources.Script.ReadRevision != sources.Revision ||
		sources.DesiredProjection.ReadRevision != sources.Revision {
		return nil, errs.New(errs.KindStateConflict, "explicit Script sources require one fixed read revision")
	}
	context := &agentpb.ScriptExplicitExecutionContext{
		ImageReference: desired.Image, User: desired.User, EntryIds: append([]string(nil), desired.EntryIDs...),
	}
	for _, grant := range desired.Volumes {
		context.Volumes = append(context.Volumes, &agentpb.ScriptExplicitVolumeGrant{
			VolumeId: grant.VolumeID, Target: grant.Target, ReadOnly: grant.ReadOnly,
		})
	}
	sort.Strings(context.EntryIds)
	sort.Slice(
		context.Volumes,
		func(i, j int) bool { return context.Volumes[i].VolumeId < context.Volumes[j].VolumeId },
	)
	digest, err := executionplan.ScriptExecutionContextDigest(context)
	if err != nil {
		return nil, err
	}
	return &agentpb.ScriptExplicitExecutionAuthority{
		Context: context, ContextSha256: digest, ScriptModRevision: uint64(sources.Script.Revision),
		ReleaseLocalImageId: sources.Release.Intent.CandidateWorkload.LocalImageID,
	}, nil
}

func scriptPreparationSourceDigest(
	sources etcd.ScriptExecutionSources, entries []entryrecord.Record, explicit *agentpb.ScriptExplicitExecutionAuthority,
) ([sha256.Size]byte, error) {
	metadata := make([]*agentpb.ScriptRunnerEntryBinding, 0, len(entries))
	for _, entry := range entries {
		binding, err := scriptEntryBindingMetadata(entry)
		if err != nil {
			return [sha256.Size]byte{}, err
		}
		metadata = append(metadata, binding)
	}
	sort.Slice(metadata, func(i, j int) bool { return metadata[i].EntryId < metadata[j].EntryId })
	encoded, err := json.Marshal(struct {
		ScriptID, EnvironmentID, ServiceID               string
		ReadRevision, ScriptRevision, ProjectionRevision int64
		ProjectionID                                     string
		RenderGeneration                                 uint64
		Explicit                                         *agentpb.ScriptExplicitExecutionAuthority
		Entries                                          []*agentpb.ScriptRunnerEntryBinding
	}{
		sources.Script.Record.Desired.ID, sources.Environment.Record.ID, sources.Service.Record.Desired.ID,
		sources.Revision, sources.Script.Revision, sources.DesiredProjection.Revision,
		sources.DesiredProjection.Record.RevisionID, sources.DesiredProjection.Record.RenderGeneration, explicit, metadata,
	})
	if err != nil {
		return [sha256.Size]byte{}, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	clear(encoded)
	return digest, nil
}
