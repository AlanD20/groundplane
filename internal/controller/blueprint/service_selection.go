package blueprint

import (
	"bytes"
	"context"
	"maps"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/blueprintparser"
	"github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
)

// Selection is an execution and authoring boundary, not a UI filter. Reject
// unrelated changes before any durable claim or executable preparation.
func (service *Service) validateServiceSelection(
	ctx context.Context,
	environmentID string,
	bundle core.BlueprintBundle,
	parsed blueprintparser.Result,
) error {
	if bundle.Service == "" {
		return nil
	}
	if strings.TrimSpace(bundle.Service) != bundle.Service {
		return errs.New(errs.KindValidationFailed, "selected Blueprint Service name is invalid")
	}
	if _, exists := parsed.Project.Services[bundle.Service]; !exists {
		if _, disabled := parsed.Project.DisabledServices[bundle.Service]; !disabled {
			return errs.New(errs.KindValidationFailed, "selected Service is not declared in the Blueprint")
		}
	}
	for _, group := range parsed.Extensions.ReleaseGroups {
		for _, name := range group.Services {
			if name == bundle.Service {
				return errs.New(errs.KindResourceInUse, "selected Service belongs to a Release Group; use group Deploy")
			}
		}
	}
	snapshot, err := service.loadEnvironmentBlueprintSnapshot(ctx, environmentID)
	if err != nil {
		return err
	}
	if !snapshot.hasHead {
		return errs.New(
			errs.KindStateConflict,
			"apply the Environment Blueprint before selecting an individual Service",
		)
	}
	attaches, _, err := service.listBlueprintAttaches(ctx, environmentID)
	if err != nil {
		return err
	}
	current, err := service.environmentBlueprintAuthoringDocument(ctx, snapshot, attaches)
	if err != nil {
		return err
	}
	currentDocument, err := blueprintparser.MarshalAuthoringDocument(current)
	if err != nil {
		return err
	}
	currentBundle := core.BlueprintBundle{RootPath: "groundplane.selection.yaml"}
	for _, file := range snapshot.desired.Record.Input.RuntimeFiles {
		currentBundle.Files = append(currentBundle.Files, core.BlueprintFile{Path: file.Path, Content: file.Content})
	}
	for {
		if _, exists := currentBundle.File(currentBundle.RootPath); !exists {
			break
		}
		currentBundle.RootPath = "_" + currentBundle.RootPath
	}
	currentBundle.ComposeSources = []string{currentBundle.RootPath}
	currentBundle.Files = append(
		currentBundle.Files,
		core.BlueprintFile{Path: currentBundle.RootPath, Content: currentDocument},
	)
	prior, err := blueprintparser.Parse(ctx, blueprintparser.EnvironmentScope{
		EnvironmentID: environmentID, Tenant: snapshot.tenant.Record.Slug,
		Project: snapshot.project.Record.Slug, Environment: snapshot.environment.Record.Name,
	}, currentBundle)
	if err != nil {
		return err
	}
	current.Compose, err = unselectedCompose(prior.Project, bundle.Service)
	if err != nil {
		return err
	}
	current.Compose, err = composerender.AuthoringComposeVolumes(
		current.Compose,
		snapshot.volumes,
		snapshot.environment.Record.VolumeDir,
	)
	if err != nil {
		return err
	}
	compose, err := unselectedCompose(parsed.Project, bundle.Service)
	if err != nil {
		return err
	}
	compose, err = composerender.AuthoringComposeVolumes(
		compose,
		snapshot.volumes,
		snapshot.environment.Record.VolumeDir,
	)
	if err != nil {
		return err
	}
	current = selectionAuthoringDocument(prior, current.Compose)
	candidate := selectionAuthoringDocument(parsed, compose)
	before, err := blueprintparser.MarshalAuthoringDocument(current)
	if err != nil {
		return err
	}
	after, err := blueprintparser.MarshalAuthoringDocument(candidate)
	if err != nil {
		return err
	}
	if !bytes.Equal(before, after) {
		return errs.New(
			errs.KindValidationFailed,
			"selected-Service Apply changes other Services or shared resources; use full Blueprint Apply",
		)
	}
	files, err := blueprintparser.SelectRuntimeFiles(
		parsed.Project,
		bundle.Files,
		snapshot.desired.Record.Input.RuntimeFiles,
	)
	if err != nil {
		return err
	}
	previousFiles := snapshot.desired.Record.Input.RuntimeFiles
	if len(files) != len(previousFiles) {
		return errs.New(
			errs.KindValidationFailed,
			"selected-Service Apply changes companion files; use full Blueprint Apply",
		)
	}
	for index, file := range files {
		if file.Path != previousFiles[index].Path || !bytes.Equal(file.Content, previousFiles[index].Content) {
			return errs.New(
				errs.KindValidationFailed,
				"selected-Service Apply changes companion files; use full Blueprint Apply",
			)
		}
	}
	return nil
}

func selectionAuthoringDocument(parsed blueprintparser.Result, compose []byte) blueprintparser.AuthoringDocument {
	return blueprintparser.AuthoringDocument{
		Envelope: parsed.Envelope, NetworkPool: parsed.Extensions.NetworkPool, Compose: compose,
		Requires: parsed.Extensions.Requires, Attachments: parsed.Extensions.Attachments,
		Entries: parsed.Extensions.Entries, Routes: parsed.Extensions.Routes,
		Scripts: parsed.Extensions.Scripts, Components: parsed.Extensions.Components,
		Backup: parsed.Extensions.Backup, ReleaseGroups: parsed.Extensions.ReleaseGroups,
	}
}

func selectedServiceMaterializations(
	records []taskmaterialization.Record,
	steps []*agentpb.ExecutionStep,
	serviceID string,
) ([]taskmaterialization.Record, []*agentpb.ExecutionStep) {
	selected := make([]taskmaterialization.Record, 0)
	byStep := make(map[string]bool)
	for _, record := range records {
		if record.ServiceID == serviceID ||
			record.ServiceID == "" && record.OutputKind == taskmaterialization.OutputGeneratedEnvironment {
			selected = append(selected, record)
			byStep[record.StepID] = true
		}
	}
	selectedSteps := make([]*agentpb.ExecutionStep, 0, len(selected))
	for _, step := range steps {
		if byStep[step.StepId] {
			selectedSteps = append(selectedSteps, step)
		}
	}
	return selected, selectedSteps
}

func unselectedCompose(project *composetypes.Project, selected string) ([]byte, error) {
	remaining := *project
	remaining.Services = maps.Clone(project.Services)
	remaining.DisabledServices = maps.Clone(project.DisabledServices)
	delete(remaining.Services, selected)
	delete(remaining.DisabledServices, selected)
	return composerender.MarshalNormalizedEnvironmentProject(&remaining)
}
