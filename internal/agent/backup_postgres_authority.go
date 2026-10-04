package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"runtime"
	"strings"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/postgres16protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/postgres16execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type postgresStepAuthority struct {
	index         postgres16protocol.ManagedReleaseIndex
	selection     postgres16execution.Selection
	serviceID     string
	artifactID    string
	labelsSHA256  []byte
	labelCount    uint32
	repositorySHA []byte
	imageID       string
	maxPlaintext  uint64
	database      string
	role          string
}

func postgresAuthority(assignment taskassignment.Assignment, step *agentpb.BackupStepAuthority) (
	postgresStepAuthority, error,
) {
	if assignment.Plan == nil || assignment.BackupAuthority == nil || step == nil ||
		assignment.Plan.BackupScope == nil || assignment.Plan.BackupScope.EnvironmentId !=
		assignment.BackupAuthority.EnvironmentId {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	return postgresSourceAuthority(step, assignment.Plan.BackupScope.Services, assignment.Plan.Artifacts)
}

func postgresSourceAuthority(step *agentpb.BackupStepAuthority, services []*agentpb.BackupServiceFact,
	artifacts []*agentpb.ComposeArtifact,
) (postgresStepAuthority, error) {
	var serviceID, database, role string
	var catalog []byte
	var maxPlaintext uint64
	if capture := step.GetCapture().GetPostgres(); capture != nil {
		serviceID, database, role = capture.DatabaseServiceId, capture.DatabaseName, capture.RoleName
		catalog, maxPlaintext = capture.ManagedReleaseIndex, capture.MaxPlaintextBytes
	} else if restore := step.GetRestore().GetPostgres(); restore != nil {
		serviceID, database, role = restore.DatabaseServiceId, restore.DatabaseName, restore.RoleName
		catalog = restore.ManagedReleaseIndex
	} else {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	index, err := postgres16protocol.DecodeManagedReleaseIndex(catalog)
	if err != nil {
		return postgresStepAuthority{}, err
	}
	image, err := index.Select(nativePostgresArchitecture())
	if err != nil {
		return postgresStepAuthority{}, err
	}
	repositorySHA, err := hex.DecodeString(strings.TrimPrefix(image.ManifestDigest(), "sha256:"))
	if err != nil || len(repositorySHA) != sha256.Size {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range services {
		if candidate.ServiceId == serviceID {
			if fact != nil {
				return postgresStepAuthority{}, invalidAgentStaging()
			}
			fact = candidate
		}
	}
	if fact == nil || fact.PriorRuntimeIntent.GetKind() !=
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING ||
		len(fact.LocalImageIdSha256) != sha256.Size {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	var selected *agentpb.ComposeService
	var selectedArtifact *agentpb.ComposeArtifact
	for _, artifact := range artifacts {
		for _, candidate := range artifact.Services {
			if candidate.ServiceId == serviceID && candidate.ComposeName == fact.CurrentName {
				if selected != nil {
					return postgresStepAuthority{}, invalidAgentStaging()
				}
				selected, selectedArtifact = candidate, artifact
			}
		}
	}
	if selected == nil || selectedArtifact == nil ||
		selectedArtifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		selected.PostgresToolsImage != index.Image ||
		uint32(len(selected.ExpectedLabels)) != fact.RequiredLabelCount {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	labelsSHA, err := backupservicefact.LabelsDigest(selected.ExpectedLabels)
	if err != nil || !bytes.Equal(labelsSHA, fact.RequiredLabelsSha256) {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	labels, err := postgresLabelMap(selected.ExpectedLabels)
	if err != nil || selectedArtifact.ProjectName == "" || selected.ComposeName == "" {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	if value, present := labels["com.docker.compose.project"]; present && value != selectedArtifact.ProjectName {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	if value, present := labels["com.docker.compose.service"]; present && value != selected.ComposeName {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	// Compose adds these runtime labels; the sealed GP label digest excludes them.
	labels["com.docker.compose.project"] = selectedArtifact.ProjectName
	labels["com.docker.compose.service"] = selected.ComposeName
	volume, err := postgresDataVolume(selectedArtifact, selected)
	if err != nil {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	volumeLabels, err := postgresLabelMap(volume.ExpectedLabels)
	if err != nil {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	if value, present := volumeLabels["com.docker.compose.project"]; present && value != selectedArtifact.ProjectName {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	if value, present := volumeLabels["com.docker.compose.volume"]; present && value != volume.ComposeName {
		return postgresStepAuthority{}, invalidAgentStaging()
	}
	volumeLabels["com.docker.compose.project"] = selectedArtifact.ProjectName
	volumeLabels["com.docker.compose.volume"] = volume.ComposeName
	return postgresStepAuthority{
		index: index, selection: postgres16execution.Selection{Labels: labels,
			ImageReference: selected.ImageReference, ImageID: "sha256:" + hex.EncodeToString(fact.LocalImageIdSha256),
			VolumeName: volume.DockerName, VolumeLabels: volumeLabels},
		serviceID: serviceID, artifactID: selectedArtifact.ArtifactId, labelsSHA256: labelsSHA,
		labelCount:    fact.RequiredLabelCount,
		repositorySHA: repositorySHA, imageID: "sha256:" + hex.EncodeToString(fact.LocalImageIdSha256),
		maxPlaintext: maxPlaintext,
		database:     database, role: role,
	}, nil
}

func postgresDataVolume(artifact *agentpb.ComposeArtifact,
	service *agentpb.ComposeService,
) (*agentpb.ComposeVolume, error) {
	if artifact == nil || service == nil || !bytes.Equal(artifact.YamlSha256, sha256Digest(artifact.CanonicalYaml)) {
		return nil, invalidAgentStaging()
	}
	var document struct {
		Services map[string]struct {
			Volumes []struct {
				Type     string `yaml:"type"`
				Source   string `yaml:"source"`
				Target   string `yaml:"target"`
				ReadOnly bool   `yaml:"read_only"`
			} `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(artifact.CanonicalYaml, &document); err != nil {
		return nil, invalidAgentStaging()
	}
	config, exists := document.Services[service.ComposeName]
	if !exists || len(config.Volumes) != 2 {
		return nil, invalidAgentStaging()
	}
	var dataSource string
	toolsSource, err := postgres16protocol.ToolsDirectory(service.PostgresToolsImage)
	if err != nil {
		return nil, invalidAgentStaging()
	}
	for _, item := range config.Volumes {
		switch item.Target {
		case "/var/lib/postgresql/data":
			if item.Type != "volume" || item.Source == "" || dataSource != "" {
				return nil, invalidAgentStaging()
			}
			dataSource = item.Source
		case postgres16protocol.HelperDirectoryPath:
			if item.Type != "bind" || item.Source != toolsSource || !item.ReadOnly {
				return nil, invalidAgentStaging()
			}
		default:
			return nil, invalidAgentStaging()
		}
	}
	if dataSource == "" {
		return nil, invalidAgentStaging()
	}
	var volume *agentpb.ComposeVolume
	for _, candidate := range artifact.Volumes {
		if candidate.ComposeName == dataSource {
			if volume != nil {
				return nil, invalidAgentStaging()
			}
			volume = candidate
		}
	}
	if volume == nil {
		return nil, invalidAgentStaging()
	}
	return volume, nil
}

func sha256Digest(content []byte) []byte {
	digest := sha256.Sum256(content)
	return digest[:]
}

func postgresLabelMap(pairs []*agentpb.LabelPair) (map[string]string, error) {
	if len(pairs) == 0 {
		return nil, invalidAgentStaging()
	}
	labels := make(map[string]string, len(pairs))
	for _, pair := range pairs {
		if pair == nil || pair.Key == "" {
			return nil, invalidAgentStaging()
		}
		if _, exists := labels[pair.Key]; exists {
			return nil, invalidAgentStaging()
		}
		labels[pair.Key] = pair.Value
	}
	return labels, nil
}

func newPostgresNonce() (postgres16protocol.Nonce, error) {
	var nonce postgres16protocol.Nonce
	if _, err := rand.Read(nonce[:]); err != nil || nonce == (postgres16protocol.Nonce{}) {
		return postgres16protocol.Nonce{}, errs.New(errs.KindInternal, "PostgreSQL execution nonce is unavailable")
	}
	return nonce, nil
}

func postgresContainerCheckpoint(authority postgresStepAuthority,
	container postgres16execution.Container,
) (*agentpb.BackupPostgresContainerObserved, error) {
	value := &agentpb.BackupPostgresContainerObserved{
		ServiceId: authority.serviceID, ContainerId: container.ID,
		RepositoryDigest:     append([]byte(nil), authority.repositorySHA...),
		ObservedLabelCount:   authority.labelCount,
		ObservedLabelsSha256: append([]byte(nil), authority.labelsSHA256...),
	}
	imageDigest, err := hex.DecodeString(strings.TrimPrefix(authority.imageID, "sha256:"))
	if err != nil || len(imageDigest) != sha256.Size {
		return nil, invalidAgentStaging()
	}
	value.DatabaseImageIdSha256 = imageDigest
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(value)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	value.ObservationSha256 = digest[:]
	return value, nil
}

func publishPostgresContainer(ctx context.Context, publisher *backupStepCheckpoint,
	resume *agentpb.BackupPostgresContainerObserved, authority postgresStepAuthority,
	container postgres16execution.Container,
) error {
	observed, err := postgresContainerCheckpoint(authority, container)
	if err != nil {
		return err
	}
	if resume != nil {
		if !proto.Equal(resume, observed) {
			return invalidAgentStaging()
		}
		return nil
	}
	return publisher.publish(ctx, &agentpb.BackupCheckpointRequest{
		Checkpoint: &agentpb.BackupCheckpointRequest_PostgresContainerObserved{
			PostgresContainerObserved: observed},
	})
}

func nativePostgresArchitecture() string { return runtime.GOARCH }
