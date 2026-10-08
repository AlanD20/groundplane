package agent

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/backupservicefact"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/infra/docker/mysql84execution"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
	"gopkg.in/yaml.v3"
)

type mysqlStepAuthority struct {
	selection    mysql84execution.Selection
	serviceID    string
	labelsSHA256 []byte
	labelCount   uint32
	imageRefSHA  []byte
	imageID      string
	maxPlaintext uint64
	database     string
	role         string
}

func mysqlAuthority(assignment taskassignment.Assignment, step *agentpb.BackupStepAuthority) (
	mysqlStepAuthority, error,
) {
	if assignment.Plan == nil || assignment.BackupAuthority == nil || step == nil ||
		assignment.Plan.BackupScope == nil ||
		assignment.Plan.BackupScope.EnvironmentId != assignment.BackupAuthority.EnvironmentId {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	return mysqlSourceAuthority(step, assignment.Plan.BackupScope.Services, assignment.Plan.Artifacts)
}

func mysqlSourceAuthority(step *agentpb.BackupStepAuthority, services []*agentpb.BackupServiceFact,
	artifacts []*agentpb.ComposeArtifact,
) (mysqlStepAuthority, error) {
	var serviceID, database, role string
	var maximum uint64
	var expectedReferenceSHA []byte
	if capture := step.GetCapture().GetMysql(); capture != nil {
		serviceID, database, role, maximum = capture.DatabaseServiceId, capture.DatabaseName,
			capture.RoleName, capture.MaxPlaintextBytes
		expectedReferenceSHA = capture.DatabaseImageReferenceSha256
	} else if restore := step.GetRestore().GetMysql(); restore != nil {
		serviceID, database, role = restore.DatabaseServiceId, restore.DatabaseName, restore.RoleName
		expectedReferenceSHA = restore.DatabaseImageReferenceSha256
	} else {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	var fact *agentpb.BackupServiceFact
	for _, candidate := range services {
		if candidate.ServiceId == serviceID {
			if fact != nil {
				return mysqlStepAuthority{}, invalidAgentStaging()
			}
			fact = candidate
		}
	}
	if fact == nil || fact.PriorRuntimeIntent.GetKind() !=
		agentpb.BackupServiceRuntimeIntent_BACKUP_SERVICE_RUNTIME_INTENT_RUNNING ||
		len(fact.LocalImageIdSha256) != sha256.Size {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	var selected *agentpb.ComposeService
	var selectedArtifact *agentpb.ComposeArtifact
	for _, artifact := range artifacts {
		for _, candidate := range artifact.Services {
			if candidate.ServiceId == serviceID && candidate.ComposeName == fact.CurrentName {
				if selected != nil {
					return mysqlStepAuthority{}, invalidAgentStaging()
				}
				selected, selectedArtifact = candidate, artifact
			}
		}
	}
	if selected == nil || selectedArtifact == nil || selected.PostgresToolsImage != "" ||
		selectedArtifact.OwnerKind != agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_ENVIRONMENT ||
		uint32(len(selected.ExpectedLabels)) != fact.RequiredLabelCount || selected.ImageReference == "" {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	referenceSHA := sha256.Sum256([]byte(selected.ImageReference))
	if !bytes.Equal(referenceSHA[:], expectedReferenceSHA) {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	labelsSHA, err := backupservicefact.LabelsDigest(selected.ExpectedLabels)
	if err != nil || !bytes.Equal(labelsSHA, fact.RequiredLabelsSha256) {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	labels, err := postgresLabelMap(selected.ExpectedLabels)
	if err != nil || selectedArtifact.ProjectName == "" || selected.ComposeName == "" {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	if value, present := labels["com.docker.compose.project"]; present && value != selectedArtifact.ProjectName {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	if value, present := labels["com.docker.compose.service"]; present && value != selected.ComposeName {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	labels["com.docker.compose.project"] = selectedArtifact.ProjectName
	labels["com.docker.compose.service"] = selected.ComposeName
	volume, err := mysqlDataVolume(selectedArtifact, selected)
	if err != nil {
		return mysqlStepAuthority{}, err
	}
	volumeLabels, err := postgresLabelMap(volume.ExpectedLabels)
	if err != nil {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	if value, present := volumeLabels["com.docker.compose.project"]; present && value != selectedArtifact.ProjectName {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	if value, present := volumeLabels["com.docker.compose.volume"]; present && value != volume.ComposeName {
		return mysqlStepAuthority{}, invalidAgentStaging()
	}
	volumeLabels["com.docker.compose.project"] = selectedArtifact.ProjectName
	volumeLabels["com.docker.compose.volume"] = volume.ComposeName
	return mysqlStepAuthority{selection: mysql84execution.Selection{ImageReference: selected.ImageReference,
		ImageID: "sha256:" + hex.EncodeToString(fact.LocalImageIdSha256), ImageOS: selected.ImageOs,
		Architecture: selected.ImageArchitecture, Variant: selected.ImageVariant, Labels: labels,
		VolumeName: volume.DockerName, VolumeLabels: volumeLabels}, serviceID: serviceID,
		labelsSHA256: labelsSHA, labelCount: fact.RequiredLabelCount, imageRefSHA: referenceSHA[:],
		imageID: "sha256:" + hex.EncodeToString(fact.LocalImageIdSha256), maxPlaintext: maximum,
		database: database, role: role}, nil
}

func mysqlDataVolume(artifact *agentpb.ComposeArtifact, service *agentpb.ComposeService) (
	*agentpb.ComposeVolume, error,
) {
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
	if !exists || len(config.Volumes) != 1 || config.Volumes[0].Type != "volume" ||
		config.Volumes[0].Source == "" || config.Volumes[0].Target != "/var/lib/mysql" || config.Volumes[0].ReadOnly {
		return nil, invalidAgentStaging()
	}
	var volume *agentpb.ComposeVolume
	for _, candidate := range artifact.Volumes {
		if candidate.ComposeName == config.Volumes[0].Source {
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

func newMySQLNonce() (mysql84protocol.Nonce, error) {
	var nonce mysql84protocol.Nonce
	if _, err := rand.Read(nonce[:]); err != nil || nonce == (mysql84protocol.Nonce{}) {
		return nonce, errs.New(errs.KindInternal, "MySQL execution nonce is unavailable")
	}
	return nonce, nil
}

func mysqlContainerCheckpoint(authority mysqlStepAuthority, container mysql84execution.Container) (
	*agentpb.BackupMySQLContainerObserved, error,
) {
	imageID, err := hex.DecodeString(strings.TrimPrefix(authority.imageID, "sha256:"))
	if err != nil || len(imageID) != sha256.Size {
		return nil, invalidAgentStaging()
	}
	value := &agentpb.BackupMySQLContainerObserved{ServiceId: authority.serviceID, ContainerId: container.ID,
		ImageReferenceSha256: append([]byte(nil), authority.imageRefSHA...), ObservedLabelCount: authority.labelCount,
		ObservedLabelsSha256: append([]byte(nil), authority.labelsSHA256...), DatabaseImageIdSha256: imageID}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(value)
	if err != nil {
		return nil, errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	value.ObservationSha256 = digest[:]
	return value, nil
}

func publishMySQLContainer(ctx context.Context, publisher *backupStepCheckpoint,
	resume *agentpb.BackupMySQLContainerObserved, authority mysqlStepAuthority,
	container mysql84execution.Container,
) error {
	observed, err := mysqlContainerCheckpoint(authority, container)
	if err != nil {
		return err
	}
	if resume != nil {
		if !proto.Equal(resume, observed) {
			return invalidAgentStaging()
		}
		return nil
	}
	return publisher.publish(
		ctx,
		&agentpb.BackupCheckpointRequest{
			Checkpoint: &agentpb.BackupCheckpointRequest_MysqlContainerObserved{MysqlContainerObserved: observed},
		},
	)
}
