package backupconfig

import "encoding/json"

type kindProbe struct {
	Kind string `json:"kind"`
	Type string `json:"type"`
}

func parseMetadata(payload []byte, entry *Entry) error {
	var probe kindProbe
	if json.Unmarshal(payload, &probe) != nil {
		return archiveError("Config manifest metadata is invalid JSON")
	}
	switch probe.Type {
	case "env":
		var value struct {
			Key  string `json:"key"`
			Type string `json:"type"`
		}
		if json.Unmarshal(payload, &value) != nil {
			return archiveError("Config manifest environment metadata is invalid")
		}
		entry.Metadata = Metadata{
			Kind:        MetadataEnvironment,
			Environment: EnvironmentMetadata{Key: value.Key},
		}
	case "file":
		var value struct {
			GID  uint32 `json:"gid"`
			Mode uint32 `json:"mode"`
			Path string `json:"path"`
			Type string `json:"type"`
			UID  uint32 `json:"uid"`
		}
		if json.Unmarshal(payload, &value) != nil {
			return archiveError("Config manifest file metadata is invalid")
		}
		entry.Metadata = Metadata{Kind: MetadataFile, File: FileMetadata{
			Path: value.Path, Mode: value.Mode, UID: value.UID, GID: value.GID,
		}}
	default:
		return archiveError("Config manifest metadata type is invalid")
	}
	return nil
}

func parseExposure(payload []byte, entry *Entry) error {
	var probe kindProbe
	if json.Unmarshal(payload, &probe) != nil {
		return archiveError("Config manifest exposure is invalid JSON")
	}
	switch probe.Kind {
	case "all":
		var value struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(payload, &value) != nil {
			return archiveError("Config manifest all-services exposure is invalid")
		}
		entry.Exposure = Exposure{Kind: ExposureAll}
	case "services":
		var value struct {
			Kind       string   `json:"kind"`
			ServiceIDs []string `json:"service_ids"`
		}
		if json.Unmarshal(payload, &value) != nil {
			return archiveError("Config manifest services exposure is invalid")
		}
		entry.Exposure = Exposure{Kind: ExposureServices, ServiceIDs: value.ServiceIDs}
	default:
		return archiveError("Config manifest exposure kind is invalid")
	}
	return nil
}

func parseSource(payload []byte, entry *Entry) error {
	var probe kindProbe
	if json.Unmarshal(payload, &probe) != nil {
		return archiveError("Config manifest source is invalid JSON")
	}
	switch probe.Kind {
	case "literal":
		var value struct {
			Kind string `json:"kind"`
		}
		if json.Unmarshal(payload, &value) != nil {
			return archiveError("Config manifest literal source is invalid")
		}
		entry.Source = Source{Kind: SourceLiteral}
	case "secret_ref":
		var value struct {
			Kind      string `json:"kind"`
			SecretRef string `json:"secret_ref"`
		}
		if json.Unmarshal(payload, &value) != nil {
			return archiveError("Config manifest secret-reference source is invalid")
		}
		entry.Source = Source{
			Kind:            SourceSecretReference,
			SecretReference: SecretReference{AuthoredKey: value.SecretRef},
		}
	case "fact":
		var value struct {
			AttachID      string  `json:"attach_id"`
			Fact          string  `json:"fact"`
			GrantAttachID *string `json:"grant_attach_id"`
			Kind          string  `json:"kind"`
		}
		if json.Unmarshal(payload, &value) != nil {
			return archiveError("Config manifest fact source is invalid")
		}
		fact := FactReference{AttachID: value.AttachID, Fact: value.Fact}
		if value.GrantAttachID != nil {
			fact.GrantAttachID = *value.GrantAttachID
		}
		entry.Source = Source{Kind: SourceFact, Fact: fact}
	default:
		return archiveError("Config manifest source kind is invalid")
	}
	return nil
}
