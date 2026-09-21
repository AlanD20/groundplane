package blueprints

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeidentity"
	"github.com/AlanD20/groundplane/internal/core"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func ValidateEnvironmentDesiredMutationAudit(value EnvironmentDesiredMutationAudit) error {
	kinds := 0
	if value.Volume != nil {
		kinds++
	}
	if value.Service != nil {
		kinds++
	}
	if value.Entry != nil {
		kinds++
	}
	if len(value.Entries) != 0 {
		kinds++
	}
	if value.Zone != nil {
		kinds++
	}
	if value.Route != nil {
		kinds++
	}
	if kinds != 1 {
		return errs.New(errs.KindValidationFailed, "desired mutation audit kind is invalid")
	}
	if value.Service != nil {
		return validateEnvironmentServiceMutationAudit(*value.Service)
	}
	if value.Entry != nil {
		return validateEnvironmentEntryMutationAudit(*value.Entry)
	}
	if len(value.Entries) != 0 {
		if len(value.Entries) > core.MaximumBulkEntryCount {
			return errs.New(errs.KindValidationFailed, "Entry bulk mutation audit is too large")
		}
		seen := make(map[string]struct{}, len(value.Entries))
		for _, entry := range value.Entries {
			if err := validateEnvironmentEntryMutationAudit(entry); err != nil {
				return err
			}
			if _, duplicate := seen[entry.EntryID]; duplicate {
				return errs.New(errs.KindValidationFailed, "Entry bulk mutation audit identity is duplicated")
			}
			seen[entry.EntryID] = struct{}{}
		}
		return nil
	}
	if value.Zone != nil {
		return validateEnvironmentZoneMutationAudit(*value.Zone)
	}
	if value.Route != nil {
		return validateEnvironmentRouteMutationAudit(*value.Route)
	}
	if ids.Validate(ids.KindVolume, value.Volume.VolumeID) != nil ||
		!validEnvironmentVolumeSlug(value.Volume.Slug) || volumeidentity.ValidateKey(value.Volume.Key) != nil {
		return errs.New(errs.KindValidationFailed, "Volume desired mutation audit is invalid")
	}
	switch value.Volume.Action {
	case EnvironmentVolumeMutationAdd:
		if !zeroDigest(value.Volume.PreconditionDigest) {
			return errs.New(errs.KindValidationFailed, "Volume add audit has a remove precondition")
		}
	case EnvironmentVolumeMutationEdit:
		if value.Volume.KeySupplied || !zeroDigest(value.Volume.PreconditionDigest) {
			return errs.New(errs.KindValidationFailed, "Volume edit audit changes immutable input")
		}
	case EnvironmentVolumeMutationRemove:
		if !value.Volume.KeySupplied || zeroDigest(value.Volume.PreconditionDigest) {
			return errs.New(errs.KindValidationFailed, "Volume remove audit lacks its accepted precondition")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Volume desired mutation action is invalid")
	}
	return nil
}

func validateEnvironmentEntryMutationAudit(value EnvironmentEntryMutationAudit) error {
	if ids.Validate(ids.KindTask, value.BaseRevisionID) != nil ||
		ids.Validate(ids.KindEnvEntry, value.EntryID) != nil {
		return errs.New(errs.KindValidationFailed, "Entry desired mutation audit identity is invalid")
	}
	if value.Action == EnvironmentEntryMutationRemove {
		if value.Record != nil {
			return errs.New(errs.KindValidationFailed, "Entry remove audit has a desired record")
		}
		return nil
	}
	if value.Action != EnvironmentEntryMutationCreate && value.Action != EnvironmentEntryMutationEdit ||
		value.Record == nil || value.Record.Entry.ID != value.EntryID ||
		entryrecord.ValidateRecord(*value.Record) != nil {
		return errs.New(errs.KindValidationFailed, "Entry desired mutation audit record is invalid")
	}
	if value.Record.Entry.Source.Kind == core.SourceLiteral && value.Record.Entry.Source.Literal != "" {
		return errs.New(errs.KindValidationFailed, "Entry desired mutation audit contains literal value bytes")
	}
	return nil
}

func validateEnvironmentServiceMutationAudit(value EnvironmentServiceMutationAudit) error {
	if ids.Validate(ids.KindService, value.ServiceID) != nil ||
		(value.BaseRevisionID != "" && ids.Validate(ids.KindTask, value.BaseRevisionID) != nil) ||
		(value.BaseRevisionID == "" && value.Action != EnvironmentServiceMutationCreate) {
		return errs.New(errs.KindValidationFailed, "Service desired mutation audit identity is invalid")
	}
	if value.Action == EnvironmentServiceMutationRemove {
		if value.Request != nil {
			return errs.New(errs.KindValidationFailed, "Service remove audit has a request body")
		}
		return nil
	}
	if value.Action != EnvironmentServiceMutationCreate && value.Action != EnvironmentServiceMutationEdit ||
		value.Request == nil {
		return errs.New(errs.KindValidationFailed, "Service desired mutation audit action is invalid")
	}
	request := value.Request
	name := request.Name
	if value.Action == EnvironmentServiceMutationCreate {
		if ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil || name == "" {
			return errs.New(errs.KindValidationFailed, "Service create audit request is invalid")
		}
	} else {
		if request.EnvironmentID != "" || request.Name != "" {
			return errs.New(errs.KindValidationFailed, "Service edit audit changes immutable input")
		}
		name = "service"
	}
	desired := core.Service{
		ID: value.ServiceID, Name: name, Image: request.Image,
		Zones: append([]string(nil), request.Zones...), Strategy: request.Strategy,
		OnFailure: request.OnFailure, Healthcheck: request.Healthcheck, Resources: request.Resources,
		Expose: append([]string(nil), request.Expose...), Restart: request.Restart, Replicas: request.Replicas,
	}
	if err := desired.Validate(); err != nil {
		return errs.New(errs.KindValidationFailed, "Service desired mutation audit request is invalid")
	}
	return nil
}
func validateEnvironmentZoneMutationAudit(value EnvironmentZoneMutationAudit) error {
	if ids.Validate(ids.KindNetwork, value.ZoneID) != nil ||
		(value.BaseRevisionID != "" && ids.Validate(ids.KindTask, value.BaseRevisionID) != nil) ||
		(value.BaseRevisionID == "" && value.Action != EnvironmentZoneMutationCreate) {
		return errs.New(errs.KindValidationFailed, "Zone desired mutation audit identity is invalid")
	}
	if value.Action == EnvironmentZoneMutationRemove && value.Request == nil {
		return nil
	}
	if value.Action != EnvironmentZoneMutationCreate || value.Request == nil {
		return errs.New(errs.KindValidationFailed, "Zone desired mutation audit action is invalid")
	}
	request := value.Request
	if zonerecord.ValidateRecord(zonerecord.Record{EnvironmentID: request.EnvironmentID, Desired: core.Zone{
		ID: value.ZoneID, Name: request.Name, Subnet: request.Subnet, Internal: request.Internal,
		OwnerKind: core.ZoneOwnerEnvironment, OwnerID: request.EnvironmentID,
	}}) != nil {
		return errs.New(errs.KindValidationFailed, "Zone create audit request is invalid")
	}
	return nil
}
func validateEnvironmentRouteMutationAudit(value EnvironmentRouteMutationAudit) error {
	if ids.Validate(ids.KindRoute, value.RouteID) != nil ||
		(value.BaseRevisionID != "" && ids.Validate(ids.KindTask, value.BaseRevisionID) != nil) ||
		(value.BaseRevisionID == "" && value.Action != EnvironmentRouteMutationCreate) {
		return errs.New(errs.KindValidationFailed, "Route desired mutation audit identity is invalid")
	}
	if value.Action == EnvironmentRouteMutationRemove && value.Request == nil {
		return nil
	}
	if value.Request == nil || value.Action == EnvironmentRouteMutationRemove {
		return errs.New(errs.KindValidationFailed, "Route desired mutation audit action is invalid")
	}
	request := value.Request
	if value.Action == EnvironmentRouteMutationCreate {
		desired := core.Route{
			ID: value.RouteID, Host: request.Host, Path: request.Path, Exposure: request.Exposure,
			TargetServiceID: request.TargetServiceID, TargetPort: request.TargetPort,
		}
		if ids.Validate(ids.KindEnvironment, request.EnvironmentID) != nil ||
			ids.Validate(ids.KindService, request.TargetServiceID) != nil || desired.Validate() != nil {
			return errs.New(errs.KindValidationFailed, "Route create audit request is invalid")
		}
		return nil
	}
	if value.Action != EnvironmentRouteMutationEdit || request.EnvironmentID != "" || request.Host != "" ||
		request.Path != "" || request.TargetServiceID != "" || request.TargetPort != 0 ||
		(request.Exposure != "public" && request.Exposure != "internal") {
		return errs.New(errs.KindValidationFailed, "Route edit audit changes immutable input")
	}
	return nil
}

func validEnvironmentVolumeSlug(value string) bool {
	if len(value) < 1 || len(value) > 63 || value[0] == '-' || value[len(value)-1] == '-' {
		return false
	}
	for _, character := range []byte(value) {
		if (character < 'a' || character > 'z') && (character < '0' || character > '9') && character != '-' {
			return false
		}
	}
	return true
}
