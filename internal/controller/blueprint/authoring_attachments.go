package blueprint

import (
	"context"
	"slices"
	"sort"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Direct Attach actions do not rewrite the last uploaded Blueprint. Reconstruct
// their authored topology from durable identities so export and review see the
// same bindings that Apply must preserve.
func (service *Service) authoringAttachmentSpecs(
	ctx context.Context,
	snapshot environmentBlueprintSnapshot,
	attaches []etcdstore.Versioned[attachrecord.Record],
) (map[string]core.AttachmentSpec, error) {
	environmentID := snapshot.environment.Record.ID
	consumerNames := make(map[string]string, len(snapshot.identities.Record.Services))
	for _, identity := range snapshot.identities.Record.Services {
		if identity.Name == "" || consumerNames[identity.ID] != "" {
			return nil, errs.New(errs.KindStateConflict, "Blueprint consumer Service identity is inconsistent")
		}
		consumerNames[identity.ID] = identity.Name
	}
	byID := make(map[string]attachrecord.Record, len(attaches))
	byName := make(map[string]struct{}, len(attaches))
	for _, current := range attaches {
		record := current.Record
		if record.EnvironmentID != environmentID || record.ID == "" || record.Name == "" {
			return nil, errs.New(errs.KindStateConflict, "Environment Attach ownership is inconsistent")
		}
		if _, exists := byID[record.ID]; exists {
			return nil, errs.New(errs.KindStateConflict, "Environment Attach identity is duplicated")
		}
		if _, exists := byName[record.Name]; exists {
			return nil, errs.New(errs.KindStateConflict, "Environment Attach name is duplicated")
		}
		byID[record.ID] = record
		byName[record.Name] = struct{}{}
	}

	services := make(map[string]servicerecord.ServiceRecord)
	readService := func(id string) (servicerecord.ServiceRecord, error) {
		if record, exists := services[id]; exists {
			return record, nil
		}
		versioned, err := service.repository.GetService(ctx, id)
		if err != nil {
			return servicerecord.ServiceRecord{}, err
		}
		record := versioned.Record
		if record.Desired.ID != id {
			return servicerecord.ServiceRecord{}, errs.New(
				errs.KindStateConflict,
				"Attach Service identity is inconsistent",
			)
		}
		services[id] = record
		return record, nil
	}
	projects := make(map[string]hierarchyrecord.ProjectRecord)
	readProject := func(id string) (hierarchyrecord.ProjectRecord, error) {
		if record, exists := projects[id]; exists {
			return record, nil
		}
		versioned, err := service.repository.GetProject(ctx, id)
		if err != nil {
			return hierarchyrecord.ProjectRecord{}, err
		}
		record := versioned.Record
		if record.ID != id || record.Kind != hierarchyrecord.ProjectKindBacking {
			return hierarchyrecord.ProjectRecord{}, errs.New(
				errs.KindStateConflict,
				"Attach backing Project is inconsistent",
			)
		}
		projects[id] = record
		return record, nil
	}

	specs := make(map[string]core.AttachmentSpec, len(attaches))
	for _, current := range attaches {
		record := current.Record
		// Unfinished removal retains execution authority, not desired topology.
		if slices.ContainsFunc(
			snapshot.identities.Record.PendingOmissions,
			func(omission projectionrecord.OwnedOmission) bool {
				return omission.Kind == ids.KindAttach && omission.ID == record.ID
			},
		) {
			continue
		}
		// Export describes authored topology, not readiness of its runtime.
		// A stopped Apply may never have sealed the new effective projection.
		consumerName, found := consumerNames[record.ServiceID]
		if !found {
			return nil, errs.New(errs.KindStateConflict, "Attach consumer is absent from the Environment Blueprint")
		}
		backing, err := readService(record.BackingServiceID)
		if err != nil {
			return nil, err
		}
		project, err := readProject(record.BackingProjectID)
		if err != nil {
			return nil, err
		}
		backingEnvironment, err := service.repository.GetEnvironment(ctx, record.BackingEnvironmentID)
		if err != nil {
			return nil, err
		}
		if backingEnvironment.Record.ID != record.BackingEnvironmentID ||
			backingEnvironment.Record.ProjectID != project.ID ||
			backing.EnvironmentID != backingEnvironment.Record.ID ||
			backing.BackingNetworkID != record.BackingNetworkID {
			return nil, errs.New(errs.KindStateConflict, "Attach topology is inconsistent")
		}
		spec := core.AttachmentSpec{
			BackingProject: project.Slug,
			BackingService: backing.Desired.Name,
			Service:        consumerName,
		}
		if record.CredentialAttachID == record.ID {
			spec.Credential.Mode = "new"
		} else {
			owner, exists := byID[record.CredentialAttachID]
			if !exists || !owner.OwnsCredential() || owner.BackingServiceID != record.BackingServiceID {
				return nil, errs.New(errs.KindStateConflict, "Attach credential owner is inconsistent")
			}
			spec.Credential = core.AttachmentCredentialSpec{Mode: "existing", Attach: owner.Name}
		}
		for _, grantID := range record.GrantAttachIDs {
			grant, exists := byID[grantID]
			if !exists || !grant.OwnsCredential() || grant.BackingServiceID != record.BackingServiceID {
				return nil, errs.New(errs.KindStateConflict, "Attach grant is inconsistent")
			}
			spec.Grants = append(spec.Grants, grant.Name)
		}
		sort.Strings(spec.Grants)
		specs[record.Name] = spec
	}
	return specs, nil
}

func validateCurrentAttachmentDeclarations(
	candidate map[string]core.AttachmentSpec,
	current map[string]core.AttachmentSpec,
	attaches []etcdstore.Versioned[attachrecord.Record],
) error {
	for _, versioned := range attaches {
		record := versioned.Record
		spec, retained := candidate[record.Name]
		if !retained {
			continue
		}
		if record.Status != core.AttachReady || record.Operation != attachrecord.AttachOperationProvision ||
			!sameAttachmentSpec(current[record.Name], spec) {
			return errs.New(errs.KindStateConflict, "Existing Blueprint Attach does not match the authored topology")
		}
	}
	return nil
}

func sameAttachmentSpec(left, right core.AttachmentSpec) bool {
	if left.BackingProject != right.BackingProject || left.BackingService != right.BackingService ||
		left.Service != right.Service || left.Credential != right.Credential ||
		len(left.Grants) != len(right.Grants) {
		return false
	}
	leftGrants := append([]string(nil), left.Grants...)
	rightGrants := append([]string(nil), right.Grants...)
	sort.Strings(leftGrants)
	sort.Strings(rightGrants)
	return slices.Equal(leftGrants, rightGrants)
}
