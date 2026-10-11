package desiredauthoring

import (
	"context"
	"slices"
	"strings"

	model "github.com/AlanD20/groundplane/internal/common/dnsrecords"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type DNSAction uint8

const (
	DNSCreate DNSAction = iota + 1
	DNSEdit
	DNSRemove
)

// DNSMutation replaces one declaration, not an Environment Apply. A nil Scope
// preserves an edited record's captured namespace through hierarchy renames.
// The caller allocates a new RecordID only for Create and includes its response
// in the replay marker committed with the returned publication.
type DNSMutation struct {
	Action               DNSAction
	RecordID             string
	ExpectedHeadRevision int64
	Label                string
	Scope                *model.NamespaceScope
	Address              string
	Service              string
	Zone                 string
}

// PrepareDNS stages one native desired revision while retaining its current
// render generation and all unrelated intention, identities and runtime bytes.
// It does not change the head, execute CoreDNS or release withdrawn names. The
// caller commits these comparisons/mutations together with its replay marker;
// successful resolver acknowledgement remains the authority for name release.
func PrepareDNS(ctx context.Context, store Store, environmentID string, marker idempotency.IdempotencyMarker,
	request DNSMutation,
) (result Publication, resultErr error) {
	if marker.Kind != idempotency.IdempotencyMarkerDirect || marker.State != idempotency.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment || marker.Locator.ScopeID != environmentID {
		return Publication{}, errs.New(
			errs.KindValidationFailed,
			"DNS mutation requires a completed Environment-scoped replay marker",
		)
	}
	if err := idempotency.ValidateIdempotencyMarker(marker); err != nil {
		return Publication{}, err
	}
	if ids.Validate(ids.KindDNSRecord, request.RecordID) != nil || request.ExpectedHeadRevision < 0 ||
		request.Action < DNSCreate || request.Action > DNSRemove {
		return Publication{}, errs.New(errs.KindValidationFailed, "DNS mutation identity or action is invalid")
	}
	snapshot, err := loadSnapshot(ctx, store, environmentID)
	if err != nil {
		return Publication{}, err
	}
	if snapshot.headRevision != request.ExpectedHeadRevision {
		return Publication{}, errs.New(
			errs.KindStateConflict,
			"Environment Blueprint changed; refresh DNS records before saving",
		)
	}
	fence, err := environmentfence.LoadOrdinary(ctx, store, environmentID, snapshot.readRevision)
	if err != nil {
		return Publication{}, err
	}
	namespace, err := snapshot.dnsNamespace(ctx, store, request.Scope)
	if err != nil {
		return Publication{}, err
	}
	if err := snapshot.mutateDNS(request, namespace); err != nil {
		return Publication{}, err
	}
	names, nameWrites, err := dnsrecords.NewReader(store).PrepareDirectDNSPublication(
		ctx, snapshot.input, snapshot.owned, request.RecordID, snapshot.readRevision,
	)
	if err != nil {
		return Publication{}, err
	}
	defer func() {
		if resultErr != nil {
			keyvalue.ClearMutationValues(nameWrites)
			keyvalue.ClearMutationValues(result.Mutations)
		}
	}()
	result, err = snapshot.stage(ctx, store, marker)
	if err != nil {
		return result, err
	}
	result.Conditions, err = environmentfence.AppendConditions(result.Conditions, fence)
	if err != nil {
		return result, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return result, err
	}
	result.Conditions = append(result.Conditions, names...)
	result.Mutations = append(result.Mutations, nameWrites...)
	result.Mutations = append(result.Mutations, epoch)
	if request.Action == DNSCreate {
		birth, err := blueprintunits.PrepareInitialAbsencePublication(environmentID, snapshot.input.RevisionID,
			[]blueprintunits.ResourceKey{{Kind: ids.KindDNSRecord, ID: request.RecordID}})
		if err != nil {
			return result, err
		}
		result.Conditions = append(result.Conditions, birth.Conditions()...)
		result.Mutations = append(result.Mutations, birth.Mutations()...)
	}
	return result, nil
}

func (snapshot *metadataSnapshot) mutateDNS(request DNSMutation, namespace model.Namespace) error {
	previous := environmentprojection.CloneEnvironmentOwnedIdentities(snapshot.owned)
	previous.RevisionID = snapshot.baseRevisionID
	index := slices.IndexFunc(snapshot.owned.DNSRecords, func(record environmentprojection.OwnedIdentity) bool {
		return record.ID == request.RecordID
	})
	if request.Action == DNSCreate && index >= 0 {
		return errs.New(errs.KindStateConflict, "DNS record already exists")
	}
	if request.Action != DNSCreate && index < 0 {
		return errs.New(
			errs.KindStateConflict,
			"DNS record no longer exists in this Environment; refresh before editing",
		)
	}
	if request.Action == DNSRemove {
		if request.Label != "" || request.Scope != nil || request.Address != "" || request.Service != "" ||
			request.Zone != "" {
			return errs.New(errs.KindValidationFailed, "DNS removal cannot include a replacement target or namespace")
		}
		delete(snapshot.input.Input.DNS, snapshot.owned.DNSRecords[index].Name)
		snapshot.owned.DNSRecords = slices.Delete(snapshot.owned.DNSRecords, index, index+1)
	} else {
		if existing := slices.IndexFunc(snapshot.owned.DNSRecords, func(record environmentprojection.OwnedIdentity) bool {
			return record.Name == request.Label && record.ID != request.RecordID
		}); existing >= 0 {
			return errs.New(errs.KindNameConflict, "DNS record label is already used in this Environment")
		}
		if request.Action == DNSEdit {
			identity := &snapshot.owned.DNSRecords[index]
			prior, found := snapshot.input.Input.DNS[identity.Name]
			if !found {
				return errs.New(errs.KindInternal, "DNS identity has no authored declaration")
			}
			if request.Scope == nil {
				namespace = prior.Namespace
			}
			delete(snapshot.input.Input.DNS, identity.Name)
			identity.Name = request.Label
		} else {
			for _, omission := range snapshot.owned.PendingOmissions {
				if omission.Kind == ids.KindDNSRecord && omission.ID == request.RecordID {
					return errs.New(errs.KindStateConflict, "DNS identity is awaiting removal; Create requires a new identity")
				}
			}
			snapshot.owned.DNSRecords = append(snapshot.owned.DNSRecords, environmentprojection.OwnedIdentity{
				ID: request.RecordID, Name: request.Label, BirthRevisionID: snapshot.input.RevisionID,
			})
		}
		if snapshot.input.Input.DNS == nil {
			snapshot.input.Input.DNS = make(map[string]model.EnvironmentSpec)
		}
		snapshot.input.Input.DNS[request.Label] = model.EnvironmentSpec{
			Namespace: namespace, Address: request.Address, Service: request.Service, Zone: request.Zone,
		}
	}
	slices.SortFunc(snapshot.owned.DNSRecords, func(left, right environmentprojection.OwnedIdentity) int {
		return strings.Compare(left.Name, right.Name)
	})
	if snapshot.baseRevisionID != "" {
		snapshot.owned.PendingOmissions = environmentprojection.RetainOwnedOmissions(snapshot.owned, previous)
	}
	if err := core.ValidateBlueprintDNSNames(snapshot.input.Input.DNS, snapshot.input.Input.Routes); err != nil {
		return err
	}
	_, err := dnsrecords.AuthoredEnvironmentRecords(snapshot.input, snapshot.owned)
	return err
}
