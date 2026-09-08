package app

import (
	"context"
	"encoding/json"
	"slices"
	"strings"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	controllerpkg "github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type AttachGrantFactParams struct {
	AttachID string
	Params   adapters.FactParams
}

type AttachFactRepository interface {
	ResolveAttach(context.Context, string, string) (etcd.Versioned[etcd.AttachRecord], error)
	GetAttachFacts(
		context.Context,
		etcd.Versioned[etcd.AttachRecord],
	) (etcd.AttachEncryptedFacts, bool, error)
}

type AttachFactService struct {
	repository AttachFactRepository
	protector  *secretvalue.Protector
}

func NewAttachFactService(
	repository AttachFactRepository,
	protector *secretvalue.Protector,
) (*AttachFactService, error) {
	if repository == nil || protector == nil {
		return nil, errs.New(errs.KindValidationFailed, "Attach fact repository and protector are required")
	}
	return &AttachFactService{repository: repository, protector: protector}, nil
}

// SealFactSets renders and seals the complete immutable fact bundle before an
// Attach is published. The caller retains ownership of FactParams.Password.
func (service *AttachFactService) SealFactSets(
	ctx context.Context,
	attachID string,
	adapter adapters.Adapter,
	own adapters.FactParams,
	grants []AttachGrantFactParams,
) ([]etcd.AttachFactSetMetadata, *etcd.AttachEncryptedFacts, error) {
	if ctx == nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Attach fact context is required")
	}
	if err := ids.Validate(ids.KindAttach, attachID); err != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Attach fact owner id is invalid")
	}
	if adapter == nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "Attach adapter is required")
	}
	if adapter.Manual() {
		if len(grants) != 0 {
			return nil, nil, errs.New(errs.KindAdapterManualOnly, "Manual Attach cannot publish grant facts")
		}
		return nil, nil, nil
	}
	authentication, err := core.ResolveBackingAuthentication(
		adapter.SupportsAuthenticationModes(), own.Authentication,
	)
	if err != nil || authentication != own.Authentication {
		return nil, nil, errs.New(errs.KindValidationFailed, "Attach fact authentication mode is invalid")
	}
	canonicalGrants := append([]AttachGrantFactParams(nil), grants...)
	slices.SortFunc(canonicalGrants, func(left AttachGrantFactParams, right AttachGrantFactParams) int {
		return strings.Compare(left.AttachID, right.AttachID)
	})
	priorGrantID := ""
	for _, grant := range canonicalGrants {
		if ids.Validate(ids.KindAttach, grant.AttachID) != nil || grant.AttachID == attachID ||
			grant.AttachID == priorGrantID || grant.Params.Authentication != authentication {
			return nil, nil, errs.New(errs.KindValidationFailed, "Attach grant fact ids must be valid and unique")
		}
		priorGrantID = grant.AttachID
	}

	sets := make([]attachFactValueSet, 0, len(canonicalGrants)+1)
	metadata := make([]etcd.AttachFactSetMetadata, 0, len(canonicalGrants)+1)
	ownedFacts := make([][]adapters.Fact, 0, len(canonicalGrants)+1)
	defer func() {
		for _, facts := range ownedFacts {
			adapters.ClearFacts(facts)
		}
	}()
	appendSet := func(grantAttachID string, params adapters.FactParams) error {
		facts, err := adapters.BuildFacts(adapter, params)
		if err != nil {
			return errs.Wrap(errs.KindValidationFailed, err)
		}
		ownedFacts = append(ownedFacts, facts)
		slices.SortFunc(facts, func(left adapters.Fact, right adapters.Fact) int {
			return strings.Compare(left.Key, right.Key)
		})
		factMetadata := make([]etcd.AttachFactDefinition, 0, len(facts))
		values := make([]attachFactValue, 0, len(facts))
		for _, fact := range facts {
			factMetadata = append(factMetadata, etcd.AttachFactDefinition{Key: fact.Key, Secret: fact.Secret})
			values = append(values, attachFactValue{Key: fact.Key, Value: fact.Value})
		}
		metadata = append(metadata, etcd.AttachFactSetMetadata{
			GrantAttachID: grantAttachID,
			Facts:         factMetadata,
		})
		sets = append(sets, attachFactValueSet{GrantAttachID: grantAttachID, Facts: values})
		return nil
	}
	if err := appendSet("", own); err != nil {
		return nil, nil, err
	}
	for _, grant := range canonicalGrants {
		if err := appendSet(grant.AttachID, grant.Params); err != nil {
			return nil, nil, err
		}
	}
	bundle := attachFactBundle{
		Version: 3, AttachID: attachID, Sets: sets,
		Identity: attachTaskIdentity{
			Authentication: authentication,
			Database:       own.Database, Role: own.Role, Password: append([]byte(nil), own.Password...),
			Grants: make([]attachTaskGrantIdentity, 0, len(canonicalGrants)),
		},
	}
	for _, grant := range canonicalGrants {
		bundle.Identity.Grants = append(bundle.Identity.Grants, attachTaskGrantIdentity{
			AttachID: grant.AttachID, Database: grant.Params.Database,
		})
	}
	defer bundle.clear()
	payload, err := json.Marshal(bundle)
	if err != nil {
		return nil, nil, errs.Wrap(errs.KindInternal, err)
	}
	defer clearAttachBytes(payload)
	envelope, err := service.protector.Seal(ctx, payload)
	if err != nil {
		return nil, nil, err
	}
	ciphertext := envelope.Ciphertext()
	defer clearAttachBytes(ciphertext)
	envelopeMetadata := envelope.Metadata()
	facts, err := etcd.NewAttachEncryptedFacts(
		attachID,
		uint8(envelopeMetadata.Version),
		string(envelopeMetadata.Cipher),
		string(envelopeMetadata.Digest.Algorithm),
		ciphertext,
	)
	if err != nil {
		return nil, nil, err
	}
	return metadata, &facts, nil
}

// ResolveTaskIdentity exposes only one task-owned encrypted identity during
// synchronous plan construction. Public fact readiness rules remain unchanged.
func (service *AttachFactService) ResolveTaskIdentity(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	taskID string,
	consume controllerpkg.AttachPlanIdentityConsumer,
) error {
	if ctx == nil || consume == nil || ids.Validate(ids.KindTask, taskID) != nil ||
		current.Record.TaskID != taskID || !current.Record.OwnsCredential() {
		return errs.New(errs.KindValidationFailed, "Attach task identity request is invalid")
	}
	allowed := current.Record.Operation == etcd.AttachOperationProvision &&
		(current.Record.Status == core.AttachPending || current.Record.Status == core.AttachProvisioning)
	allowed = allowed || current.Record.Operation == etcd.AttachOperationDetach &&
		current.Record.Status == core.AttachDetaching
	if !allowed {
		return errs.New(errs.KindStateConflict, "Attach task identity is unavailable in the current lifecycle")
	}
	return service.openBundle(ctx, current, func(bundle *attachFactBundle) error {
		identity := controllerpkg.AttachPlanIdentity{
			Authentication: bundle.Identity.Authentication,
			Database:       bundle.Identity.Database,
			Role:           bundle.Identity.Role,
			Password:       append([]byte(nil), bundle.Identity.Password...),
			Grants:         make([]controllerpkg.AttachPlanGrantIdentity, 0, len(bundle.Identity.Grants)),
		}
		for _, grant := range bundle.Identity.Grants {
			identity.Grants = append(identity.Grants, controllerpkg.AttachPlanGrantIdentity{
				AttachID: grant.AttachID, Database: grant.Database,
			})
		}
		defer identity.Clear()
		return consume(identity)
	})
}

// ResolveReadyDatabase exposes only the stable database identity needed to
// construct another ready Attach's grant procedure and fact set.
func (service *AttachFactService) ResolveReadyDatabase(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	consume func(string) error,
) error {
	if ctx == nil || consume == nil {
		return errs.New(errs.KindValidationFailed, "Attach database identity context and consumer are required")
	}
	if current.Record.Status != core.AttachReady || !current.Record.OwnsCredential() {
		return errs.New(errs.KindStateConflict, "Attach database identity is available only while ready")
	}
	return service.openBundle(ctx, current, func(bundle *attachFactBundle) error {
		if bundle.Identity.Database == "" {
			return errs.New(errs.KindInternal, "Ready Attach database identity is missing")
		}
		return consume(bundle.Identity.Database)
	})
}

// ResolveBackupIdentity opens only repository-supplied fixed-revision facts
// and exposes the exact ready Attach database/role pair during consume.
func (service *AttachFactService) ResolveBackupIdentity(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	stored etcd.AttachEncryptedFacts,
	consume func(etcd.BackupPostgresIdentity) error,
) error {
	if ctx == nil || consume == nil || current.Record.Status != core.AttachReady ||
		!current.Record.OwnsCredential() ||
		stored.AttachID != current.Record.ID {
		return errs.New(errs.KindStateConflict, "backup Attach identity is unavailable")
	}
	return service.openStoredBundle(ctx, current.Record, stored, func(bundle *attachFactBundle) error {
		if bundle.Identity.Database == "" || bundle.Identity.Role == "" {
			return errs.New(errs.KindInternal, "backup Attach identity is incomplete")
		}
		return consume(etcd.BackupPostgresIdentity{
			Database: bundle.Identity.Database,
			Role:     bundle.Identity.Role,
		})
	})
}

// ResolveRemovalDatabase exposes only the stable database identity required by
// a destructive impact preview. Failed provisioning may have applied the
// database side effect, so its sealed identity remains part of the cascade.
func (service *AttachFactService) ResolveRemovalDatabase(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	consume func(string) error,
) error {
	if ctx == nil || consume == nil {
		return errs.New(errs.KindValidationFailed, "Attach removal database context and consumer are required")
	}
	if !current.Record.OwnsCredential() || len(current.Record.FactSets) == 0 {
		return consume("")
	}
	if current.Record.Status != core.AttachReady && current.Record.Status != core.AttachFailed {
		return errs.New(errs.KindStateConflict, "Attach must be terminal before backing Zone removal")
	}
	return service.openBundle(ctx, current, func(bundle *attachFactBundle) error {
		return consume(bundle.Identity.Database)
	})
}

// ResolveFact resolves mutable labels on every call and exposes one verified
// value only for the duration of consume.
func (service *AttachFactService) ResolveFact(
	ctx context.Context,
	environmentID string,
	reference core.FactRef,
	destinationSecret bool,
	consume secretvalue.PlaintextConsumer,
) error {
	if ctx == nil || consume == nil {
		return errs.New(errs.KindValidationFailed, "Attach fact context and consumer are required")
	}
	if reference.Attach == "" || reference.Key == "" {
		return errs.New(errs.KindValidationFailed, "Attach and fact key are required")
	}
	current, err := service.repository.ResolveAttach(ctx, environmentID, reference.Attach)
	if err != nil {
		return err
	}
	if current.Record.Status != core.AttachReady {
		return errs.New(errs.KindStateConflict, "Attach facts are available only while the Attach is ready")
	}
	if !current.Record.OwnsCredential() {
		owner, resolveErr := service.repository.ResolveAttach(
			ctx, environmentID, current.Record.CredentialAttachID,
		)
		if resolveErr != nil {
			return resolveErr
		}
		if owner.Record.Status != core.AttachReady || !owner.Record.OwnsCredential() ||
			owner.Record.BackingServiceID != current.Record.BackingServiceID {
			return errs.New(errs.KindStateConflict, "Attach credential owner is not ready")
		}
		current = owner
	}
	grantAttachID := ""
	if reference.Grant != "" {
		grant, resolveErr := service.repository.ResolveAttach(ctx, environmentID, reference.Grant)
		if resolveErr != nil {
			return resolveErr
		}
		if grant.Record.Status != core.AttachReady ||
			grant.Record.BackingServiceID != current.Record.BackingServiceID ||
			!slices.Contains(current.Record.GrantAttachIDs, grant.Record.ID) {
			return errs.New(errs.KindScopeUnauthorized, "Fact grant is not a ready grant of the owning Attach")
		}
		grantAttachID = grant.Record.ID
	}
	definition, declared := attachFactMetadataDefinition(current.Record.FactSets, grantAttachID, reference.Key)
	if !declared {
		return errs.New(errs.KindValidationFailed, "Attach fact key is not declared by the selected fact set")
	}
	if definition.Secret && !destinationSecret {
		return errs.New(errs.KindValidationFailed, "Secret Attach fact requires a secret Entry destination")
	}
	return service.openBundle(ctx, current, func(bundle *attachFactBundle) error {
		for _, set := range bundle.Sets {
			if set.GrantAttachID != grantAttachID {
				continue
			}
			for _, fact := range set.Facts {
				if fact.Key == reference.Key {
					return consume(fact.Value)
				}
			}
		}
		return errs.New(errs.KindInternal, "Attach fact value is missing")
	})
}

func (service *AttachFactService) openBundle(
	ctx context.Context,
	current etcd.Versioned[etcd.AttachRecord],
	consume func(*attachFactBundle) error,
) error {
	stored, ok, err := service.repository.GetAttachFacts(ctx, current)
	if err != nil {
		return err
	}
	if !ok {
		return errs.New(errs.KindInternal, "Ready Attach has no encrypted facts")
	}
	defer clearAttachBytes(stored.Ciphertext)
	return service.openStoredBundle(ctx, current.Record, stored, consume)
}

func (service *AttachFactService) openStoredBundle(
	ctx context.Context,
	record etcd.AttachRecord,
	stored etcd.AttachEncryptedFacts,
	consume func(*attachFactBundle) error,
) error {
	envelope, err := secretvalue.Restore(secretvalue.Metadata{
		Version: secretvalue.EnvelopeVersion(stored.EnvelopeVersion),
		Cipher:  secretvalue.CipherSuite(stored.Cipher),
		Digest: secretvalue.Digest{
			Algorithm: secretvalue.DigestAlgorithm(stored.DigestAlgorithm),
			Value:     stored.CiphertextSHA256,
		},
	}, stored.Ciphertext)
	if err != nil {
		return err
	}
	return service.protector.Open(ctx, envelope, func(plaintext []byte) error {
		var bundle attachFactBundle
		if decodeErr := json.Unmarshal(plaintext, &bundle); decodeErr != nil {
			return errs.New(errs.KindInternal, "Attach fact plaintext is corrupt")
		}
		defer bundle.clear()
		if !validAttachFactBundle(bundle, record) {
			return errs.New(errs.KindInternal, "Attach fact plaintext does not match durable metadata")
		}
		return consume(&bundle)
	})
}

type attachFactBundle struct {
	Version  uint8                `json:"version"`
	AttachID string               `json:"attach_id"`
	Identity attachTaskIdentity   `json:"identity"`
	Sets     []attachFactValueSet `json:"sets"`
}

type attachTaskIdentity struct {
	Authentication core.BackingAuthentication `json:"authentication,omitempty"`
	Database       string                     `json:"database,omitempty"`
	Role           string                     `json:"role"`
	Password       []byte                     `json:"password"`
	Grants         []attachTaskGrantIdentity  `json:"grants,omitempty"`
}

type attachTaskGrantIdentity struct {
	AttachID string `json:"attach_id"`
	Database string `json:"database"`
}

type attachFactValueSet struct {
	GrantAttachID string            `json:"grant_attach_id,omitempty"`
	Facts         []attachFactValue `json:"facts"`
}

type attachFactValue struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

func attachFactMetadataDefinition(
	sets []etcd.AttachFactSetMetadata,
	grantAttachID string,
	key string,
) (etcd.AttachFactDefinition, bool) {
	for _, set := range sets {
		if set.GrantAttachID != grantAttachID {
			continue
		}
		for _, fact := range set.Facts {
			if fact.Key == key {
				return fact, true
			}
		}
		return etcd.AttachFactDefinition{}, false
	}
	return etcd.AttachFactDefinition{}, false
}

func validAttachFactBundle(bundle attachFactBundle, record etcd.AttachRecord) bool {
	if bundle.Version != 3 || bundle.AttachID != record.ID ||
		!validAttachTaskIdentityAuthentication(bundle.Identity) ||
		len(bundle.Identity.Grants) != len(record.GrantAttachIDs) ||
		len(bundle.Sets) != len(record.FactSets) {
		return false
	}
	for index, grant := range bundle.Identity.Grants {
		if grant.AttachID != record.GrantAttachIDs[index] || grant.Database == "" {
			return false
		}
	}
	for setIndex, set := range bundle.Sets {
		metadata := record.FactSets[setIndex]
		if set.GrantAttachID != metadata.GrantAttachID || len(set.Facts) != len(metadata.Facts) {
			return false
		}
		for factIndex, fact := range set.Facts {
			if fact.Key != metadata.Facts[factIndex].Key || len(fact.Value) == 0 {
				return false
			}
		}
	}
	return true
}

func validAttachTaskIdentityAuthentication(identity attachTaskIdentity) bool {
	switch identity.Authentication {
	case "", core.BackingAuthenticationUsernamePassword:
		return identity.Role != "" && len(identity.Password) != 0
	case core.BackingAuthenticationPassword:
		return identity.Role == "default" && len(identity.Password) != 0
	case core.BackingAuthenticationNone:
		return identity.Role == "" && len(identity.Password) == 0 && len(identity.Grants) == 0
	default:
		return false
	}
}

func (bundle *attachFactBundle) clear() {
	clearAttachBytes(bundle.Identity.Password)
	bundle.Identity.Password = nil
	bundle.Identity.Grants = nil
	for setIndex := range bundle.Sets {
		for factIndex := range bundle.Sets[setIndex].Facts {
			clearAttachBytes(bundle.Sets[setIndex].Facts[factIndex].Value)
			bundle.Sets[setIndex].Facts[factIndex].Value = nil
		}
		bundle.Sets[setIndex].Facts = nil
	}
	bundle.Sets = nil
}

func clearAttachBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
