package remotehost

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	record "github.com/AlanD20/groundplane/internal/common/remotehost"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const enrollmentLifetime = 10 * time.Minute

type Service struct {
	repository Repository
	authority  *Authority
	now        func() time.Time
}

func New(repository Repository, authority *Authority) (*Service, error) {
	if repository == nil || authority == nil {
		return nil, errs.New(errs.KindInternal, "Remote Host enrollment dependencies are unavailable")
	}
	return &Service{repository: repository, authority: authority, now: time.Now}, nil
}

func (service *Service) ClientRoots() *x509.CertPool { return service.authority.ClientRoots() }

func (service *Service) Register(ctx context.Context, name string) (HostView, error) {
	host := record.Host{ID: ids.New(ids.KindHost), Name: name}
	if err := host.Validate(); err != nil {
		return HostView{}, err
	}
	if err := service.repository.CreateHost(ctx, host); err != nil {
		return HostView{}, err
	}
	return hostView(host), nil
}

func (service *Service) Show(ctx context.Context, hostID string) (HostView, error) {
	current, err := service.read(ctx, hostID)
	if err != nil {
		return HostView{}, err
	}
	return hostView(current.Host), nil
}

// EnrollmentToken is intentionally opaque to serializers. Encode it only on
// the machine bootstrap transport, never in operator responses or logs.
type EnrollmentToken struct{ raw [32]byte }

func (token EnrollmentToken) String() string   { return "[redacted]" }
func (token EnrollmentToken) GoString() string { return token.String() }

func (token EnrollmentToken) BootstrapValue() string {
	return base64.RawURLEncoding.EncodeToString(token.raw[:])
}

func ParseEnrollmentToken(value string) (EnrollmentToken, error) {
	var token EnrollmentToken
	encoding := base64.RawURLEncoding.Strict()
	if len(value) != encoding.EncodedLen(len(token.raw)) {
		return EnrollmentToken{}, denied()
	}
	count, err := encoding.Decode(token.raw[:], []byte(value))
	if err != nil || count != len(token.raw) || token.raw == [32]byte{} {
		return EnrollmentToken{}, denied()
	}
	return token, nil
}

// BeginEnrollment authorizes an exact enrollment CSR key before any machine
// connection is admitted. Only the protected bootstrap channel carries the
// returned token.
func (service *Service) BeginEnrollment(
	ctx context.Context,
	hostID string,
	keyDigest [32]byte,
) (HostView, EnrollmentToken, error) {
	if keyDigest == [32]byte{} {
		return HostView{}, EnrollmentToken{}, invalidCSR()
	}
	current, err := service.read(ctx, hostID)
	if err != nil {
		return HostView{}, EnrollmentToken{}, err
	}
	next := current.Host
	now := service.now().UTC()
	if next.Agent != nil || next.Generation == math.MaxUint64 ||
		(next.Pending != nil && now.Before(next.Pending.ExpiresAt)) {
		return HostView{}, EnrollmentToken{}, errs.New(
			errs.KindStateConflict,
			"Remote Host already has an Agent or pending enrollment",
		)
	}
	var token EnrollmentToken
	if _, err := rand.Read(token.raw[:]); err != nil {
		return HostView{}, EnrollmentToken{}, errs.Wrap(errs.KindInternal, err)
	}
	next.Generation++
	next.Pending = &record.PendingEnrollment{
		AgentID: ids.New(ids.KindAgent), TokenDigest: sha256.Sum256(token.raw[:]),
		KeyDigest: keyDigest, ExpiresAt: now.Add(enrollmentLifetime),
	}
	if err := service.save(ctx, current, next); err != nil {
		clear(token.raw[:])
		return HostView{}, EnrollmentToken{}, err
	}
	return hostView(next), token, nil
}

// MachineCertificate is returned only to the protected machine enrollment or
// renewal connection. The caller retains its private key and confirms the
// certificate from a new client-authenticated TLS session before it can work.
type MachineCertificate struct {
	Identity record.Identity
	DER      []byte `json:"-"`
}

func (service *Service) Enroll(
	ctx context.Context,
	hostID string,
	token EnrollmentToken,
	csrDER []byte,
) (MachineCertificate, error) {
	defer clear(token.raw[:])
	current, err := service.read(ctx, hostID)
	if err != nil {
		return MachineCertificate{}, concealHost(err)
	}
	pending := current.Host.Pending
	digest := sha256.Sum256(token.raw[:])
	now := service.now().UTC()
	if pending == nil || !now.Before(pending.ExpiresAt) ||
		subtle.ConstantTimeCompare(pending.TokenDigest[:], digest[:]) != 1 {
		return MachineCertificate{}, denied()
	}
	request, err := parseCSR(csrDER)
	if err != nil {
		return MachineCertificate{}, err
	}
	if sha256.Sum256(request.RawSubjectPublicKeyInfo) != pending.KeyDigest {
		return MachineCertificate{}, denied()
	}
	identity := current.Host.Identity(pending.AgentID)
	der, expires, err := service.authority.issue(identity, request, now)
	if err != nil {
		return MachineCertificate{}, err
	}
	next := current.Host
	prepared := *pending
	prepared.Certificate = &record.CertificateCredential{Digest: sha256.Sum256(der), ExpiresAt: expires}
	next.Pending = &prepared
	if err := service.save(ctx, current, next); err != nil {
		return MachineCertificate{}, err
	}
	return MachineCertificate{Identity: identity, DER: der}, nil
}

// Renew stages a new certificate from the currently enrolled Agent's
// authenticated TLS connection. The active certificate remains usable until
// Confirm proves possession of the replacement key.
func (service *Service) Renew(
	ctx context.Context,
	peer tls.ConnectionState,
	csrDER []byte,
) (MachineCertificate, error) {
	current, identity, err := service.currentPeer(ctx, peer)
	if err != nil {
		return MachineCertificate{}, err
	}
	request, err := parseCSR(csrDER)
	if err != nil {
		return MachineCertificate{}, err
	}
	now := service.now().UTC()
	if !now.Before(current.Host.Agent.Certificate.ExpiresAt) {
		return MachineCertificate{}, denied()
	}
	der, expires, err := service.authority.issue(identity, request, now)
	if err != nil {
		return MachineCertificate{}, err
	}
	next := current.Host
	credential := *next.Agent
	credential.Renewal = &record.CertificateCredential{Digest: sha256.Sum256(der), ExpiresAt: expires}
	next.Agent = &credential
	if err := service.save(ctx, current, next); err != nil {
		return MachineCertificate{}, err
	}
	return MachineCertificate{Identity: identity, DER: der}, nil
}

func (service *Service) read(ctx context.Context, hostID string) (record.VersionedHost, error) {
	if ids.Validate(ids.KindHost, hostID) != nil {
		return record.VersionedHost{}, errs.New(errs.KindValidationFailed, "Remote Host id is invalid")
	}
	current, err := service.repository.ReadHost(ctx, hostID)
	if err != nil {
		return record.VersionedHost{}, err
	}
	if current.Revision <= 0 || current.Host.ID != hostID || current.Host.Validate() != nil {
		return record.VersionedHost{}, errs.New(errs.KindInternal, "Remote Host record is invalid")
	}
	return current, nil
}

func (service *Service) save(ctx context.Context, current record.VersionedHost, next record.Host) error {
	if err := next.Validate(); err != nil {
		return err
	}
	updated, err := service.repository.UpdateHost(ctx, current.Revision, next)
	if err != nil {
		return err
	}
	if !updated {
		return staleHost()
	}
	return nil
}

func staleHost() error {
	return errs.New(errs.KindStateConflict, "Remote Host enrollment changed concurrently")
}
