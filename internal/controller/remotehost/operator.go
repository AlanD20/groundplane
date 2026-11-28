package remotehost

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/pem"
	"math"
	"net/http"
	"strconv"

	"github.com/AlanD20/groundplane/internal/common/ids"
	record "github.com/AlanD20/groundplane/internal/common/remotehost"
	intent "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const OperatorRoute = "/remote-hosts"
const EnrollmentRoute = OperatorRoute + "/{id}/enrollment"

type OperatorRepository interface {
	ListHosts(context.Context, keyvalue.PageRequest) (keyvalue.Page[record.Host], error)
	PublishOperatorChange(context.Context, record.VersionedHost, record.Host, workqueue.Change) (bool, error)
}

// Operator manages public CSR approval through the trusted human boundary.
// The Agent must confirm possession on its separate authenticated transport.
type Operator struct {
	service    *Service
	repository OperatorRepository
	intents    *intent.Coordinator
	replays    *etcd.IdempotencyRepository
}

func NewOperator(service *Service, repository OperatorRepository, intents *intent.Coordinator,
	replays *etcd.IdempotencyRepository,
) (*Operator, error) {
	if service == nil || repository == nil || intents == nil || replays == nil {
		return nil, errs.New(errs.KindInternal, "Remote Host operator dependencies are unavailable")
	}
	return &Operator{service: service, repository: repository, intents: intents, replays: replays}, nil
}

func (operator *Operator) ListRemoteHosts(
	ctx context.Context,
	request keyvalue.PageRequest,
) (api.Page[api.RemoteHost], error) {
	page, err := operator.repository.ListHosts(ctx, request)
	if err != nil {
		return api.Page[api.RemoteHost]{}, err
	}
	result := api.Page[api.RemoteHost]{
		Items: make([]api.RemoteHost, 0, len(page.Items)), Revision: page.Revision, NextCursor: page.NextCursor,
	}
	for _, host := range page.Items {
		result.Items = append(result.Items, publicHost(hostView(host.Record)))
	}
	return result, nil
}

func (operator *Operator) ShowRemoteHost(ctx context.Context, hostID string) (api.RemoteHost, error) {
	host, err := operator.service.Show(ctx, hostID)
	return publicHost(host), err
}

func (operator *Operator) CreateRemoteHost(
	ctx context.Context,
	name, key string,
) (idempotency.IdempotencyResponse, error) {
	return operator.mutate(ctx, humanMutation{
		method: http.MethodPost, route: OperatorRoute, key: key, status: http.StatusCreated,
		fields: []intent.Field{{Name: "name", Value: intent.String(name)}},
	}, func() (hostChange, error) {
		host := record.Host{ID: ids.New(ids.KindHost), Name: name}
		if err := host.Validate(); err != nil {
			return hostChange{}, err
		}
		return withHostResponse(hostChange{next: host}, publicHost(hostView(host)))
	})
}

func (operator *Operator) StageRemoteHostEnrollment(ctx context.Context, hostID string,
	input api.RemoteHostEnrollmentRequest, key string,
) (idempotency.IdempotencyResponse, error) {
	csrPEM := bytes.TrimSpace([]byte(input.CSR))
	block, remaining := pem.Decode(csrPEM)
	if block == nil || !bytes.HasPrefix(csrPEM, []byte("-----BEGIN CERTIFICATE REQUEST-----")) ||
		block.Type != "CERTIFICATE REQUEST" || len(block.Headers) != 0 || len(bytes.TrimSpace(remaining)) != 0 {
		return idempotency.IdempotencyResponse{}, invalidCSR()
	}
	request, err := parseCSR(block.Bytes)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	csrDigest := sha256.Sum256(block.Bytes)
	return operator.mutate(ctx, humanMutation{
		method: http.MethodPost, route: EnrollmentRoute, key: key, hostID: hostID, status: http.StatusOK,
		fields: []intent.Field{
			{Name: "expected_generation", Value: intent.String(strconv.FormatUint(input.ExpectedGeneration, 10))},
			{Name: "csr_sha256", Value: intent.String(hex.EncodeToString(csrDigest[:]))},
		},
	}, func() (hostChange, error) {
		current, err := operator.service.read(ctx, hostID)
		if err != nil {
			return hostChange{}, err
		}
		next := current.Host
		now := operator.service.now().UTC()
		if next.Generation != input.ExpectedGeneration {
			return hostChange{}, staleHost()
		}
		if next.Agent == nil && (next.Generation == math.MaxUint64 ||
			next.Pending != nil && now.Before(next.Pending.ExpiresAt)) {
			return hostChange{}, errs.New(errs.KindStateConflict, "Remote Host already has a pending enrollment")
		}
		if next.Agent != nil && next.Agent.Renewal != nil && now.Before(next.Agent.Renewal.ExpiresAt) {
			return hostChange{}, errs.New(
				errs.KindStateConflict,
				"Remote Agent already has a pending certificate replacement",
			)
		}
		var identity record.Identity
		if next.Agent == nil {
			next.Generation++
			identity = next.Identity(ids.New(ids.KindAgent))
		} else {
			identity = next.Identity(next.Agent.AgentID)
		}
		der, expires, err := operator.service.authority.issue(identity, request, now)
		if err != nil {
			return hostChange{}, err
		}
		credential := record.CertificateCredential{Digest: sha256.Sum256(der), ExpiresAt: expires}
		if next.Agent == nil {
			// Use the same durable state as machine enrollment, with an
			// undisclosed random token. Only confirmation can consume it.
			var token EnrollmentToken
			if _, err := rand.Read(token.raw[:]); err != nil {
				return hostChange{}, errs.Wrap(errs.KindInternal, err)
			}
			tokenDigest := sha256.Sum256(token.raw[:])
			clear(token.raw[:])
			next.Pending = &record.PendingEnrollment{
				AgentID: identity.AgentID, KeyDigest: sha256.Sum256(request.RawSubjectPublicKeyInfo),
				TokenDigest: tokenDigest, ExpiresAt: now.Add(enrollmentLifetime), Certificate: &credential,
			}
		} else {
			draft := *next.Agent
			draft.Renewal = &credential
			next.Agent = &draft
		}
		result := api.RemoteHostEnrollment{
			Host: publicHost(hostView(next)), ExpiresAt: expires,
			Certificate: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		}
		return withHostResponse(hostChange{previous: current, next: next}, result)
	})
}

func (operator *Operator) RevokeRemoteHostEnrollment(
	ctx context.Context,
	hostID string,
	generation uint64,
	key string,
) (
	idempotency.IdempotencyResponse, error,
) {
	return operator.mutate(ctx, humanMutation{
		method: http.MethodDelete, route: EnrollmentRoute, key: key, hostID: hostID, status: http.StatusNoContent,
		fields: []intent.Field{{Name: "expected_generation", Value: intent.String(strconv.FormatUint(generation, 10))}},
	}, func() (hostChange, error) {
		current, err := operator.service.read(ctx, hostID)
		if err != nil {
			return hostChange{}, err
		}
		next := current.Host
		if generation == 0 || generation != next.Generation {
			return hostChange{}, staleHost()
		}
		next.Agent, next.Pending = nil, nil
		return hostChange{previous: current, next: next}, nil
	})
}

func publicHost(view HostView) api.RemoteHost {
	host := api.RemoteHost{
		ID:         view.ID,
		Name:       view.Name,
		State:      string(view.State),
		AgentID:    view.AgentID,
		Generation: view.Generation,
	}
	if !view.CredentialExpiry.IsZero() {
		host.CredentialExpiry = &view.CredentialExpiry
	}
	return host
}
