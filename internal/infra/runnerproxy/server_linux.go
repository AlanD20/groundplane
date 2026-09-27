package runnerproxy

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const dockerAPIVersion = "1.52"
const buildRequestTimeout = 30 * time.Minute

// Server is the closed workflow Docker boundary. The Controller supplies the
// current runtime receipt; no request can select a peer identity or daemon.
// Unsupported routes remain denied while additional capability handlers are
// implemented. This server must not be replaced with a generic reverse proxy.
type Server struct {
	authority PeerAuthority
	builds    *BuildExecutor
	spool     string
	buildSlot chan struct{}
}

func NewServer(authority PeerAuthority, builds *BuildExecutor, spool string) (*Server, error) {
	if !validPeerAuthority(authority) || builds == nil || spool == "" ||
		builds.policy.runnerID != authority.RunnerID || builds.policy.epoch != authority.RuntimeEpoch {
		return nil, buildEvidenceConflict()
	}
	return &Server{authority: authority, builds: builds, spool: spool, buildSlot: make(chan struct{}, 1)}, nil
}

// Serve takes a release-created Unix listener. The launcher owns its exact
// socket path, permissions and removal; this method neither unlinks nor adopts
// another process's socket. Closing the context revokes all in-flight requests.
func (proxy *Server) Serve(ctx context.Context, listener *net.UnixListener) error {
	if ctx == nil || listener == nil {
		return buildEvidenceConflict()
	}
	serveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	server := &http.Server{
		Handler: proxy, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 5 * time.Minute,
		WriteTimeout: buildRequestTimeout, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10,
		BaseContext: func(net.Listener) context.Context { return serveCtx },
		ConnContext: func(ctx context.Context, connection net.Conn) context.Context {
			authenticated, ok := connection.(interface{ Validate(context.Context) error })
			if !ok {
				return ctx
			}
			// The HTTP layer receives only revalidation, not the process handle
			// or connection implementation owned by the authenticated listener.
			return context.WithValue(ctx, peerContextKey{}, authenticated.Validate)
		},
	}
	authenticated := &peerListener{
		UnixListener: listener,
		ctx:          serveCtx,
		cancel:       cancel,
		authority:    proxy.authority,
		slots:        make(chan struct{}, 8),
	}
	stop := make(chan struct{})
	joined := make(chan struct{})
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			_ = server.Close() // Serve reports terminal transport errors; cancellation closes every peer.
		case <-stop:
		}
	}()
	err := server.Serve(authenticated)
	cancel()
	closeErr := server.Close()
	close(stop)
	<-joined
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return errs.Wrap(errs.KindInternal, errors.Join(err, closeErr))
	}
	if closeErr != nil {
		return errs.Wrap(errs.KindInternal, closeErr)
	}
	return ctx.Err()
}

type peerContextKey struct{}

func (proxy *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	validatePeer, ok := request.Context().Value(peerContextKey{}).(func(context.Context) error)
	if !ok || validatePeer(request.Context()) != nil {
		writeProxyFailure(writer, http.StatusForbidden, "Runner peer authority is unavailable")
		return
	}
	if request.URL.RawPath != "" || request.URL.Fragment != "" || request.URL.IsAbs() ||
		request.Header.Get("Upgrade") != "" || request.Header.Get("Content-Encoding") != "" {
		writeProxyFailure(writer, http.StatusForbidden, "unsupported Runner Docker request")
		return
	}
	path := strings.TrimPrefix(request.URL.Path, "/v"+dockerAPIVersion)
	switch {
	case path == "/_ping" && (request.Method == http.MethodHead || request.Method == http.MethodGet):
		if request.URL.RawQuery != "" || request.ContentLength > 0 || len(request.TransferEncoding) != 0 {
			writeProxyFailure(writer, 403, "unsupported ping input")
			return
		}
		writer.Header().Set("API-Version", dockerAPIVersion)
		writer.Header().Set("Docker-Experimental", "false")
		writer.Header().Set("OSType", "linux")
		writer.Header().Set("Content-Type", "text/plain")
		_, _ = writer.Write([]byte("OK")) // A disconnected health reader requires no recovery.
	case path == "/version" && request.Method == http.MethodGet:
		if request.URL.RawQuery != "" || request.ContentLength > 0 || len(request.TransferEncoding) != 0 {
			writeProxyFailure(writer, 403, "unsupported version input")
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(writer).Encode(struct {
			Version string `json:"Version"`
			API     string `json:"ApiVersion"`
			Minimum string `json:"MinAPIVersion"`
			OS      string `json:"Os"`
			Arch    string `json:"Arch"`
		}{"groundplane-runner", dockerAPIVersion, dockerAPIVersion, "linux", runtime.GOARCH})
	case path == "/build" && request.Method == http.MethodPost:
		proxy.build(writer, request, validatePeer)
	default:
		writeProxyFailure(writer, http.StatusForbidden, "Docker operation is outside the Runner policy")
	}
}

func (proxy *Server) build(
	writer http.ResponseWriter,
	request *http.Request,
	validatePeer func(context.Context) error,
) {
	// Registry credentials need their own explicit boundary; never forward an
	// ambient Docker CLI credential header to another daemon silently.
	registryHeader := request.Header.Values("X-Registry-Config")
	if len(registryHeader) > 1 || request.Header.Get("X-Registry-Auth") != "" {
		writeProxyFailure(writer, 403, "unsupported build authentication")
		return
	}
	if len(registryHeader) == 1 {
		decoded, err := base64.URLEncoding.DecodeString(registryHeader[0])
		empty := len(decoded) == 2 && decoded[0] == '{' && decoded[1] == '}'
		clear(decoded)
		if err != nil || !empty {
			writeProxyFailure(writer, 403, "unsupported build authentication")
			return
		}
	}
	ctx, cancel := context.WithTimeout(request.Context(), buildRequestTimeout)
	defer cancel()
	select {
	case proxy.buildSlot <- struct{}{}:
		defer func() { <-proxy.buildSlot }()
	case <-ctx.Done():
		return
	}
	defer request.Body.Close()
	prepared, err := PrepareBuildContext(
		ctx,
		proxy.spool,
		http.MaxBytesReader(writer, request.Body, maximumContextInput),
	)
	if err != nil {
		writeProxyFailure(writer, http.StatusBadRequest, "invalid Runner build context")
		return
	}
	defer prepared.Close()
	if validatePeer(ctx) != nil {
		writeProxyFailure(writer, http.StatusForbidden, "Runner peer authority was revoked")
		return
	}
	operationID := ids.New(ids.KindOperation)
	writer.Header().Set("Content-Type", "application/json")
	writer.Header().Set("X-Groundplane-Build-Operation", operationID)
	writer.Header().Add("Trailer", "X-Groundplane-Build-Image")
	stream := &buildResponseWriter{ResponseWriter: writer}
	receipt, err := proxy.builds.Build(ctx, operationID, request.URL.RawQuery, prepared, stream)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if !stream.started {
			writeProxyFailure(writer, http.StatusConflict, "Runner build was rejected or remains unresolved")
			return
		}
		_ = json.NewEncoder(stream).Encode(struct {
			Error string `json:"error"`
		}{"Runner build failed or remains unresolved"})
		return
	}
	writer.Header().Set("X-Groundplane-Build-Image", receipt.ImageID)
}

type buildResponseWriter struct {
	http.ResponseWriter
	started bool
}

func (writer *buildResponseWriter) Write(value []byte) (int, error) {
	writer.started = true
	count, err := writer.ResponseWriter.Write(value)
	if err == nil {
		err = http.NewResponseController(writer.ResponseWriter).Flush()
	}
	return count, err
}

func writeProxyFailure(writer http.ResponseWriter, status int, message string) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(struct {
		Message string `json:"message"`
	}{message})
}

type peerListener struct {
	*net.UnixListener
	ctx       context.Context
	cancel    context.CancelFunc
	authority PeerAuthority
	slots     chan struct{}
}

func (listener *peerListener) Close() error {
	// Accept may be waiting for capacity rather than inside AcceptUnix. Revoke
	// both waits, and every active request, before closing the underlying socket.
	listener.cancel()
	return listener.UnixListener.Close()
}

func (listener *peerListener) Accept() (net.Conn, error) {
	for {
		select {
		case listener.slots <- struct{}{}:
		case <-listener.ctx.Done():
			return nil, listener.ctx.Err()
		}
		connection, err := listener.AcceptUnix()
		if err != nil {
			<-listener.slots
			return nil, err
		}
		peer, err := AuthenticatePeer(listener.ctx, connection, listener.authority)
		if err != nil {
			_ = connection.Close() // Rejected peers never reach HTTP or Docker.
			<-listener.slots
			continue
		}
		return &peerConnection{UnixConn: connection, peer: peer, release: func() { <-listener.slots }}, nil
	}
}

type peerConnection struct {
	*net.UnixConn
	peer     *Peer
	release  func()
	once     sync.Once
	closeErr error
}

func (connection *peerConnection) Validate(ctx context.Context) error {
	return connection.peer.Validate(ctx)
}

func (connection *peerConnection) Close() error {
	connection.once.Do(func() {
		connection.closeErr = errors.Join(connection.UnixConn.Close(), connection.peer.Close())
		connection.release()
	})
	return connection.closeErr
}
