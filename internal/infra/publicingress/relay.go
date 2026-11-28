package publicingress

import (
	"context"
	"crypto/subtle"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ingressexecution"
	"github.com/AlanD20/groundplane/internal/common/jcs"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RunRelay is the independent serving process. It has no Docker or
// Controller credentials, no mutation or control socket, and reads only the
// owned activation directory mounted read-only.
func RunRelay(ctx context.Context, listen netip.AddrPort, peer netip.Addr) error {
	root, err := os.OpenRoot(RelayStateRoot)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	defer root.Close()
	source := &relaySource{root: root, listen: listen}
	server := &http.Server{
		Handler: newRelay(source, peer), ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 64 << 10,
	}
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp4", listen.String())
	if err != nil {
		return errs.New(errs.KindStateConflict, "public ingress private relay cannot bind its exact address")
	}
	defer listener.Close()
	stop := context.AfterFunc(ctx, func() { _ = server.Close() })
	defer stop()
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

type relaySource struct {
	root   *os.Root
	listen netip.AddrPort
	mu     sync.Mutex
	file   os.FileInfo
	name   string
	routes []routeOrigin
}

func (source *relaySource) read() []routeOrigin {
	source.mu.Lock()
	defer source.mu.Unlock()
	name := stateFilename
	info, err := source.root.Lstat(name)
	if os.IsNotExist(err) && source.name != stateFilename {
		name = StartupFilename
		info, err = source.root.Lstat(name)
	}
	if err != nil || os.SameFile(info, source.file) && source.name == name {
		return source.routes
	}
	raw, _, err := (&stateDirectory{root: source.root}).read(name, maximumStateBytes)
	if err != nil || len(raw) == 0 {
		return source.routes
	}
	files := [][]byte{raw}
	if name == stateFilename {
		state, err := jcs.Decode[activation](raw)
		if err != nil || !recordedConfigValid(state.Current) ||
			state.Candidate != nil && !recordedConfigValid(*state.Candidate) {
			return source.routes
		}
		files = [][]byte{state.Current.JSON}
		if state.Candidate != nil && !state.Restoring {
			files = append(files, state.Candidate.JSON)
		}
	}
	allRoutes := make([]routeOrigin, 0)
	for _, file := range files {
		routes, err := routeOrigins(file)
		if err != nil {
			return source.routes
		}
		for _, route := range routes {
			if route.relay != source.listen || route.proof == "" {
				return source.routes
			}
		}
		allRoutes = append(allRoutes, routes...)
	}
	source.routes, source.name, source.file = allRoutes, name, info
	return allRoutes
}

func newRelay(source *relaySource, peer netip.Addr) http.Handler {
	transport := &http.Transport{
		DialContext:     (&net.Dialer{Timeout: probeTimeout}).DialContext,
		IdleConnTimeout: 90 * time.Second, MaxIdleConns: 256,
		ResponseHeaderTimeout: 60 * time.Second, MaxResponseHeaderBytes: 64 << 10,
	}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		remote, err := netip.ParseAddrPort(request.RemoteAddr)
		if err != nil || remote.Addr() != peer {
			http.NotFound(response, request)
			return
		}
		target, ok := relayTarget(request, source.read())
		if !ok {
			http.NotFound(response, request)
			return
		}
		proxy := httputil.ReverseProxy{
			Transport: transport,
			Rewrite: func(out *httputil.ProxyRequest) {
				out.Out.URL.Scheme, out.Out.URL.Host = "http", target.String()
				out.Out.Host = out.In.Host
				out.Out.Header.Del(ingressexecution.RelayOriginHeader)
				out.Out.Header.Del(ingressexecution.RelayProofHeader)
				for _, name := range []string{"X-Forwarded-For", "X-Forwarded-Host", "X-Forwarded-Proto"} {
					for _, value := range out.In.Header.Values(name) {
						out.Out.Header.Add(name, value)
					}
				}
			},
			ModifyResponse: func(response *http.Response) error {
				response.Header.Del(ingressexecution.RelayOriginHeader)
				response.Header.Del(ingressexecution.RelayProofHeader)
				return nil
			},
			ErrorHandler: func(response http.ResponseWriter, _ *http.Request, _ error) {
				http.Error(response, "Bad Gateway", http.StatusBadGateway)
			},
		}
		proxy.ServeHTTP(response, request)
	})
}

func relayTarget(request *http.Request, routes []routeOrigin) (netip.AddrPort, bool) {
	var none netip.AddrPort
	path := request.URL.Path
	if request.Method == http.MethodConnect || request.URL.IsAbs() ||
		request.URL.Host != "" || request.URL.RawPath != "" || !strings.HasPrefix(path, "/") ||
		strings.ContainsAny(path, "%\\") || strings.Contains(path, "//") {
		return none, false
	}
	for segment := range strings.SplitSeq(path, "/") {
		if segment == "." || segment == ".." {
			return none, false
		}
	}
	host := strings.ToLower(request.Host)
	if withoutPort, port, err := net.SplitHostPort(host); err == nil {
		if port != strconv.Itoa(443) {
			return none, false
		}
		host = withoutPort
	}
	addresses := request.Header.Values(ingressexecution.RelayOriginHeader)
	proofs := request.Header.Values(ingressexecution.RelayProofHeader)
	if len(addresses) != 1 || len(proofs) != 1 || len(proofs[0]) != 64 {
		return none, false
	}
	for _, route := range routes {
		if route.host == host && matchesPath(route.path, path) && route.dial.String() == addresses[0] &&
			subtle.ConstantTimeCompare([]byte(route.proof), []byte(proofs[0])) == 1 {
			return route.dial, true
		}
	}
	return none, false
}
