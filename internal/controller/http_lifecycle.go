package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	productionReadHeaderTimeout  = 5 * time.Second
	productionIdleTimeout        = 60 * time.Second
	productionMaxHeaderBytes     = 64 << 10
	productionRequestTimeout     = 30 * time.Second
	productionBodyReadTimeout    = 30 * time.Second
	productionResponseTimeout    = 10 * time.Second
	productionJSONBodyLimit      = 1 << 20
	productionBlueprintBodyLimit = 2 << 20
	productionSSEHeartbeat       = 15 * time.Second
	productionSSEWriteTimeout    = 5 * time.Second
	productionShutdownGrace      = 20 * time.Second

	sseHeartbeatFrame = ": keepalive\n\n"
)

type httpPolicy struct {
	readHeaderTimeout time.Duration
	readTimeout       time.Duration
	writeTimeout      time.Duration
	idleTimeout       time.Duration
	maxHeaderBytes    int

	requestTimeout       time.Duration
	bodyReadTimeout      time.Duration
	responseWriteTimeout time.Duration
	jsonBodyLimit        int64
	blueprintBodyLimit   int64
	sseHeartbeat         time.Duration
	sseWriteTimeout      time.Duration
	shutdownGrace        time.Duration
	now                  func() time.Time

	// beforeShutdown is nil in production. Tests use it to hold open the
	// narrow interval after draining begins and before the listener closes.
	beforeShutdown func()
}

func productionHTTPPolicy() httpPolicy {
	return httpPolicy{
		readHeaderTimeout:    productionReadHeaderTimeout,
		readTimeout:          0,
		writeTimeout:         0,
		idleTimeout:          productionIdleTimeout,
		maxHeaderBytes:       productionMaxHeaderBytes,
		requestTimeout:       productionRequestTimeout,
		bodyReadTimeout:      productionBodyReadTimeout,
		responseWriteTimeout: productionResponseTimeout,
		jsonBodyLimit:        productionJSONBodyLimit,
		blueprintBodyLimit:   productionBlueprintBodyLimit,
		sseHeartbeat:         productionSSEHeartbeat,
		sseWriteTimeout:      productionSSEWriteTimeout,
		shutdownGrace:        productionShutdownGrace,
		now:                  time.Now,
	}
}

type bodyClass uint8

const (
	bodyless bodyClass = iota
	jsonBody
	blueprintBody
)

type routePolicy struct {
	body      bodyClass
	streaming bool
}

func (s *Server) jsonRoute(pattern string, handler http.HandlerFunc) {
	s.Mux.HandleFunc(pattern, handler)
	s.setRoutePolicy(pattern, routePolicy{body: jsonBody})
}

func (s *Server) streamRoute(pattern string, handler http.HandlerFunc) {
	s.Mux.HandleFunc(pattern, handler)
	s.setRoutePolicy(pattern, routePolicy{streaming: true})
}

func (s *Server) blueprintRoute(pattern string, handler http.HandlerFunc) {
	s.Mux.HandleFunc(pattern, handler)
	s.setRoutePolicy(pattern, routePolicy{body: blueprintBody})
}

func (s *Server) setRoutePolicy(pattern string, policy routePolicy) {
	if s.routePolicies == nil {
		s.routePolicies = make(map[string]routePolicy)
	}
	s.routePolicies[pattern] = policy
}

func (s *Server) policyFor(r *http.Request) routePolicy {
	_, pattern := s.Mux.Handler(r)
	policy := s.routePolicies[pattern]
	policy.streaming = policy.streaming && acceptsEventStream(r)
	return policy
}

func acceptsEventStream(r *http.Request) bool {
	for _, value := range r.Header.Values("Accept") {
		for _, candidate := range strings.Split(value, ",") {
			mediaType, _, err := mime.ParseMediaType(strings.TrimSpace(candidate))
			if err == nil && mediaType == "text/event-stream" {
				return true
			}
		}
	}
	return false
}

type lifecycleContextKey struct{}

type httpLifecycle struct {
	server    *Server
	policy    httpPolicy
	draining  atomic.Bool
	drain     chan struct{}
	drainOnce sync.Once
}

func newHTTPLifecycle(server *Server, policy httpPolicy) *httpLifecycle {
	return &httpLifecycle{server: server, policy: policy, drain: make(chan struct{})}
}

func (l *httpLifecycle) beginDrain() {
	l.draining.Store(true)
	l.drainOnce.Do(func() { close(l.drain) })
}

func (l *httpLifecycle) handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if l.draining.Load() {
			w.Header().Set("Connection", "close")
			l.server.writeRequestProblem(w, http.StatusServiceUnavailable, "controller is draining")
			return
		}

		r = r.WithContext(context.WithValue(r.Context(), lifecycleContextKey{}, l))
		route := l.server.policyFor(r)
		if route.streaming {
			next.ServeHTTP(w, r)
			return
		}
		l.serveOrdinary(w, r, route, next)
	})
}

func (l *httpLifecycle) serveOrdinary(
	w http.ResponseWriter,
	r *http.Request,
	route routePolicy,
	next http.Handler,
) {
	ctx, cancel := context.WithTimeout(r.Context(), l.policy.requestTimeout)
	defer cancel()
	r = r.WithContext(ctx)

	writer := newResponseDeadlineWriter(w, l.policy.responseWriteTimeout, l.policy.now)
	defer writer.clear(l.server.Logger)

	if route.body != bodyless {
		controller := http.NewResponseController(w)
		if err := controller.SetReadDeadline(l.policy.now().Add(l.policy.bodyReadTimeout)); err != nil {
			l.server.writeProblem(writer, errs.New(errs.KindInternal, "request deadline is unavailable"))
			return
		}
		defer clearReadDeadline(controller, l.server.Logger)
	}

	switch route.body {
	case jsonBody:
		if !l.server.prepareJSONBody(writer, r, l.policy.jsonBodyLimit) {
			return
		}
	case blueprintBody:
		if !l.server.limitRequestBody(writer, r, l.policy.blueprintBodyLimit) {
			return
		}
	}

	next.ServeHTTP(writer, r)
}

type responseDeadlineWriter struct {
	http.ResponseWriter
	controller *http.ResponseController
	timeout    time.Duration
	now        func() time.Time
	started    bool
	startErr   error
}

func newResponseDeadlineWriter(w http.ResponseWriter, timeout time.Duration, now func() time.Time) *responseDeadlineWriter {
	return &responseDeadlineWriter{
		ResponseWriter: w,
		controller:     http.NewResponseController(w),
		timeout:        timeout,
		now:            now,
	}
}

func (w *responseDeadlineWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseDeadlineWriter) WriteHeader(status int) {
	if w.start() != nil {
		return
	}
	w.ResponseWriter.WriteHeader(status)
}

func (w *responseDeadlineWriter) Write(body []byte) (int, error) {
	if err := w.start(); err != nil {
		return 0, err
	}
	return w.ResponseWriter.Write(body)
}

func (w *responseDeadlineWriter) start() error {
	if w.started {
		return w.startErr
	}
	w.started = true
	w.startErr = w.controller.SetWriteDeadline(w.now().Add(w.timeout))
	return w.startErr
}

func (w *responseDeadlineWriter) clear(logger *slog.Logger) {
	if !w.started || w.startErr != nil {
		return
	}
	if err := w.controller.SetWriteDeadline(time.Time{}); err != nil && logger != nil {
		logger.Error("controller: clear response write deadline", slog.Any("error", err))
	}
}

func clearReadDeadline(controller *http.ResponseController, logger *slog.Logger) {
	if err := controller.SetReadDeadline(time.Time{}); err != nil && logger != nil {
		logger.Error("controller: clear request read deadline", slog.Any("error", err))
	}
}

func (s *Server) prepareJSONBody(w http.ResponseWriter, r *http.Request, limit int64) bool {
	if !s.limitRequestBody(w, r, limit) {
		return false
	}
	if r.Body == nil || r.Body == http.NoBody {
		return true
	}

	body, err := io.ReadAll(r.Body)
	closeErr := r.Body.Close()
	if err != nil {
		s.writeBodyReadProblem(w, err)
		return false
	}
	if closeErr != nil {
		s.writeRequestProblem(w, http.StatusBadRequest, "malformed request body")
		return false
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	return true
}

// limitRequestBody performs the transport check shared by JSON and Blueprint
// routes. Blueprint handlers keep the wrapped stream and use MultipartReader;
// they never buffer the multipart request as a single byte slice.
func (s *Server) limitRequestBody(w http.ResponseWriter, r *http.Request, limit int64) bool {
	if r.ContentLength > limit {
		s.writeRequestProblem(w, http.StatusRequestEntityTooLarge, "request body exceeds route limit")
		return false
	}
	if r.Body != nil && r.Body != http.NoBody {
		// MaxBytesReader uses net/http's concrete writer to mark the connection
		// non-reusable after overflow. Peel Groundplane wrappers only for that
		// transport hook; problem responses still use the deadline writer.
		r.Body = http.MaxBytesReader(unwrapResponseWriter(w), r.Body, limit)
	}
	return true
}

func unwrapResponseWriter(w http.ResponseWriter) http.ResponseWriter {
	for {
		unwrapper, ok := w.(interface{ Unwrap() http.ResponseWriter })
		if !ok {
			return w
		}
		next := unwrapper.Unwrap()
		if next == nil || next == w {
			return w
		}
		w = next
	}
}

func (s *Server) writeBodyReadProblem(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		s.writeRequestProblem(w, http.StatusRequestEntityTooLarge, "request body exceeds route limit")
		return
	}
	s.writeRequestProblem(w, http.StatusBadRequest, "malformed request body")
}

func (s *Server) writeRequestProblem(w http.ResponseWriter, status int, detail string) {
	problem := requestProblem(status, detail, nil)
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(problem.HTTPStatus())
	if err := json.NewEncoder(w).Encode(problem); err != nil && s.Logger != nil {
		s.Logger.Error("controller: write request problem", slog.Any("error", err))
	}
}

// writeSSE streams complete SSE frames supplied by a product handler. It owns
// only transport lifecycle; event names and payloads remain product concerns.
func (s *Server) writeSSE(w http.ResponseWriter, r *http.Request, events <-chan []byte) {
	lifecycle, ok := r.Context().Value(lifecycleContextKey{}).(*httpLifecycle)
	if !ok {
		s.writeProblem(w, errs.New(errs.KindInternal, "HTTP lifecycle is unavailable"))
		return
	}
	lifecycle.serveSSE(w, r, events)
}

func (l *httpLifecycle) serveSSE(w http.ResponseWriter, r *http.Request, events <-chan []byte) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	controller := http.NewResponseController(w)
	if !flushSSEHeaders(controller, w, l.policy.sseWriteTimeout, l.policy.now) {
		return
	}

	ticker := time.NewTicker(l.policy.sseHeartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-l.drain:
			return
		case frame, ok := <-events:
			if !ok || !writeSSEFrame(controller, w, frame, l.policy.sseWriteTimeout, l.policy.now) {
				return
			}
		case <-ticker.C:
			if !writeSSEFrame(
				controller,
				w,
				[]byte(sseHeartbeatFrame),
				l.policy.sseWriteTimeout,
				l.policy.now,
			) {
				return
			}
		}
	}
}

func flushSSEHeaders(
	controller *http.ResponseController,
	w http.ResponseWriter,
	timeout time.Duration,
	now func() time.Time,
) bool {
	if err := controller.SetWriteDeadline(now().Add(timeout)); err != nil {
		return false
	}
	w.WriteHeader(http.StatusOK)
	if err := controller.Flush(); err != nil {
		return false
	}
	return controller.SetWriteDeadline(time.Time{}) == nil
}

func writeSSEFrame(
	controller *http.ResponseController,
	w http.ResponseWriter,
	frame []byte,
	timeout time.Duration,
	now func() time.Time,
) bool {
	if err := controller.SetWriteDeadline(now().Add(timeout)); err != nil {
		return false
	}
	if _, err := w.Write(frame); err != nil {
		return false
	}
	if err := controller.Flush(); err != nil {
		return false
	}
	return controller.SetWriteDeadline(time.Time{}) == nil
}

type readyListener struct {
	net.Listener
	ready chan struct{}
	once  sync.Once
}

func (l *readyListener) Accept() (net.Conn, error) {
	l.once.Do(func() { close(l.ready) })
	return l.Listener.Accept()
}

// Serve starts the HTTP server and blocks until ctx is cancelled. Request
// contexts retain ctx values but are detached from its cancellation so the
// graceful interval can finish ordinary work.
func (s *Server) Serve(ctx context.Context, addr string) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return s.serveListener(ctx, listener, productionHTTPPolicy())
}

func (s *Server) serveListener(ctx context.Context, listener net.Listener, policy httpPolicy) error {
	requestBase, cancelAll := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelAll()

	lifecycle := newHTTPLifecycle(s, policy)
	srv := newHTTPServer(listener.Addr().String(), lifecycle.handler(s.requestHandler()), requestBase, policy)
	trackedListener := &readyListener{Listener: listener, ready: make(chan struct{})}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(trackedListener) }()
	if s.Logger != nil {
		s.Logger.Info("controller: listening", "addr", listener.Addr().String())
	}

	select {
	case <-trackedListener.ready:
	case err := <-serveErr:
		return classifyServeError(err)
	}

	select {
	case err := <-serveErr:
		return classifyServeError(err)
	case <-ctx.Done():
	}

	lifecycle.beginDrain()
	if policy.beforeShutdown != nil {
		policy.beforeShutdown()
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), policy.shutdownGrace)
	shutdownErr := srv.Shutdown(shutdownCtx)
	cancelShutdown()

	var closeErr error
	if shutdownErr != nil {
		cancelAll()
		closeErr = srv.Close()
		if errors.Is(shutdownErr, context.DeadlineExceeded) && s.Logger != nil {
			s.Logger.Warn(
				"controller: graceful HTTP shutdown deadline expired",
				slog.Duration("grace", policy.shutdownGrace),
			)
		}
	} else {
		cancelAll()
	}

	return classifyShutdownErrors(<-serveErr, shutdownErr, closeErr)
}

func newHTTPServer(addr string, handler http.Handler, requestBase context.Context, policy httpPolicy) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: policy.readHeaderTimeout,
		ReadTimeout:       policy.readTimeout,
		WriteTimeout:      policy.writeTimeout,
		IdleTimeout:       policy.idleTimeout,
		MaxHeaderBytes:    policy.maxHeaderBytes,
		BaseContext: func(net.Listener) context.Context {
			return requestBase
		},
	}
}

func classifyShutdownErrors(serveErr, shutdownErr, closeErr error) error {
	var unexpected []error
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		unexpected = append(unexpected, serveErr)
	}
	if shutdownErr != nil && !errors.Is(shutdownErr, context.DeadlineExceeded) {
		unexpected = append(unexpected, shutdownErr)
	}
	if closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
		unexpected = append(unexpected, closeErr)
	}
	if err := errors.Join(unexpected...); err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	return nil
}

func classifyServeError(err error) error {
	if err == nil || errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return errs.Wrap(errs.KindInternal, err)
}
