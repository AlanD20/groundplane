package handlers

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Delivery: pinned transport policy values; live enforcement is tested separately.
func TestProductionHTTPPolicyIsPinned(t *testing.T) {
	// Rationale: transport limits are invariants rather than operator tuning;
	// one accidental default change would alter every API and SSE route.
	policy := productionHTTPPolicy()
	if policy.readHeaderTimeout != 5*time.Second || policy.readTimeout != 0 || policy.writeTimeout != 0 {
		t.Fatalf(
			"read policy = (%s, %s, %s), want (5s, 0, 0)",
			policy.readHeaderTimeout,
			policy.readTimeout,
			policy.writeTimeout,
		)
	}
	if policy.idleTimeout != 60*time.Second || policy.maxHeaderBytes != 64<<10 {
		t.Fatalf("connection policy = (%s, %d), want (60s, 65536)", policy.idleTimeout, policy.maxHeaderBytes)
	}
	if policy.requestTimeout != 30*time.Second || policy.bodyReadTimeout != 30*time.Second ||
		policy.responseWriteTimeout != 10*time.Second {
		t.Fatalf(
			"ordinary deadlines = (%s, %s, %s), want (30s, 30s, 10s)",
			policy.requestTimeout,
			policy.bodyReadTimeout,
			policy.responseWriteTimeout,
		)
	}
	if policy.jsonBodyLimit != 1<<20 || policy.blueprintBodyLimit != 2<<20 {
		t.Fatalf(
			"body limits = (%d, %d), want (1048576, 2097152)",
			policy.jsonBodyLimit,
			policy.blueprintBodyLimit,
		)
	}
	if policy.sseHeartbeat != 15*time.Second || policy.sseWriteTimeout != 5*time.Second ||
		policy.shutdownGrace != 20*time.Second {
		t.Fatalf(
			"stream/shutdown policy = (%s, %s, %s), want (15s, 5s, 20s)",
			policy.sseHeartbeat,
			policy.sseWriteTimeout,
			policy.shutdownGrace,
		)
	}
}

// Delivery: registered Backup route policy; no Backup handler or effect is executed.
// Rationale: A bodyless Backup run must not silently inherit JSON input handling.
func TestBackupRunRouteIsBodyless(t *testing.T) {
	server := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), Options{})
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/v1/environments/env_1/backup-run",
		strings.NewReader(`{"source_ids":["backup-source_1"]}`),
	)
	_, pattern := server.Mux.Handler(request)
	if pattern != "POST /api/v1/environments/{id}/backup-run" {
		t.Fatalf("backup run pattern = %q", pattern)
	}
	if policy := server.policyFor(request); policy.body != bodyless {
		t.Fatalf("backup run body policy = %d, want bodyless", policy.body)
	}
}

// QA: TASK-06, LOG-02; local server configuration, not a running stream.
func TestNewHTTPServerAppliesOnlyNarrowGlobalTimeouts(t *testing.T) {
	// Rationale: nonzero server-wide read/write deadlines would either conflate
	// body classes or eventually kill a healthy indefinite SSE stream.
	policy := productionHTTPPolicy()
	server := newHTTPServer("127.0.0.1:0", http.NotFoundHandler(), context.Background(), policy)
	if server.ReadHeaderTimeout != policy.readHeaderTimeout || server.ReadTimeout != 0 || server.WriteTimeout != 0 ||
		server.IdleTimeout != policy.idleTimeout || server.MaxHeaderBytes != policy.maxHeaderBytes {
		t.Fatalf("http.Server policy was not applied exactly")
	}
}

// QA: UI-03, REL-04; loopback slow-header refusal with shortened policy only.
func TestIncompleteHeaderTimesOutBeforeDispatch(t *testing.T) {
	// Rationale: slow headers must consume a bounded connection without ever
	// entering application code or promising an RFC 7807 pre-dispatch response.
	var dispatched atomic.Bool
	policy := shortHTTPPolicy()
	policy.readHeaderTimeout = 25 * time.Millisecond
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.Mux.HandleFunc("/", func(http.ResponseWriter, *http.Request) { dispatched.Store(true) })
	})
	defer stopLifecycleServer(t, cancel, done)

	connection := dialLifecycleServer(t, addr)
	defer connection.Close()
	if _, err := io.WriteString(connection, "GET / HTTP/1.1\r\nHost:"); err != nil {
		t.Fatalf("write partial request: %v", err)
	}
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := bufio.NewReader(connection).ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("incomplete-header read = %v, want server EOF, not the test read deadline", err)
	}
	if dispatched.Load() {
		t.Fatal("handler ran for an incomplete request header")
	}
}

// QA: UI-03; loopback header refusal, not an application-formatted error.
func TestOversizedHeaderIsPlainPreDispatchFailure(t *testing.T) {
	// Rationale: MaxHeaderBytes is enforced before handlers, so the documented
	// RFC 7807 exception must be observable and must not dispatch application code.
	var dispatched atomic.Bool
	policy := shortHTTPPolicy()
	policy.maxHeaderBytes = 256
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.Mux.HandleFunc("/", func(http.ResponseWriter, *http.Request) { dispatched.Store(true) })
	})
	defer stopLifecycleServer(t, cancel, done)

	connection := dialLifecycleServer(t, addr)
	defer connection.Close()
	request := "GET / HTTP/1.1\r\nHost: groundplane\r\nX-Oversized: " + strings.Repeat("x", 8<<10) + "\r\n\r\n"
	if _, err := io.WriteString(connection, request); err != nil {
		t.Fatalf("write oversized request: %v", err)
	}
	response, err := http.ReadResponse(bufio.NewReader(connection), &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read oversized-header response: %v", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("oversized-header status = %d, want 431", response.StatusCode)
	}
	if dispatched.Load() {
		t.Fatal("handler ran for an oversized request header")
	}
	if response.Header.Get("Content-Type") == "application/problem+json" {
		t.Fatal("pre-dispatch net/http failure falsely used the application formatter")
	}
}

// QA: REL-04; loopback idle-socket release, not sustained resource qualification.
func TestIdleKeepAliveConnectionCloses(t *testing.T) {
	// Rationale: an inactive keep-alive socket must not hold a Controller file
	// descriptor indefinitely while active streams remain exempt.
	policy := shortHTTPPolicy()
	policy.idleTimeout = 25 * time.Millisecond
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.Mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok") })
	})
	defer stopLifecycleServer(t, cancel, done)

	connection := dialLifecycleServer(t, addr)
	defer connection.Close()
	if _, err := io.WriteString(connection, "GET / HTTP/1.1\r\nHost: groundplane\r\n\r\n"); err != nil {
		t.Fatalf("write request: %v", err)
	}
	reader := bufio.NewReader(connection)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatalf("read response body: %v", err)
	}
	response.Body.Close()
	if err := connection.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	if _, err := reader.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("idle connection read = %v, want server EOF, not the test read deadline", err)
	}
}

// QA: UI-05; fake-clock/writer deadline application only.
func TestOrdinaryResponseDeadlineStartsOnFirstWriteAndClears(t *testing.T) {
	// Rationale: an ordinary handler may compute most of its response window;
	// the write budget starts at its first output, not at request dispatch.
	writer := &recordingResponseWriter{header: make(http.Header)}
	policy := shortHTTPPolicy()
	now := time.Unix(100, 0)
	policy.now = func() time.Time { return now }
	lifecycle := newHTTPLifecycle(testControllerServer(), policy)
	handler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		now = time.Unix(107, 0)
		_, _ = io.WriteString(w, "ok")
	})
	lifecycle.serveOrdinary(writer, httptest.NewRequest(http.MethodGet, "/", nil), routePolicy{}, handler)
	want := []string{"deadline", "write", "clear"}
	if strings.Join(writer.operations, ",") != strings.Join(want, ",") {
		t.Fatalf("operations = %v, want %v", writer.operations, want)
	}
	if !slices.Equal(writer.deadlines, []time.Time{time.Unix(108, 0), {}}) {
		t.Fatalf("write deadlines = %v, want first-write time plus one second, then clear", writer.deadlines)
	}
}

// QA: UI-05; loopback handler cancellation, not durable mutation cancellation.
func TestOrdinaryRequestContextExpires(t *testing.T) {
	// Rationale: non-stream route work must observe its processing deadline
	// even when no request or response bytes are currently moving.
	policy := shortHTTPPolicy()
	policy.requestTimeout = 20 * time.Millisecond
	expired := make(chan struct{})
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.Mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
			close(expired)
			w.WriteHeader(http.StatusNoContent)
		})
	})
	defer stopLifecycleServer(t, cancel, done)
	response, err := openSSE(addr)
	if err != nil {
		t.Fatalf("ordinary request: %v", err)
	}
	response.Body.Close()
	select {
	case <-expired:
	case <-time.After(time.Second):
		t.Fatal("ordinary request context did not expire")
	}
}

// QA: UI-03; loopback transport ceiling, not semantic JSON validation.
func TestJSONBodyBoundariesKnownAndChunked(t *testing.T) {
	// Rationale: Content-Length is only an early optimization; MaxBytesReader
	// must enforce the same 413 contract for undeclared chunked bodies.
	for _, chunked := range []bool{false, true} {
		for _, size := range []int{32, 33} {
			policy := shortHTTPPolicy()
			policy.jsonBodyLimit = 32
			var dispatched atomic.Bool
			_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
				server.jsonRoute("POST /", func(w http.ResponseWriter, _ *http.Request) {
					dispatched.Store(true)
					w.WriteHeader(http.StatusNoContent)
				})
			})
			request, err := http.NewRequest(
				http.MethodPost,
				"http://"+addr+"/",
				bytes.NewReader(bytes.Repeat([]byte("x"), size)),
			)
			if err != nil {
				t.Fatalf("new request: %v", err)
			}
			if chunked {
				request.ContentLength = -1
			}
			response, err := http.DefaultClient.Do(request)
			if err != nil {
				t.Fatalf("request chunked=%t size=%d: %v", chunked, size, err)
			}
			if size == 32 {
				if response.StatusCode != http.StatusNoContent {
					t.Errorf("chunked=%t exact status = %d, want 204", chunked, response.StatusCode)
				}
				response.Body.Close()
			} else {
				assertRequestTooLarge(t, response)
				if dispatched.Load() {
					t.Errorf("chunked=%t oversized body reached handler", chunked)
				}
			}
			cancel()
			waitLifecycleServer(t, done)
		}
	}
}

// QA: BP-03; raw stream guard at an injected bound, not multipart decoding.
func TestRawBodyLimitDefersReadAndEnforcesKnownAndUnknownLengths(t *testing.T) {
	// Rationale: the raw-body guard used by Blueprint must reject declared
	// overflow before reading and leave accepted/unknown-length bodies streaming.
	server := testControllerServer()
	for _, known := range []bool{true, false} {
		for _, size := range []int{32, 33} {
			request := httptest.NewRequest(
				http.MethodPut,
				"/",
				io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("b"), size))),
			)
			request.ContentLength = int64(size)
			if !known {
				request.ContentLength = -1
			}
			body := &countedLifecycleRequestBody{ReadCloser: request.Body}
			request.Body = body
			response := httptest.NewRecorder()
			allowed := server.limitRequestBody(response, request, 32)
			if body.reads != 0 || allowed != (!known || size <= 32) {
				t.Fatalf("known=%t size=%d: allowed=%t reads=%d", known, size, allowed, body.reads)
			}
			if allowed {
				n, err := io.Copy(io.Discard, request.Body)
				if n != 32 {
					t.Fatalf("known=%t size=%d: exposed %d bytes, want 32", known, size, n)
				}
				if err != nil {
					server.writeBodyReadProblem(response, err)
				}
			}
			if size == 32 && response.Code != http.StatusOK {
				t.Errorf("known=%t exact status = %d, want untouched recorder", known, response.Code)
			}
			if size == 33 && response.Code != http.StatusRequestEntityTooLarge {
				t.Errorf("known=%t oversized status = %d, want 413", known, response.Code)
			}
		}
	}
}

// QA: TASK-06, LOG-02; loopback heartbeat liveness, not durable event sequencing.
func TestSSESurvivesOrdinaryDeadlineAndHeartbeats(t *testing.T) {
	// Rationale: an SSE stream must remain healthy beyond the ordinary request
	// deadline and make idle liveness visible with the exact accepted comment.
	policy := shortHTTPPolicy()
	policy.requestTimeout = 15 * time.Millisecond
	policy.sseHeartbeat = 8 * time.Millisecond
	events := make(chan []byte)
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.streamRoute("GET /", func(w http.ResponseWriter, r *http.Request) { server.writeSSE(w, r, events) })
	})
	defer stopLifecycleServer(t, cancel, done)

	response, err := openSSE(addr)
	if err != nil {
		t.Fatalf("open SSE: %v", err)
	}
	defer response.Body.Close()
	reader := bufio.NewReader(response.Body)
	started := time.Now()
	heartbeats := 0
	for time.Since(started) <= policy.requestTimeout || heartbeats < 2 {
		line, readErr := reader.ReadString('\n')
		if readErr != nil {
			t.Fatalf("read SSE heartbeat: %v", readErr)
		}
		if line == ": keepalive\n" {
			heartbeats++
		}
	}
}

// QA: TASK-06, UI-01; local Accept classification, not full content negotiation.
func TestStreamRouteRequiresEventStreamNegotiation(t *testing.T) {
	// Rationale: activity serves both the JSON task-list alias and live SSE;
	// only an explicit event-stream Accept value may bypass ordinary deadlines.
	server := testControllerServer()
	server.streamRoute("GET /", func(http.ResponseWriter, *http.Request) {})
	jsonRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	if server.policyFor(jsonRequest).streaming {
		t.Fatal("JSON request was classified as an indefinite stream")
	}
	streamRequest := httptest.NewRequest(http.MethodGet, "/", nil)
	streamRequest.Header.Set("Accept", "application/json, text/event-stream; charset=utf-8")
	if !server.policyFor(streamRequest).streaming {
		t.Fatal("explicit text/event-stream request was classified as ordinary")
	}
}

// QA: TASK-06, LOG-02; fake-clock/writer flush budget, not remote receipt.
func TestSSEWriteSetsFlushesAndClearsDeadline(t *testing.T) {
	// Rationale: every individual flush needs a renewable deadline; a single
	// absolute server timeout would terminate a healthy long-lived stream.
	writer := &recordingResponseWriter{header: make(http.Header)}
	policy := shortHTTPPolicy()
	policy.now = func() time.Time { return time.Unix(100, 0) }
	lifecycle := newHTTPLifecycle(testControllerServer(), policy)
	events := make(chan []byte, 1)
	events <- []byte("event: task\ndata: ready\n\n")
	close(events)
	lifecycle.serveSSE(writer, httptest.NewRequest(http.MethodGet, "/", nil), events)
	want := []string{"deadline", "header", "flush", "clear", "deadline", "write", "flush", "clear"}
	if strings.Join(writer.operations, ",") != strings.Join(want, ",") {
		t.Fatalf("operations = %v, want %v", writer.operations, want)
	}
	if !slices.Equal(writer.deadlines, []time.Time{time.Unix(101, 0), {}, time.Unix(101, 0), {}}) {
		t.Fatalf("SSE deadlines = %v, want one-second budgets and clears for headers and event", writer.deadlines)
	}
}

// QA: TASK-06, LOG-02; local source-close/body-read boundary.
func TestSSEDoesNotConsumeRequestBodyAndStopsWhenSourceCloses(t *testing.T) {
	// Rationale: SSE is a bodyless route and a closed event source is a normal
	// termination signal, not a reason to wait for the next heartbeat.
	writer := &recordingResponseWriter{header: make(http.Header)}
	lifecycle := newHTTPLifecycle(testControllerServer(), shortHTTPPolicy())
	events := make(chan []byte)
	close(events)
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Body = panicReadCloser{}
	lifecycle.serveSSE(writer, request, events)
}

// QA: TASK-06, LOG-02; loopback handler exit, not external subscription cleanup.
func TestSSEStopsWhenClientDisconnects(t *testing.T) {
	// Rationale: disconnect cancellation must release the per-stream goroutine
	// rather than waiting for process shutdown or an event source close.
	policy := shortHTTPPolicy()
	policy.sseHeartbeat = 8 * time.Millisecond
	exited := make(chan struct{})
	events := make(chan []byte)
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.streamRoute("GET /", func(w http.ResponseWriter, r *http.Request) {
			defer close(exited)
			server.writeSSE(w, r, events)
		})
	})
	defer stopLifecycleServer(t, cancel, done)
	response, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("open SSE: %v", err)
	}
	response.Body.Close()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("SSE handler did not exit after client disconnect")
	}
}

// QA: TASK-06, LOG-02; loopback non-reading client, not production load.
func TestSSEStalledClientHitsPerWriteDeadline(t *testing.T) {
	// Rationale: a client that stops reading must not block one Controller
	// response goroutine indefinitely even though global WriteTimeout is zero.
	policy := shortHTTPPolicy()
	policy.sseWriteTimeout = 20 * time.Millisecond
	exited := make(chan struct{})
	events := make(chan []byte, 1)
	events <- append(append([]byte("data: "), bytes.Repeat([]byte("x"), 16<<20)...), '\n', '\n')
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.streamRoute("GET /", func(w http.ResponseWriter, r *http.Request) {
			defer close(exited)
			server.writeSSE(w, r, events)
		})
	})
	defer stopLifecycleServer(t, cancel, done)
	connection := dialLifecycleServer(t, addr)
	defer connection.Close()
	if _, err := io.WriteString(
		connection,
		"GET / HTTP/1.1\r\nHost: groundplane\r\nAccept: text/event-stream\r\n\r\n",
	); err != nil {
		t.Fatalf("write SSE request: %v", err)
	}
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("stalled SSE write did not hit its deadline")
	}
}

// QA: REL-01; loopback request completion during shutdown, not GP runtime restart.
func TestGracefulShutdownLetsOrdinaryRequestFinish(t *testing.T) {
	// Rationale: process cancellation starts draining but must not cancel an
	// ordinary in-flight request that can complete inside the grace interval.
	policy := shortHTTPPolicy()
	policy.shutdownGrace = time.Second
	started := make(chan struct{})
	release := make(chan struct{})
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.Mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
			close(started)
			<-release
			_, _ = io.WriteString(w, "finished")
		})
	})
	responseDone := make(chan string, 1)
	go func() {
		response, err := http.Get("http://" + addr + "/")
		if err != nil {
			responseDone <- "request error: " + err.Error()
			return
		}
		body, _ := io.ReadAll(response.Body)
		response.Body.Close()
		responseDone <- string(body)
	}()
	<-started
	cancel()
	close(release)
	if got := <-responseDone; got != "finished" {
		t.Fatalf("response = %q, want finished", got)
	}
	waitLifecycleServer(t, done)
}

// QA: REL-01, TASK-06, LOG-02; loopback stream drain, not host continuity.
func TestGracefulShutdownDrainsSSE(t *testing.T) {
	// Rationale: Shutdown does not close active streams itself, so the explicit
	// drain signal must release SSE before the grace deadline.
	policy := shortHTTPPolicy()
	policy.shutdownGrace = time.Second
	events := make(chan []byte)
	exited := make(chan struct{})
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.streamRoute("GET /", func(w http.ResponseWriter, r *http.Request) {
			defer close(exited)
			server.writeSSE(w, r, events)
		})
	})
	response, err := openSSE(addr)
	if err != nil {
		t.Fatalf("open SSE: %v", err)
	}
	cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("SSE did not exit on drain")
	}
	response.Body.Close()
	waitLifecycleServer(t, done)
}

// QA: REL-01; expired grace cancels handler base and stops serving; no runtime restart.
func TestGraceExpiryCancelsRequestBaseAndStopsServing(t *testing.T) {
	// Rationale: an ordinary handler that ignores its processing deadline must
	// still receive base cancellation and allow serving to stop after grace expires.
	policy := shortHTTPPolicy()
	policy.requestTimeout = time.Hour
	policy.shutdownGrace = 25 * time.Millisecond
	started := make(chan struct{})
	cancelled := make(chan struct{})
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.Mux.HandleFunc("/", func(_ http.ResponseWriter, r *http.Request) {
			close(started)
			<-r.Context().Done()
			close(cancelled)
		})
	})
	go func() {
		response, err := http.Get("http://" + addr + "/")
		if err == nil {
			response.Body.Close()
		}
	}()
	<-started
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("grace expiry did not cancel the detached request base")
	}
	waitLifecycleServer(t, done)
}

// QA: REL-01, UI-05; loopback drain/accept race, not native upgrade recovery.
func TestDrainRaceReturns503ThenClosesListener(t *testing.T) {
	// Rationale: a connection accepted between the drain flag and listener
	// closure must receive a deterministic 503 and must not remain reusable.
	policy := shortHTTPPolicy()
	draining := make(chan struct{})
	releaseShutdown := make(chan struct{})
	policy.beforeShutdown = func() {
		close(draining)
		<-releaseShutdown
	}
	_, addr, cancel, done := startLifecycleServer(t, policy, func(server *Server) {
		server.Mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	})
	cancel()
	<-draining
	response, err := http.Get("http://" + addr + "/")
	if err != nil {
		t.Fatalf("request during listener-close race: %v", err)
	}
	if response.StatusCode != http.StatusServiceUnavailable || !response.Close {
		t.Errorf("drain response = (%d, close=%t), want (503, true)", response.StatusCode, response.Close)
	}
	response.Body.Close()
	close(releaseShutdown)
	waitLifecycleServer(t, done)
	if connection, dialErr := net.DialTimeout("tcp", addr, 50*time.Millisecond); dialErr == nil {
		connection.Close()
		t.Fatal("new connection succeeded after shutdown")
	}
}

type countedLifecycleRequestBody struct {
	io.ReadCloser
	reads int
}

func (body *countedLifecycleRequestBody) Read(buffer []byte) (int, error) {
	body.reads++
	return body.ReadCloser.Read(buffer)
}

type panicReadCloser struct{}

func (panicReadCloser) Read([]byte) (int, error) { panic("SSE request body was read") }
func (panicReadCloser) Close() error             { return nil }

type recordingResponseWriter struct {
	header     http.Header
	operations []string
	deadlines  []time.Time
}

func (w *recordingResponseWriter) Header() http.Header { return w.header }
func (w *recordingResponseWriter) WriteHeader(int)     { w.operations = append(w.operations, "header") }
func (w *recordingResponseWriter) Write(body []byte) (int, error) {
	w.operations = append(w.operations, "write")
	return len(body), nil
}
func (w *recordingResponseWriter) FlushError() error {
	w.operations = append(w.operations, "flush")
	return nil
}
func (w *recordingResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	if deadline.IsZero() {
		w.operations = append(w.operations, "clear")
	} else {
		w.operations = append(w.operations, "deadline")
	}
	return nil
}

func shortHTTPPolicy() httpPolicy {
	policy := productionHTTPPolicy()
	policy.readHeaderTimeout = time.Second
	policy.idleTimeout = time.Second
	policy.requestTimeout = time.Second
	policy.bodyReadTimeout = time.Second
	policy.responseWriteTimeout = time.Second
	policy.sseHeartbeat = time.Second
	policy.sseWriteTimeout = time.Second
	policy.shutdownGrace = time.Second
	return policy
}

func testControllerServer() *Server {
	return &Server{
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		Mux:           http.NewServeMux(),
		routePolicies: make(map[string]routePolicy),
	}
}

func startLifecycleServer(
	t *testing.T,
	policy httpPolicy,
	configure func(*Server),
) (*Server, string, context.CancelFunc, <-chan error) {
	t.Helper()
	server := testControllerServer()
	configure(server)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.serveListener(ctx, listener, policy) }()
	return server, listener.Addr().String(), cancel, done
}

func stopLifecycleServer(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	waitLifecycleServer(t, done)
}

func waitLifecycleServer(t *testing.T, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serve lifecycle: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("serve lifecycle did not stop")
	}
}

func dialLifecycleServer(t *testing.T, addr string) net.Conn {
	t.Helper()
	connection, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return connection
}

func openSSE(addr string) (*http.Response, error) {
	request, err := http.NewRequest(http.MethodGet, "http://"+addr+"/", nil)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "text/event-stream")
	return http.DefaultClient.Do(request)
}

func assertRequestTooLarge(t *testing.T, response *http.Response) {
	t.Helper()
	defer response.Body.Close()
	if response.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", response.StatusCode)
	}
	var problem errs.Problem
	if err := json.NewDecoder(response.Body).Decode(&problem); err != nil {
		t.Fatalf("decode problem: %v", err)
	}
	if problem.Status != http.StatusRequestEntityTooLarge || problem.Code != errs.CodeRequestFailed {
		t.Fatalf("problem = %#v, want 413 request.failed", problem)
	}
}
