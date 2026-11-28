package handlers

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"net/http"

	"github.com/AlanD20/groundplane/internal/infra/httplistener"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RunnerListener is an independently authenticated machine entry point. It is
// bound with the operator listeners but never serves their mux.
type RunnerListener struct {
	Address string
	Handler http.Handler
	TLS     *tls.Config
}

// Serve binds every configured address before accepting requests. Readiness
// requires every HTTP server to have entered Accept; a partial binding failure
// closes all opened listeners without ever qualifying a native upgrade.
func (s *Server) Serve(ctx context.Context, addresses []string) error {
	if s.hostTerminals != nil {
		defer s.hostTerminals.Close()
	}
	if ctx == nil {
		return errs.New(errs.KindInternal, "controller HTTP context is required")
	}
	type binding struct {
		listener   *readyListener
		restricted http.Handler
		streaming  bool
	}
	listeners := make([]binding, 0, len(addresses)+3)
	closeOpened := func() {
		for _, opened := range listeners {
			_ = opened.listener.Close()
		}
	}
	for _, address := range addresses {
		listener, err := httplistener.Listen(ctx, address)
		if err != nil {
			closeOpened()
			return errs.Wrap(errs.KindInternal, err)
		}
		listeners = append(listeners, binding{listener: &readyListener{Listener: listener, ready: make(chan struct{})}})
	}
	if len(listeners) == 0 {
		return errs.New(errs.KindValidationFailed, "controller HTTP listeners are required")
	}
	for _, runner := range []*RunnerListener{s.runnerListener, s.runnerRegistryListener, s.githubWebhookListener} {
		if runner == nil {
			continue
		}
		if runner.Address == "" || runner.Handler == nil || (runner == s.runnerRegistryListener && runner.TLS == nil) {
			closeOpened()
			return errs.New(errs.KindInternal, "machine HTTP listener is incomplete")
		}
		listener, err := httplistener.Listen(ctx, runner.Address)
		if err != nil {
			closeOpened()
			return errs.Wrap(errs.KindInternal, err)
		}
		var transport net.Listener = listener
		if runner.TLS != nil {
			transport = tls.NewListener(listener, runner.TLS)
		}
		listeners = append(listeners, binding{
			listener:   &readyListener{Listener: transport, ready: make(chan struct{})},
			restricted: runner.Handler, streaming: runner == s.runnerRegistryListener,
		})
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(listeners))
	for _, bound := range listeners {
		go func() {
			policy := productionHTTPPolicy()
			if bound.restricted != nil {
				if !bound.streaming {
					policy.readTimeout, policy.writeTimeout = policy.requestTimeout, policy.requestTimeout
				}
				results <- s.serveHTTPHandler(runCtx, bound.listener, policy, s.withRequestReference(bound.restricted), func() {})
				return
			}
			results <- s.serveListener(runCtx, bound.listener, policy)
		}()
	}
	readyDone := make(chan struct{})
	go func() {
		defer close(readyDone)
		for _, bound := range listeners {
			select {
			case <-runCtx.Done():
				return
			case <-bound.listener.ready:
			}
		}
		if runCtx.Err() == nil && s.onHTTPReady != nil {
			s.onHTTPReady()
		}
	}()
	errorsByListener := make([]error, 0, len(listeners))
	for remaining := len(listeners); remaining > 0; remaining-- {
		err := <-results
		if remaining == len(listeners) {
			cancel()
		}
		if err != nil {
			errorsByListener = append(errorsByListener, err)
		}
	}
	<-readyDone
	return errors.Join(errorsByListener...)
}
