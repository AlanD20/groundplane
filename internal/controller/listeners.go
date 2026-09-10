package controller

import (
	"context"
	"errors"
	"net"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Serve binds every configured address before accepting requests. Readiness
// requires every HTTP server to have entered Accept; a partial binding failure
// closes all opened listeners without ever qualifying a native upgrade.
func (s *Server) Serve(ctx context.Context, addresses []string) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "controller HTTP context is required")
	}
	listeners := make([]*readyListener, 0, len(addresses))
	for _, address := range addresses {
		listener, err := net.Listen("tcp", address)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close() // Undo this call's partial binding only.
			}
			return errs.Wrap(errs.KindInternal, err)
		}
		listeners = append(listeners, &readyListener{Listener: listener, ready: make(chan struct{})})
	}
	if len(listeners) == 0 {
		return errs.New(errs.KindValidationFailed, "controller HTTP listeners are required")
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	results := make(chan error, len(listeners))
	for _, listener := range listeners {
		go func() { results <- s.serveListener(runCtx, listener, productionHTTPPolicy()) }()
	}
	readyDone := make(chan struct{})
	go func() {
		defer close(readyDone)
		for _, listener := range listeners {
			select {
			case <-runCtx.Done():
				return
			case <-listener.ready:
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
