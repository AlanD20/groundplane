package core

import "testing"

func TestRouteValidateRejectsCaddySyntaxInjection(t *testing.T) {
	t.Parallel()
	invalid := []string{
		"/ok\n}\nrespond 200\n",
		"/ok\r\nhandle /*",
		"/ok\x00bad",
		"/two words",
		"/tab\tpath",
		"/{http.request.uri}",
		"/escaped\\ path",
		"/quoted\"path",
		"/single'quote",
		"/bad%2",
		"/bad%xx",
		"/wild*card",
	}
	for _, path := range invalid {
		path := path
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			route := Route{
				Host: "app.example.com", Path: path, Exposure: "public",
				TargetServiceID: "svc_target", TargetPort: 8080,
			}
			if err := route.Validate(); err == nil {
				t.Fatalf("Route.Validate() accepted unsafe path %q", path)
			}
		})
	}
}

func TestRouteValidateAcceptsSafeCaddyPathTokens(t *testing.T) {
	t.Parallel()
	for _, path := range []string{"/", "/api", "/api/*", "/assets/app-v1.2.js", "/encoded/%2Fvalue"} {
		route := Route{
			Host: "app.example.com", Path: path, Exposure: "public",
			TargetServiceID: "svc_target", TargetPort: 8080,
		}
		if err := route.Validate(); err != nil {
			t.Fatalf("Route.Validate(%q) error = %v", path, err)
		}
	}
}

func TestRouteValidateRejectsIPAddressAsDNSHost(t *testing.T) {
	t.Parallel()
	route := Route{
		Host: "127.0.0.1", Path: "/", Exposure: "public",
		TargetServiceID: "svc_target", TargetPort: 8080,
	}
	if err := route.Validate(); err == nil {
		t.Fatal("Route.Validate() accepted an IP literal as a DNS host")
	}
}
