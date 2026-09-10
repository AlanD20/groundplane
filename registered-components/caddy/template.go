package caddy

import (
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/AlanD20/groundplane-component-sdk/component"
)

const (
	defaultTemplateBody = "{gp.routes}\n"
	routesMarker        = "{gp.routes}"
	maxTemplateBytes    = 32 << 10
)

type routeMatch struct {
	host string
	path string
}

type templateRenderer struct {
	routes    map[routeMatch]component.HTTPRoute
	used      map[routeMatch]bool
	aggregate string
}

func renderCaddyfile(templateBody string, routes []component.HTTPRoute) ([]byte, error) {
	// ADR0075 defines empty as the all-Route default, not an empty serving file.
	if templateBody == "" {
		templateBody = defaultTemplateBody
	}
	if len(templateBody) > maxTemplateBytes || !utf8.ValidString(templateBody) ||
		strings.IndexByte(templateBody, 0) >= 0 || strings.Contains(templateBody, "{routes}") ||
		strings.Count(templateBody, routesMarker) > 1 {
		return nil, fmt.Errorf("caddy: invalid full template or retired aggregate marker")
	}
	renderer := templateRenderer{
		routes: make(map[routeMatch]component.HTTPRoute, len(routes)),
		used:   make(map[routeMatch]bool, len(routes)), aggregate: renderRouteBlocks(routes),
	}
	for _, route := range routes {
		renderer.routes[routeMatch{route.Host, route.Path}] = route
	}
	rendered, err := renderer.render(templateBody)
	if err != nil {
		return nil, err
	}
	if len(renderer.used) != len(renderer.routes) {
		return nil, fmt.Errorf("caddy: full template must reference every Route upstream or use {gp.routes}")
	}
	if !strings.HasSuffix(rendered, "\n") {
		rendered += "\n"
	}
	return []byte(rendered), nil
}

func (renderer *templateRenderer) render(body string) (string, error) {
	var output strings.Builder
	for {
		start := strings.Index(body, "{gp.")
		if start < 0 {
			output.WriteString(body)
			return output.String(), nil
		}
		output.WriteString(body[:start])
		body = body[start:]
		end := strings.IndexByte(body, '}')
		if end < 0 {
			return "", fmt.Errorf("caddy: unterminated Groundplane template reference")
		}
		value, err := renderer.substitute(body[1:end])
		if err != nil {
			return "", err
		}
		output.WriteString(value)
		body = body[end+1:]
	}
}

func (renderer *templateRenderer) substitute(reference string) (string, error) {
	if reference == "gp.routes" {
		for match := range renderer.routes {
			renderer.used[match] = true
		}
		return renderer.aggregate, nil
	}
	selector, found := strings.CutPrefix(reference, "gp.route:")
	if !found {
		return "", fmt.Errorf("caddy: unknown Groundplane template reference")
	}
	host, tail, found := strings.Cut(selector, ":")
	separator := strings.LastIndexByte(tail, ':')
	if !found || separator < 1 || separator == len(tail)-1 {
		return "", fmt.Errorf("caddy: Route template reference requires host, path and field")
	}
	match := routeMatch{host: host, path: tail[:separator]}
	route, found := renderer.routes[match]
	if !found {
		return "", fmt.Errorf("caddy: template references an undeclared Route")
	}
	switch tail[separator+1:] {
	case "host":
		return route.Host, nil
	case "path":
		if route.Path == "/" {
			return "/*", nil
		}
		return route.Path, nil
	case "upstream":
		renderer.used[match] = true
		return route.BackendServiceName + ":" + strconv.Itoa(int(route.TargetPort)), nil
	default:
		return "", fmt.Errorf("caddy: unknown Route template field")
	}
}
