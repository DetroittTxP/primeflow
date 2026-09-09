package server

import (
	iofs "io/fs"
	"net/http"
	"path"
	"regexp"
	"strings"
	"testing"
)

var (
	// href="..." / src="..." in the pre-auth markup.
	htmlRefPat = regexp.MustCompile(`(?:href|src)="([^"]+)"`)
	// `import { x } from './a.js';` and the bare `import './a.js';`.
	jsImportPat = regexp.MustCompile(`(?m)^\s*import\s+(?:[^'"]*\bfrom\s+)?['"]([^'"]+)['"]`)
)

// The sign-in and password-reset pages are drawn before a session exists, so
// every file they pull has to be reachable without one. When the console was
// split into ES modules their styles and script moved out of the markup into
// /css and /js while the allowlist kept its pre-split contents, and the gate
// answered the stylesheet request with a 303 to the login page: the browser
// asked for CSS, got HTML, and rendered the form unstyled.
//
// The wanted list is therefore not spelled out here -- it is read back out of
// the markup that ships in the binary, followed through the module graph. Add
// a stylesheet, a favicon or an import to either page and this test names the
// file that is about to redirect instead of load.
func TestPreAuthPageAssetsNeedNoSession(t *testing.T) {
	for _, page := range []string{"login.html", "reset.html"} {
		for _, ref := range preAuthRefs(t, page) {
			if !openOperatorPath(http.MethodGet, ref) {
				t.Errorf("%s loads %s, which the auth gate redirects to /login.html; "+
					"add it to openOperatorPath", page, ref)
			}
		}
	}
}

// The allowlist names files, one by one, rather than opening /css and /js: the
// console proper stays behind the gate.
func TestConsoleAssetsStayBehindTheGate(t *testing.T) {
	for _, p := range []string{
		"/index.html", "/css/app.css", "/js/app.js", "/js/api.js",
		"/js/views/runs.js", "/api/v1/runs",
	} {
		if openOperatorPath(http.MethodGet, p) {
			t.Errorf("%s must require a session", p)
		}
	}
}

// preAuthRefs walks the references a page makes, transitively through any
// module it imports, and returns them as the request paths a browser would ask
// for. Off-origin and in-page references are skipped; a reference the embedded
// filesystem has no file for is still returned, because a server route is as
// valid a target as an asset -- it just ends the walk.
func preAuthRefs(t *testing.T, page string) []string {
	t.Helper()
	ui, err := fsSub(uiFS, "ui")
	if err != nil {
		t.Fatalf("ui assets missing: %v", err)
	}

	var (
		out  []string
		seen = map[string]bool{}
		walk func(dir string, body []byte, pat *regexp.Regexp)
	)
	walk = func(dir string, body []byte, pat *regexp.Regexp) {
		for _, m := range pat.FindAllSubmatch(body, -1) {
			ref := string(m[1])
			if i := strings.IndexAny(ref, "?#"); i >= 0 {
				ref = ref[:i]
			}
			if ref == "" || strings.Contains(ref, "://") || strings.Contains(ref, ":") {
				continue // another origin, or a data:/mailto: URL
			}
			urlPath := ref
			if !strings.HasPrefix(ref, "/") {
				urlPath = path.Join(dir, ref)
			}
			if seen[urlPath] {
				continue
			}
			seen[urlPath] = true
			out = append(out, urlPath)

			if !strings.HasSuffix(urlPath, ".js") {
				continue
			}
			mod, err := iofs.ReadFile(ui, strings.TrimPrefix(urlPath, "/"))
			if err != nil {
				continue // a route, not a file in the binary
			}
			walk(path.Dir(urlPath), mod, jsImportPat)
		}
	}

	body, err := iofs.ReadFile(ui, page)
	if err != nil {
		t.Fatalf("read %s: %v", page, err)
	}
	walk("/", body, htmlRefPat)
	if len(out) == 0 {
		t.Fatalf("%s references nothing -- the scan is broken, not the allowlist", page)
	}
	return out
}
