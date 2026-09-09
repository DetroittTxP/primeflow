package server

import (
	iofs "io/fs"
	"net/http"
	"net/http/httptest"
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

// A token in the query string authenticates the SSE stream and nothing else.
// The stream is the one route a browser EventSource cannot set a header on;
// everywhere else a query-string credential leaks into access logs, proxy logs,
// Referer headers and history while granting a CSRF-exempt admin principal.
func TestTokenQueryParamIsScopedToTheStream(t *testing.T) {
	s := &Server{cfg: Config{APIToken: "s3cret"}}
	for _, tc := range []struct {
		method, target string
		want           bool
	}{
		{http.MethodGet, streamPath + "?token=s3cret", true},
		{http.MethodGet, "/api/v1/users?token=s3cret", false},
		{http.MethodDelete, "/api/v1/users/u1?token=s3cret", false},
		{http.MethodPost, streamPath + "?token=s3cret", false},
		{http.MethodGet, streamPath + "?token=wrong", false},
	} {
		p, _ := s.resolvePrincipal(httptest.NewRequest(tc.method, tc.target, nil))
		if got := p != nil && p.Machine; got != tc.want {
			t.Errorf("%s %s: machine principal = %v, want %v", tc.method, tc.target, got, tc.want)
		}
	}

	// The Authorization header is unaffected and still works on any route.
	r := httptest.NewRequest(http.MethodDelete, "/api/v1/users/u1", nil)
	r.Header.Set("Authorization", "Bearer s3cret")
	if p, _ := s.resolvePrincipal(r); p == nil || !p.Machine {
		t.Error("the bearer header should authenticate on any route")
	}
}

// Without a cap every endpoint, the unauthenticated ones included, buffers
// whatever a client chooses to send.
func TestDecodeRejectsAnOversizedBody(t *testing.T) {
	body := `{"x":"` + strings.Repeat("a", maxBodyBytes) + `"}`
	r := httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(body))
	var v struct {
		X string `json:"x"`
	}
	if err := decode(r, &v); err == nil {
		t.Fatal("expected a body over the limit to be rejected")
	}

	// A normal body still decodes.
	r = httptest.NewRequest(http.MethodPost, "/api/v1/runs", strings.NewReader(`{"x":"ok"}`))
	if err := decode(r, &v); err != nil || v.X != "ok" {
		t.Fatalf("small body: err=%v x=%q", err, v.X)
	}
}
