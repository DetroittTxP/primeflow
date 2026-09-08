package server

import (
	"bytes"
	"embed"
	"fmt"
	iofs "io/fs"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/primex/primeflow/internal/core"
	"github.com/primex/primeflow/internal/scheduler"
)

//go:embed ui/*
var uiFS embed.FS

// uiHandler serves the bundled operator console.
//
// The console is a single static file, so the assets are served directly rather
// than through http.FileServer: its canonical redirect between "/" and
// "/index.html" would otherwise bounce the browser in a loop.
func (s *Server) uiHandler() http.Handler {
	sub, err := fsSub(uiFS, "ui")
	if err != nil {
		s.log.Error("ui assets missing", "err", err)
		return http.NotFoundHandler()
	}
	modTime := time.Now()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		if name == "" || name == "index.html" {
			name = "index.html"
		}
		data, err := iofs.ReadFile(sub, name)
		if err != nil {
			// The console keeps its current view in the path, so a path with
			// no file extension is a console route rather than a missing
			// asset: hand back the app and let it route on the client. This
			// handler is the mux's catch-all, so the server's own routes stay
			// a 404 when they are unmatched or switched off.
			if path.Ext(name) != "" || strings.HasPrefix(name, "api/") || name == "metrics" {
				http.NotFound(w, r)
				return
			}
			name = "index.html"
			if data, err = iofs.ReadFile(sub, name); err != nil {
				http.NotFound(w, r)
				return
			}
		}
		w.Header().Set("Cache-Control", "no-cache")
		http.ServeContent(w, r, name, modTime, bytes.NewReader(data))
	})
}

// validateSchedule rejects a schedule the scheduler could never act on, at the
// moment it is submitted rather than silently every cycle afterwards.
func validateSchedule(d core.Deployment) ([]time.Time, error) {
	now := time.Now().UTC()
	times, err := scheduler.NextRuns(d, now, now.Add(365*24*time.Hour), 1)
	if err != nil {
		return nil, fmt.Errorf("invalid schedule: %w", err)
	}
	if len(times) == 0 {
		return nil, fmt.Errorf("schedule %q never fires", d.Schedule)
	}
	return times, nil
}

// fsSub narrows the embedded filesystem to the ui directory.
func fsSub(f embed.FS, dir string) (fsys iofs.FS, err error) {
	return iofs.Sub(f, dir)
}
