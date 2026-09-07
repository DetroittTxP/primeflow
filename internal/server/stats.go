package server

import (
	"net/http"
	"time"
)

// stats powers the console Dashboard: time-bucketed flow-run / task-run / event
// activity over a window (8h, 24h or 7d).
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	window := 24 * time.Hour
	switch r.URL.Query().Get("window") {
	case "8h":
		window = 8 * time.Hour
	case "168h", "7d", "1w":
		window = 7 * 24 * time.Hour
	case "24h", "":
		window = 24 * time.Hour
	default:
		if d, err := time.ParseDuration(r.URL.Query().Get("window")); err == nil && d > 0 && d <= 31*24*time.Hour {
			window = d
		}
	}
	st, err := s.store.Stats(r.Context(), window, 48)
	if err != nil {
		// A missing date_bin (Postgres < 14) or a transient error still yields a
		// usable, if empty, payload rather than a broken dashboard.
		s.log.Warn("stats query failed", "err", err)
	}
	writeJSON(w, http.StatusOK, st)
}
