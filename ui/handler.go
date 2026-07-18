// Package ui provides the embedded web interface for the mita task manager.
//
// The interface is a single page backed by a small JSON API:
//
//	GET  {base}/                          the dashboard page
//	GET  {base}/assets/styles.css         embedded stylesheet
//	GET  {base}/api/state?window=SECONDS  full state snapshot (stats, tasks, upcoming runs, events)
//	GET  {base}/api/schedule/preview      validate an expression: ?expr=...
//	POST {base}/api/tasks/{name}/{action} run | pause | resume | remove | schedule
//
// The schedule action takes a JSON body {"expr": "..."}. Errors are reported
// with meaningful HTTP status codes (404 unknown task, 409 overlap/concurrency
// conflicts, 400 invalid input) and a JSON body {"error": "..."}.
package ui

import (
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"strconv"
	"strings"
	"time"
)

//go:embed index.html styles.css
var webUI embed.FS

var webTemplates = template.Must(template.New("web").ParseFS(webUI, "index.html"))

// Manager provides the task operations needed by the web UI.
type Manager interface {
	ListTasks() []Task
	Stats() Stats
	RecentEvents(limit int) []Event
	UpcomingRuns(within time.Duration, perTaskLimit int) map[string][]time.Time
	EnableTask(name string) error
	DisableTask(name string) error
	RunTaskNow(name string) error
	RemoveTask(name string) error
	UpdateSchedule(name, expr string) error
	PreviewSchedule(expr string, n int) (normalized string, fires []time.Time, err error)
}

// Task is the web-facing task snapshot.
type Task struct {
	Name             string
	Schedule         string
	Enabled          bool
	Running          bool
	RunningCount     int
	AddedAt          time.Time
	LastRun          time.Time
	NextRun          time.Time
	RunCount         int64
	ErrorCount       int64
	SkipCount        int64
	LastError        string
	Timeout          time.Duration
	AllowOverlapping bool
}

// Stats is the web-facing aggregate task manager snapshot.
type Stats struct {
	TotalTasks       int
	EnabledTasks     int
	RunningTasks     int
	TotalRuns        int64
	TotalErrors      int64
	TotalSkips       int64
	MaxConcurrent    int
	AllowOverlapping bool
	StartedAt        time.Time
}

// Event is one web-facing entry of the manager's recent-event buffer.
type Event struct {
	At       time.Time
	Kind     string // completed | failed | skipped | lifecycle
	TaskName string
	Message  string
	Duration time.Duration
}

// StatusError carries an HTTP status code alongside a manager error.
// The mita adapter wraps errors with it so handlers can answer with
// meaningful status codes without this package importing mita.
type StatusError struct {
	Code int
	Err  error
}

func (e *StatusError) Error() string { return e.Err.Error() }
func (e *StatusError) Unwrap() error { return e.Err }

const (
	defaultWindow   = 5 * time.Minute
	maxWindow       = time.Hour
	perTaskUpcoming = 120
	eventLimit      = 100
)

type handler struct {
	baseURL string
	manager Manager
}

// NewHandler creates an HTTP handler for the task manager web interface.
func NewHandler(baseURL string, manager Manager) *http.ServeMux {
	h := &handler{
		baseURL: normalizeBaseURL(baseURL),
		manager: manager,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET "+route(h.baseURL, "/{$}"), h.handleIndex)
	mux.HandleFunc("GET "+route(h.baseURL, "/assets/styles.css"), serveStyles)
	mux.HandleFunc("GET "+route(h.baseURL, "/api/state"), h.handleState)
	mux.HandleFunc("GET "+route(h.baseURL, "/api/schedule/preview"), h.handlePreview)
	mux.HandleFunc("POST "+route(h.baseURL, "/api/tasks/{name}/{action}"), h.handleTaskAction)

	return mux
}

func normalizeBaseURL(baseURL string) string {
	baseURL = strings.TrimSpace(baseURL)
	baseURL = strings.TrimSuffix(baseURL, "/")
	if baseURL == "" || baseURL == "/" {
		return ""
	}
	if !strings.HasPrefix(baseURL, "/") {
		return "/" + baseURL
	}
	return baseURL
}

func route(baseURL, path string) string {
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	return baseURL + path
}

func serveStyles(w http.ResponseWriter, r *http.Request) {
	styles, err := webUI.ReadFile("styles.css")
	if err != nil {
		http.Error(w, "Stylesheet unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	_, _ = w.Write(styles)
}

func (h *handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	data := pageData{
		StylesURL: route(h.baseURL, "/assets/styles.css"),
		APIBase:   route(h.baseURL, "/api"),
	}
	if err := webTemplates.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *handler) handleState(w http.ResponseWriter, r *http.Request) {
	window := defaultWindow
	if v := r.URL.Query().Get("window"); v != "" {
		if secs, err := strconv.Atoi(v); err == nil {
			window = time.Duration(secs) * time.Second
		}
	}
	if window < time.Minute {
		window = time.Minute
	}
	if window > maxWindow {
		window = maxWindow
	}

	tasks := h.manager.ListTasks()
	upcoming := h.manager.UpcomingRuns(window, perTaskUpcoming)
	stats := h.manager.Stats()
	events := h.manager.RecentEvents(eventLimit)

	apiTasks := make([]apiTask, len(tasks))
	for i, t := range tasks {
		apiTasks[i] = apiTask{
			Name:             t.Name,
			Schedule:         t.Schedule,
			Meaning:          humanizeExpr(t.Schedule),
			Enabled:          t.Enabled,
			Running:          t.Running,
			RunningCount:     t.RunningCount,
			AddedAt:          timePtr(t.AddedAt),
			LastRun:          timePtr(t.LastRun),
			NextRun:          timePtr(t.NextRun),
			RunCount:         t.RunCount,
			ErrorCount:       t.ErrorCount,
			SkipCount:        t.SkipCount,
			LastError:        t.LastError,
			TimeoutMs:        t.Timeout.Milliseconds(),
			AllowOverlapping: t.AllowOverlapping,
			Upcoming:         upcoming[t.Name],
		}
	}

	apiEvents := make([]apiEvent, len(events))
	for i, ev := range events {
		apiEvents[i] = apiEvent{
			At:         ev.At,
			Kind:       ev.Kind,
			Task:       ev.TaskName,
			Message:    ev.Message,
			DurationMs: ev.Duration.Milliseconds(),
		}
	}

	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, http.StatusOK, apiState{
		Now:           time.Now(),
		WindowSeconds: int(window / time.Second),
		Stats: apiStats{
			TotalTasks:       stats.TotalTasks,
			EnabledTasks:     stats.EnabledTasks,
			RunningTasks:     stats.RunningTasks,
			TotalRuns:        stats.TotalRuns,
			TotalErrors:      stats.TotalErrors,
			TotalSkips:       stats.TotalSkips,
			MaxConcurrent:    stats.MaxConcurrent,
			AllowOverlapping: stats.AllowOverlapping,
			StartedAt:        timePtr(stats.StartedAt),
		},
		Tasks:  apiTasks,
		Events: apiEvents,
	})
}

func (h *handler) handlePreview(w http.ResponseWriter, r *http.Request) {
	expr := strings.TrimSpace(r.URL.Query().Get("expr"))
	if expr == "" {
		writeJSON(w, http.StatusBadRequest, previewResult{OK: false, Error: "expression is empty"})
		return
	}
	normalized, fires, err := h.manager.PreviewSchedule(expr, 3)
	if err != nil {
		writeJSON(w, statusFor(err, http.StatusBadRequest), previewResult{OK: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, previewResult{
		OK:      true,
		Expr:    normalized,
		Meaning: humanizeExpr(normalized),
		Next:    fires,
	})
}

func (h *handler) handleTaskAction(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	action := r.PathValue("action")

	var err error
	switch action {
	case "run":
		err = h.manager.RunTaskNow(name)
	case "pause":
		err = h.manager.DisableTask(name)
	case "resume":
		err = h.manager.EnableTask(name)
	case "remove":
		err = h.manager.RemoveTask(name)
	case "schedule":
		var body struct {
			Expr string `json:"expr"`
		}
		if decodeErr := json.NewDecoder(r.Body).Decode(&body); decodeErr != nil || strings.TrimSpace(body.Expr) == "" {
			writeJSON(w, http.StatusBadRequest, actionResult{OK: false, Error: "request body must be JSON with a non-empty \"expr\""})
			return
		}
		err = h.manager.UpdateSchedule(name, strings.TrimSpace(body.Expr))
	default:
		writeJSON(w, http.StatusNotFound, actionResult{OK: false, Error: fmt.Sprintf("unknown action %q", action)})
		return
	}

	if err != nil {
		writeJSON(w, statusFor(err, http.StatusInternalServerError), actionResult{OK: false, Error: err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, actionResult{OK: true})
}

// statusFor extracts the HTTP status from a StatusError, or returns fallback.
func statusFor(err error, fallback int) int {
	var se *StatusError
	if errors.As(err, &se) && se.Code != 0 {
		return se.Code
	}
	return fallback
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// timePtr maps zero times to nil so they serialize as null instead of year 1.
func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

type pageData struct {
	StylesURL string
	APIBase   string
}

type apiState struct {
	Now           time.Time  `json:"now"`
	WindowSeconds int        `json:"window_seconds"`
	Stats         apiStats   `json:"stats"`
	Tasks         []apiTask  `json:"tasks"`
	Events        []apiEvent `json:"events"`
}

type apiTask struct {
	Name             string      `json:"name"`
	Schedule         string      `json:"schedule"`
	Meaning          string      `json:"meaning"`
	Enabled          bool        `json:"enabled"`
	Running          bool        `json:"running"`
	RunningCount     int         `json:"running_count"`
	AddedAt          *time.Time  `json:"added_at"`
	LastRun          *time.Time  `json:"last_run"`
	NextRun          *time.Time  `json:"next_run"`
	RunCount         int64       `json:"run_count"`
	ErrorCount       int64       `json:"error_count"`
	SkipCount        int64       `json:"skip_count"`
	LastError        string      `json:"last_error"`
	TimeoutMs        int64       `json:"timeout_ms"`
	AllowOverlapping bool        `json:"allow_overlapping"`
	Upcoming         []time.Time `json:"upcoming"`
}

type apiStats struct {
	TotalTasks       int        `json:"total_tasks"`
	EnabledTasks     int        `json:"enabled_tasks"`
	RunningTasks     int        `json:"running_tasks"`
	TotalRuns        int64      `json:"total_runs"`
	TotalErrors      int64      `json:"total_errors"`
	TotalSkips       int64      `json:"total_skips"`
	MaxConcurrent    int        `json:"max_concurrent"`
	AllowOverlapping bool       `json:"allow_overlapping"`
	StartedAt        *time.Time `json:"started_at"`
}

type apiEvent struct {
	At         time.Time `json:"at"`
	Kind       string    `json:"kind"`
	Task       string    `json:"task"`
	Message    string    `json:"message"`
	DurationMs int64     `json:"duration_ms"`
}

type previewResult struct {
	OK      bool        `json:"ok"`
	Expr    string      `json:"expr,omitempty"`
	Meaning string      `json:"meaning,omitempty"`
	Next    []time.Time `json:"next,omitempty"`
	Error   string      `json:"error,omitempty"`
}

type actionResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}
