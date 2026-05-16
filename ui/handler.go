package ui

import (
	"embed"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sort"
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
	EnableTask(name string) error
	DisableTask(name string) error
	RunTaskNow(name string) error
	RemoveTask(name string) error
}

// Task is the web-facing task snapshot.
type Task struct {
	Name         string
	Schedule     string
	Enabled      bool
	Running      bool
	RunningCount int
	LastRun      time.Time
	NextRun      time.Time
	RunCount     int64
	ErrorCount   int64
	LastError    string
}

// Stats is the web-facing aggregate task manager snapshot.
type Stats struct {
	TotalTasks       int
	EnabledTasks     int
	RunningTasks     int
	TotalRuns        int64
	TotalErrors      int64
	MaxConcurrent    int
	AllowOverlapping bool
}

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
	mux.HandleFunc(route(h.baseURL, "/"), h.handleIndex("tasks"))
	mux.HandleFunc(route(h.baseURL, "/stats"), h.handleIndex("stats"))
	mux.HandleFunc(route(h.baseURL, "/action"), h.handleTaskAction())
	mux.HandleFunc(route(h.baseURL, "/api"), h.handleAPI())
	mux.HandleFunc(route(h.baseURL, "/assets/styles.css"), serveStyles)

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
	if path == "" {
		path = "/"
	}
	if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}
	if baseURL == "" {
		return path
	}
	if path == "/" {
		return baseURL + "/"
	}
	return baseURL + path
}

func serveStyles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	styles, err := webUI.ReadFile("styles.css")
	if err != nil {
		http.Error(w, "Stylesheet unavailable", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/css; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=3600")
	if r.Method == http.MethodHead {
		return
	}
	_, _ = w.Write(styles)
}

func (h *handler) handleIndex(initialTab string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if initialTab == "tasks" && r.URL.Path != route(h.baseURL, "/") {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		renderPage(w, h.pageData(initialTab))
	}
}

func (h *handler) handleAPI() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		tasks := h.manager.ListTasks()
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].Name < tasks[j].Name })

		apiTasks := make([]*apiTask, len(tasks))
		for i, t := range tasks {
			apiTasks[i] = &apiTask{
				Name:         t.Name,
				Schedule:     t.Schedule,
				Enabled:      t.Enabled,
				Running:      t.Running,
				RunningCount: t.RunningCount,
				LastRun:      t.LastRun,
				NextRun:      t.NextRun,
				RunCount:     t.RunCount,
				ErrorCount:   t.ErrorCount,
				LastError:    t.LastError,
			}
		}

		stats := h.manager.Stats()
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, apiResponse{
			Tasks: apiTasks,
			Stats: apiStats{
				TotalTasks:       stats.TotalTasks,
				EnabledTasks:     stats.EnabledTasks,
				RunningTasks:     stats.RunningTasks,
				TotalRuns:        stats.TotalRuns,
				TotalErrors:      stats.TotalErrors,
				MaxConcurrent:    stats.MaxConcurrent,
				AllowOverlapping: stats.AllowOverlapping,
			},
		})
	}
}

func (h *handler) handleTaskAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSON(w, actionResult{OK: false, Error: fmt.Sprintf("failed to parse form: %v", err)})
			return
		}

		name := r.FormValue("name")
		action := r.FormValue("action")

		var err error
		switch {
		case name == "" || action == "":
			err = fmt.Errorf("missing name or action")
		case action == "enable":
			err = h.manager.EnableTask(name)
		case action == "disable":
			err = h.manager.DisableTask(name)
		case action == "run":
			err = h.manager.RunTaskNow(name)
		case action == "remove":
			err = h.manager.RemoveTask(name)
		default:
			err = fmt.Errorf("invalid action %q", action)
		}

		if err != nil {
			writeJSON(w, actionResult{OK: false, Error: err.Error()})
			return
		}
		writeJSON(w, actionResult{OK: true})
	}
}

func renderPage(w http.ResponseWriter, data pageData) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := webTemplates.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type pageData struct {
	StylesURL  string
	ActionURL  string
	APIURL     string
	InitialTab string
}

type apiTask struct {
	Name         string    `json:"name"`
	Schedule     string    `json:"schedule"`
	Enabled      bool      `json:"enabled"`
	Running      bool      `json:"running"`
	RunningCount int       `json:"running_count"`
	LastRun      time.Time `json:"last_run"`
	NextRun      time.Time `json:"next_run"`
	RunCount     int64     `json:"run_count"`
	ErrorCount   int64     `json:"error_count"`
	LastError    string    `json:"last_error"`
}

type apiStats struct {
	TotalTasks       int   `json:"total_tasks"`
	EnabledTasks     int   `json:"enabled_tasks"`
	RunningTasks     int   `json:"running_tasks"`
	TotalRuns        int64 `json:"total_runs"`
	TotalErrors      int64 `json:"total_errors"`
	MaxConcurrent    int   `json:"max_concurrent"`
	AllowOverlapping bool  `json:"allow_overlapping"`
}

type apiResponse struct {
	Tasks []*apiTask `json:"tasks"`
	Stats apiStats   `json:"stats"`
}

type actionResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func (h *handler) pageData(initialTab string) pageData {
	return pageData{
		StylesURL:  route(h.baseURL, "/assets/styles.css"),
		ActionURL:  route(h.baseURL, "/action"),
		APIURL:     route(h.baseURL, "/api"),
		InitialTab: initialTab,
	}
}
