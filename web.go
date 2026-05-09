package mita

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

//go:embed ui/index.html ui/styles.css
var webUI embed.FS

var webTemplates = template.Must(template.New("web").ParseFS(webUI, "ui/index.html"))

// WebHandler creates an HTTP handler for the task manager web interface.
// The baseURL parameter should be the URL prefix where the handler is mounted (e.g., "/tasks").
// Returns a ServeMux that can be integrated into your HTTP server.
func (tm *TaskManager) WebHandler(baseURL string) *http.ServeMux {
	baseURL = normalizeWebBaseURL(baseURL)

	mux := http.NewServeMux()
	mux.HandleFunc(webRoute(baseURL, "/"), tm.handleIndex(baseURL))
	mux.HandleFunc(webRoute(baseURL, "/stats"), tm.handleStats(baseURL))
	mux.HandleFunc(webRoute(baseURL, "/action"), tm.handleTaskAction())
	mux.HandleFunc(webRoute(baseURL, "/api"), tm.handleAPI())
	mux.HandleFunc(webRoute(baseURL, "/assets/styles.css"), serveWebStyles)

	return mux
}

func normalizeWebBaseURL(baseURL string) string {
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

func webRoute(baseURL, path string) string {
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

func serveWebStyles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	styles, err := webUI.ReadFile("ui/styles.css")
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

func (tm *TaskManager) handleIndex(baseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != webRoute(baseURL, "/") {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		renderWebPage(w, tm.webPageData(baseURL, "tasks"))
	}
}

func (tm *TaskManager) handleStats(baseURL string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		renderWebPage(w, tm.webPageData(baseURL, "stats"))
	}
}

func (tm *TaskManager) handleAPI() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		tasks := tm.ListTasks()
		sort.Slice(tasks, func(i, j int) bool { return tasks[i].Name < tasks[j].Name })

		apiTasks := make([]*webAPITask, len(tasks))
		for i, t := range tasks {
			apiTasks[i] = &webAPITask{
				Name:       t.Name,
				Schedule:   t.Schedule,
				Enabled:    t.Enabled,
				Running:    t.Running,
				LastRun:    t.LastRun,
				NextRun:    t.NextRun,
				RunCount:   t.RunCount,
				ErrorCount: t.ErrorCount,
				LastError:  t.LastError,
			}
		}

		raw := tm.GetStats()
		w.Header().Set("Cache-Control", "no-store")
		writeJSON(w, webAPIResponse{
			Tasks: apiTasks,
			Stats: webAPIStats{
				TotalTasks:       intStat(raw["total_tasks"]),
				EnabledTasks:     intStat(raw["enabled_tasks"]),
				RunningTasks:     intStat(raw["running_tasks"]),
				TotalRuns:        int64Stat(raw["total_runs"]),
				TotalErrors:      int64Stat(raw["total_errors"]),
				MaxConcurrent:    intStat(raw["max_concurrent"]),
				AllowOverlapping: boolStat(raw["allow_overlapping"]),
			},
		})
	}
}

func (tm *TaskManager) handleTaskAction() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if err := r.ParseForm(); err != nil {
			writeJSON(w, webActionResult{OK: false, Error: fmt.Sprintf("failed to parse form: %v", err)})
			return
		}

		name := r.FormValue("name")
		action := r.FormValue("action")

		var err error
		switch {
		case name == "" || action == "":
			err = fmt.Errorf("missing name or action")
		case action == "enable":
			err = tm.EnableTask(name)
		case action == "disable":
			err = tm.DisableTask(name)
		case action == "run":
			err = tm.RunTaskNow(name)
		case action == "remove":
			err = tm.RemoveTask(name)
		default:
			err = fmt.Errorf("invalid action %q", action)
		}

		if err != nil {
			writeJSON(w, webActionResult{OK: false, Error: err.Error()})
			return
		}
		writeJSON(w, webActionResult{OK: true})
	}
}

func renderWebPage(w http.ResponseWriter, data webPage) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := webTemplates.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

type webPage struct {
	StylesURL  string
	ActionURL  string
	APIURL     string
	InitialTab string
}

type webAPITask struct {
	Name       string    `json:"name"`
	Schedule   string    `json:"schedule"`
	Enabled    bool      `json:"enabled"`
	Running    bool      `json:"running"`
	LastRun    time.Time `json:"last_run"`
	NextRun    time.Time `json:"next_run"`
	RunCount   int64     `json:"run_count"`
	ErrorCount int64     `json:"error_count"`
	LastError  string    `json:"last_error"`
}

type webAPIStats struct {
	TotalTasks       int   `json:"total_tasks"`
	EnabledTasks     int   `json:"enabled_tasks"`
	RunningTasks     int   `json:"running_tasks"`
	TotalRuns        int64 `json:"total_runs"`
	TotalErrors      int64 `json:"total_errors"`
	MaxConcurrent    int   `json:"max_concurrent"`
	AllowOverlapping bool  `json:"allow_overlapping"`
}

type webAPIResponse struct {
	Tasks []*webAPITask `json:"tasks"`
	Stats webAPIStats   `json:"stats"`
}

type webActionResult struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

func (tm *TaskManager) webPageData(baseURL, initialTab string) webPage {
	return webPage{
		StylesURL:  webRoute(baseURL, "/assets/styles.css"),
		ActionURL:  webRoute(baseURL, "/action"),
		APIURL:     webRoute(baseURL, "/api"),
		InitialTab: initialTab,
	}
}

func intStat(value interface{}) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	default:
		return 0
	}
}

func int64Stat(value interface{}) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

func boolStat(value interface{}) bool {
	v, _ := value.(bool)
	return v
}
