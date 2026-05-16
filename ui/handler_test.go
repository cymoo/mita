package ui

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

type fakeManager struct {
	tasks   []Task
	stats   Stats
	actions []string
	err     error
}

func (m *fakeManager) ListTasks() []Task {
	return m.tasks
}

func (m *fakeManager) Stats() Stats {
	return m.stats
}

func (m *fakeManager) EnableTask(name string) error {
	m.actions = append(m.actions, "enable:"+name)
	return m.err
}

func (m *fakeManager) DisableTask(name string) error {
	m.actions = append(m.actions, "disable:"+name)
	return m.err
}

func (m *fakeManager) RunTaskNow(name string) error {
	m.actions = append(m.actions, "run:"+name)
	return m.err
}

func (m *fakeManager) RemoveTask(name string) error {
	m.actions = append(m.actions, "remove:"+name)
	return m.err
}

func TestNewHandlerRoutesAndAssets(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		indexPath string
		statsPath string
		apiPath   string
		cssPath   string
	}{
		{
			name:      "root",
			baseURL:   "",
			indexPath: "/",
			statsPath: "/stats",
			apiPath:   "/api",
			cssPath:   "/assets/styles.css",
		},
		{
			name:      "mounted with missing slash and trailing slash",
			baseURL:   "tasks/",
			indexPath: "/tasks/",
			statsPath: "/tasks/stats",
			apiPath:   "/tasks/api",
			cssPath:   "/tasks/assets/styles.css",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &fakeManager{
				tasks: []Task{{Name: "web-test", Schedule: "0 * * * * *", Enabled: true}},
				stats: Stats{TotalTasks: 1, EnabledTasks: 1},
			}
			handler := NewHandler(tt.baseURL, manager)

			for _, path := range []string{tt.indexPath, tt.statsPath} {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("%s status = %d, want %d", path, rec.Code, http.StatusOK)
				}
				if body := rec.Body.String(); !strings.Contains(body, "mita") {
					t.Fatalf("%s body missing expected content", path)
				}
			}

			apiReq := httptest.NewRequest(http.MethodGet, tt.apiPath, nil)
			apiRec := httptest.NewRecorder()
			handler.ServeHTTP(apiRec, apiReq)
			if apiRec.Code != http.StatusOK {
				t.Fatalf("api status = %d, want %d", apiRec.Code, http.StatusOK)
			}

			cssReq := httptest.NewRequest(http.MethodGet, tt.cssPath, nil)
			cssRec := httptest.NewRecorder()
			handler.ServeHTTP(cssRec, cssReq)
			if cssRec.Code != http.StatusOK {
				t.Fatalf("css status = %d, want %d", cssRec.Code, http.StatusOK)
			}
			if ct := cssRec.Header().Get("Content-Type"); !strings.Contains(ct, "text/css") {
				t.Fatalf("css Content-Type = %q, want text/css", ct)
			}
		})
	}
}

func TestHandlerAPIReturnsSortedJSON(t *testing.T) {
	now := time.Now()
	manager := &fakeManager{
		tasks: []Task{
			{Name: "z-task", Schedule: "0 * * * * *", Enabled: true},
			{Name: "a-task", Schedule: "*/5 * * * * *", Enabled: true, Running: true, RunningCount: 2, LastRun: now, RunCount: 3, ErrorCount: 1, LastError: "boom"},
		},
		stats: Stats{TotalTasks: 2, EnabledTasks: 2, RunningTasks: 1, TotalRuns: 3, TotalErrors: 1, MaxConcurrent: 4, AllowOverlapping: true},
	}
	handler := NewHandler("/", manager)

	req := httptest.NewRequest(http.MethodGet, "/api", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}

	var resp struct {
		Tasks []struct {
			Name         string `json:"name"`
			RunningCount int    `json:"running_count"`
			ErrorCount   int64  `json:"error_count"`
		} `json:"tasks"`
		Stats struct {
			TotalTasks       int  `json:"total_tasks"`
			AllowOverlapping bool `json:"allow_overlapping"`
		} `json:"stats"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if len(resp.Tasks) != 2 || resp.Tasks[0].Name != "a-task" || resp.Tasks[1].Name != "z-task" {
		t.Fatalf("tasks not sorted by name: %+v", resp.Tasks)
	}
	if resp.Tasks[0].RunningCount != 2 || resp.Tasks[0].ErrorCount != 1 {
		t.Fatalf("unexpected first task payload: %+v", resp.Tasks[0])
	}
	if resp.Stats.TotalTasks != 2 || !resp.Stats.AllowOverlapping {
		t.Fatalf("unexpected stats payload: %+v", resp.Stats)
	}
}

func TestHandlerActions(t *testing.T) {
	tests := []struct {
		name       string
		action     string
		wantAction string
		err        error
		wantOK     bool
	}{
		{name: "enable", action: "enable", wantAction: "enable:task", wantOK: true},
		{name: "disable", action: "disable", wantAction: "disable:task", wantOK: true},
		{name: "run", action: "run", wantAction: "run:task", wantOK: true},
		{name: "remove", action: "remove", wantAction: "remove:task", wantOK: true},
		{name: "manager error", action: "run", wantAction: "run:task", err: errors.New("nope"), wantOK: false},
		{name: "invalid action", action: "bogus", wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			manager := &fakeManager{err: tt.err}
			handler := NewHandler("/tasks", manager)
			form := url.Values{}
			form.Set("name", "task")
			form.Set("action", tt.action)

			req := httptest.NewRequest(http.MethodPost, "/tasks/action", strings.NewReader(form.Encode()))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
			}
			var result actionResult
			if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
				t.Fatalf("failed to decode response: %v", err)
			}
			if result.OK != tt.wantOK {
				t.Fatalf("OK = %v, want %v; error=%q", result.OK, tt.wantOK, result.Error)
			}
			if tt.wantAction != "" && (len(manager.actions) != 1 || manager.actions[0] != tt.wantAction) {
				t.Fatalf("actions = %v, want %s", manager.actions, tt.wantAction)
			}
		})
	}
}

func TestHandlerRejectsInvalidRequests(t *testing.T) {
	handler := NewHandler("", &fakeManager{})

	tests := []struct {
		name   string
		method string
		path   string
		body   string
		want   int
	}{
		{name: "unknown root path", method: http.MethodGet, path: "/not-a-real-path", want: http.StatusNotFound},
		{name: "index invalid method", method: http.MethodPost, path: "/", want: http.StatusMethodNotAllowed},
		{name: "api invalid method", method: http.MethodPost, path: "/api", want: http.StatusMethodNotAllowed},
		{name: "action invalid method", method: http.MethodGet, path: "/action", want: http.StatusMethodNotAllowed},
		{name: "css invalid method", method: http.MethodPost, path: "/assets/styles.css", want: http.StatusMethodNotAllowed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != tt.want {
				t.Fatalf("status = %d, want %d", rec.Code, tt.want)
			}
		})
	}
}

func TestHandlerActionMissingParams(t *testing.T) {
	handler := NewHandler("", &fakeManager{})
	req := httptest.NewRequest(http.MethodPost, "/action", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	var result actionResult
	if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result.OK || result.Error == "" {
		t.Fatalf("result = %+v, want failure with error", result)
	}
}
