package ui

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type fakeManager struct {
	tasks    []Task
	stats    Stats
	events   []Event
	upcoming map[string][]time.Time
	actions  []string
	err      error
}

func (m *fakeManager) ListTasks() []Task { return m.tasks }
func (m *fakeManager) Stats() Stats      { return m.stats }
func (m *fakeManager) RecentEvents(limit int) []Event {
	if limit > 0 && limit < len(m.events) {
		return m.events[:limit]
	}
	return m.events
}
func (m *fakeManager) UpcomingRuns(within time.Duration, perTaskLimit int) map[string][]time.Time {
	return m.upcoming
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
func (m *fakeManager) UpdateSchedule(name, expr string) error {
	m.actions = append(m.actions, "schedule:"+name+":"+expr)
	return m.err
}
func (m *fakeManager) PreviewSchedule(expr string, n int) (string, []time.Time, error) {
	if m.err != nil {
		return "", nil, m.err
	}
	fires := make([]time.Time, n)
	base := time.Now()
	for i := range fires {
		fires[i] = base.Add(time.Duration(i+1) * time.Minute)
	}
	return expr, fires, nil
}

func newFakeManager() *fakeManager {
	started := time.Now().Add(-2 * time.Hour)
	return &fakeManager{
		tasks: []Task{{
			Name:       "alpha",
			Schedule:   "0 */5 * * * *",
			Enabled:    true,
			RunCount:   12,
			ErrorCount: 1,
			SkipCount:  2,
			AddedAt:    time.Now().Add(-time.Hour),
			Timeout:    30 * time.Second,
		}},
		stats: Stats{
			TotalTasks:   1,
			EnabledTasks: 1,
			TotalRuns:    12,
			TotalErrors:  1,
			TotalSkips:   2,
			StartedAt:    started,
		},
		events: []Event{
			{At: time.Now(), Kind: "completed", TaskName: "alpha", Message: "completed", Duration: 300 * time.Millisecond},
		},
		upcoming: map[string][]time.Time{
			"alpha": {time.Now().Add(time.Minute)},
		},
	}
}

func TestHandlerRoutes(t *testing.T) {
	tests := []struct {
		name    string
		baseURL string
		prefix  string
	}{
		{name: "root", baseURL: "", prefix: ""},
		{name: "mounted", baseURL: "/tasks", prefix: "/tasks"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewHandler(tt.baseURL, newFakeManager())

			get := func(path string) *httptest.ResponseRecorder {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tt.prefix+path, nil))
				return rec
			}

			if rec := get("/"); rec.Code != http.StatusOK {
				t.Fatalf("index status = %d, want 200", rec.Code)
			} else if !strings.Contains(rec.Body.String(), "mita") {
				t.Fatal("index body missing expected content")
			}

			if rec := get("/assets/styles.css"); rec.Code != http.StatusOK {
				t.Fatalf("css status = %d, want 200", rec.Code)
			} else if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/css") {
				t.Fatalf("css Content-Type = %q", ct)
			}

			if rec := get("/api/state"); rec.Code != http.StatusOK {
				t.Fatalf("state status = %d, want 200", rec.Code)
			} else if !strings.Contains(rec.Body.String(), "alpha") {
				t.Fatal("state response missing task name")
			}

			if rec := get("/not-a-real-path"); rec.Code != http.StatusNotFound {
				t.Fatalf("unknown path status = %d, want 404", rec.Code)
			}
		})
	}
}

func TestHandlerStatePayload(t *testing.T) {
	handler := NewHandler("", newFakeManager())

	req := httptest.NewRequest(http.MethodGet, "/api/state?window=900", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if cc := rec.Header().Get("Cache-Control"); cc != "no-store" {
		t.Fatalf("Cache-Control = %q, want no-store", cc)
	}

	var resp struct {
		Now           time.Time `json:"now"`
		WindowSeconds int       `json:"window_seconds"`
		Stats         struct {
			TotalTasks int        `json:"total_tasks"`
			TotalSkips int64      `json:"total_skips"`
			StartedAt  *time.Time `json:"started_at"`
		} `json:"stats"`
		Tasks []struct {
			Name      string      `json:"name"`
			Meaning   string      `json:"meaning"`
			SkipCount int64       `json:"skip_count"`
			TimeoutMs int64       `json:"timeout_ms"`
			LastRun   *time.Time  `json:"last_run"`
			Upcoming  []time.Time `json:"upcoming"`
		} `json:"tasks"`
		Events []struct {
			Kind       string `json:"kind"`
			Task       string `json:"task"`
			DurationMs int64  `json:"duration_ms"`
		} `json:"events"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	if resp.Now.IsZero() {
		t.Fatal("now is zero")
	}
	if resp.WindowSeconds != 900 {
		t.Fatalf("window_seconds = %d, want 900", resp.WindowSeconds)
	}
	if resp.Stats.StartedAt == nil {
		t.Fatal("started_at missing")
	}
	if resp.Stats.TotalSkips != 2 {
		t.Fatalf("total_skips = %d, want 2", resp.Stats.TotalSkips)
	}
	if len(resp.Tasks) != 1 {
		t.Fatalf("tasks = %d, want 1", len(resp.Tasks))
	}
	task := resp.Tasks[0]
	if task.Meaning != "every 5 min" {
		t.Fatalf("meaning = %q, want %q", task.Meaning, "every 5 min")
	}
	if task.SkipCount != 2 || task.TimeoutMs != 30000 {
		t.Fatalf("skip_count/timeout_ms = %d/%d, want 2/30000", task.SkipCount, task.TimeoutMs)
	}
	if task.LastRun != nil {
		t.Fatalf("zero last_run should serialize as null, got %v", task.LastRun)
	}
	if len(task.Upcoming) != 1 {
		t.Fatalf("upcoming = %d entries, want 1", len(task.Upcoming))
	}
	if len(resp.Events) != 1 || resp.Events[0].Kind != "completed" || resp.Events[0].DurationMs != 300 {
		t.Fatalf("unexpected events: %+v", resp.Events)
	}
}

func TestHandlerActions(t *testing.T) {
	tests := []struct {
		action string
		body   string
		want   string
	}{
		{action: "run", want: "run:alpha"},
		{action: "pause", want: "disable:alpha"},
		{action: "resume", want: "enable:alpha"},
		{action: "remove", want: "remove:alpha"},
		{action: "schedule", body: `{"expr":"@every 90s"}`, want: "schedule:alpha:@every 90s"},
	}

	for _, tt := range tests {
		t.Run(tt.action, func(t *testing.T) {
			m := newFakeManager()
			handler := NewHandler("/tasks", m)

			var body *strings.Reader
			if tt.body != "" {
				body = strings.NewReader(tt.body)
			} else {
				body = strings.NewReader("")
			}
			req := httptest.NewRequest(http.MethodPost, "/tasks/api/tasks/alpha/"+tt.action, body)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
			}
			var result struct {
				OK bool `json:"ok"`
			}
			if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
				t.Fatalf("failed to decode JSON: %v", err)
			}
			if !result.OK {
				t.Fatal("ok = false, want true")
			}
			if len(m.actions) != 1 || m.actions[0] != tt.want {
				t.Fatalf("actions = %v, want [%s]", m.actions, tt.want)
			}
		})
	}
}

func TestHandlerActionStatusCodes(t *testing.T) {
	t.Run("manager error carries status code", func(t *testing.T) {
		m := newFakeManager()
		m.err = &StatusError{Code: http.StatusConflict, Err: errors.New("task already running")}
		handler := NewHandler("", m)

		req := httptest.NewRequest(http.MethodPost, "/api/tasks/alpha/run", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409", rec.Code)
		}
		if !strings.Contains(rec.Body.String(), "task already running") {
			t.Fatal("error message missing from body")
		}
	})

	t.Run("unknown action is 404", func(t *testing.T) {
		handler := NewHandler("", newFakeManager())
		req := httptest.NewRequest(http.MethodPost, "/api/tasks/alpha/explode", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
	})

	t.Run("schedule without body is 400", func(t *testing.T) {
		handler := NewHandler("", newFakeManager())
		req := httptest.NewRequest(http.MethodPost, "/api/tasks/alpha/schedule", strings.NewReader(""))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("GET on action route is rejected", func(t *testing.T) {
		handler := NewHandler("", newFakeManager())
		req := httptest.NewRequest(http.MethodGet, "/api/tasks/alpha/run", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("status = %d, want 405", rec.Code)
		}
	})
}

func TestHandlerActionSpecialTaskNames(t *testing.T) {
	m := newFakeManager()
	name := "weird name & ümlaut"
	m.tasks[0].Name = name
	handler := NewHandler("", m)

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/"+strings.ReplaceAll(strings.ReplaceAll(name, " ", "%20"), "&", "%26")+"/run", nil)
	req.URL.RawPath = ""
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rec.Code, rec.Body.String())
	}
	if len(m.actions) != 1 || m.actions[0] != "run:"+name {
		t.Fatalf("actions = %v, want run:%s", m.actions, name)
	}
}

func TestHandlerPreview(t *testing.T) {
	t.Run("valid expression", func(t *testing.T) {
		handler := NewHandler("", newFakeManager())
		req := httptest.NewRequest(http.MethodGet, "/api/schedule/preview?expr="+strings.ReplaceAll("0 */5 * * * *", " ", "%20"), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		var result previewResult
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !result.OK || result.Meaning != "every 5 min" || len(result.Next) != 3 {
			t.Fatalf("unexpected preview: %+v", result)
		}
	})

	t.Run("invalid expression", func(t *testing.T) {
		m := newFakeManager()
		m.err = &StatusError{Code: http.StatusBadRequest, Err: errors.New("invalid schedule")}
		handler := NewHandler("", m)
		req := httptest.NewRequest(http.MethodGet, "/api/schedule/preview?expr=garbage", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("empty expression", func(t *testing.T) {
		handler := NewHandler("", newFakeManager())
		req := httptest.NewRequest(http.MethodGet, "/api/schedule/preview", nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
	})
}

func TestHumanizeExpr(t *testing.T) {
	tests := []struct {
		expr string
		want string
	}{
		{"@every 90s", "every 90s"},
		{"@hourly", "hourly"},
		{"*/5 * * * * *", "every 5s"},
		{"* * * * * *", "every second"},
		{"0 */15 * * * *", "every 15 min"},
		{"0 * * * * *", "every minute"},
		{"0 0 */6 * * *", "every 6h"},
		{"0 0 * * * *", "hourly"},
		{"0 30 2 * * *", "daily 02:30"},
		{"0 0 9 * * 1", "0 0 9 * * 1"}, // weekday-restricted: fall back to raw expr
		{"weird", "weird"},
	}
	for _, tt := range tests {
		if got := humanizeExpr(tt.expr); got != tt.want {
			t.Errorf("humanizeExpr(%q) = %q, want %q", tt.expr, got, tt.want)
		}
	}
}
