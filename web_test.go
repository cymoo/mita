package mita

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestWebHandlerRoutesAndAssets(t *testing.T) {
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
			tm := New()
			defer tm.Stop()
			if err := tm.AddTask("web-test", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
				t.Fatalf("AddTask() failed: %v", err)
			}
			handler := tm.WebHandler(tt.baseURL)

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
			}
			if rec := get("/api/state"); rec.Code != http.StatusOK {
				t.Fatalf("state status = %d, want 200", rec.Code)
			} else if !strings.Contains(rec.Body.String(), "web-test") {
				t.Fatal("state response missing task name")
			}
			if rec := get("/not-a-real-path"); rec.Code != http.StatusNotFound {
				t.Fatalf("unknown path status = %d, want 404", rec.Code)
			}
		})
	}
}

func TestWebHandlerState(t *testing.T) {
	tm := New(WithMaxConcurrent(3))
	defer tm.Stop()
	if err := tm.AddTask("api-task", Every().Seconds(30), func(ctx context.Context) error { return nil },
		WithTaskTimeout(45*time.Second)); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	if err := tm.RunTaskNowAndWait(context.Background(), "api-task"); err != nil {
		t.Fatalf("RunTaskNowAndWait() failed: %v", err)
	}
	if err := tm.Start(); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}
	handler := tm.WebHandler("")

	req := httptest.NewRequest(http.MethodGet, "/api/state?window=300", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp struct {
		Now   time.Time `json:"now"`
		Stats struct {
			TotalTasks    int        `json:"total_tasks"`
			MaxConcurrent int        `json:"max_concurrent"`
			StartedAt     *time.Time `json:"started_at"`
		} `json:"stats"`
		Tasks []struct {
			Name      string      `json:"name"`
			Meaning   string      `json:"meaning"`
			RunCount  int64       `json:"run_count"`
			TimeoutMs int64       `json:"timeout_ms"`
			Upcoming  []time.Time `json:"upcoming"`
		} `json:"tasks"`
		Events []struct {
			Kind string `json:"kind"`
			Task string `json:"task"`
		} `json:"events"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	if resp.Stats.TotalTasks != 1 || resp.Stats.MaxConcurrent != 3 {
		t.Fatalf("unexpected stats: %+v", resp.Stats)
	}
	if resp.Stats.StartedAt == nil {
		t.Fatal("started_at missing after Start()")
	}
	task := resp.Tasks[0]
	if task.Meaning != "every 30s" || task.RunCount != 1 || task.TimeoutMs != 45000 {
		t.Fatalf("unexpected task: %+v", task)
	}
	// every-30s task must have upcoming fires inside a 5-minute window
	if len(task.Upcoming) < 5 {
		t.Fatalf("upcoming = %d fires, want >= 5", len(task.Upcoming))
	}
	// the completed manual run must be in the event feed
	foundCompleted := false
	for _, ev := range resp.Events {
		if ev.Kind == "completed" && ev.Task == "api-task" {
			foundCompleted = true
		}
	}
	if !foundCompleted {
		t.Fatalf("completed event missing from feed: %+v", resp.Events)
	}
}

func TestWebHandlerActions(t *testing.T) {
	tm := New()
	defer tm.Stop()
	if err := tm.AddTask("action-test", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	handler := tm.WebHandler("/tasks")

	post := func(path, body string) *httptest.ResponseRecorder {
		var rd *strings.Reader
		if body != "" {
			rd = strings.NewReader(body)
		} else {
			rd = strings.NewReader("")
		}
		req := httptest.NewRequest(http.MethodPost, path, rd)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}

	t.Run("pause and resume", func(t *testing.T) {
		if rec := post("/tasks/api/tasks/action-test/pause", ""); rec.Code != http.StatusOK {
			t.Fatalf("pause status = %d: %s", rec.Code, rec.Body.String())
		}
		info, _ := tm.GetTask("action-test")
		if info.Enabled {
			t.Fatal("task still enabled after pause")
		}
		if rec := post("/tasks/api/tasks/action-test/resume", ""); rec.Code != http.StatusOK {
			t.Fatalf("resume status = %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("reschedule", func(t *testing.T) {
		if rec := post("/tasks/api/tasks/action-test/schedule", `{"expr":"*/5 * * * *"}`); rec.Code != http.StatusOK {
			t.Fatalf("schedule status = %d: %s", rec.Code, rec.Body.String())
		}
		info, _ := tm.GetTask("action-test")
		if info.Schedule != "0 */5 * * * *" {
			t.Fatalf("Schedule = %q, want normalized %q", info.Schedule, "0 */5 * * * *")
		}
	})

	t.Run("invalid schedule is 400 and leaves task unchanged", func(t *testing.T) {
		rec := post("/tasks/api/tasks/action-test/schedule", `{"expr":"*/90 * * * * *"}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
		}
		info, _ := tm.GetTask("action-test")
		if info.Schedule != "0 */5 * * * *" {
			t.Fatalf("Schedule changed to %q after invalid update", info.Schedule)
		}
	})

	t.Run("unknown task is 404", func(t *testing.T) {
		if rec := post("/tasks/api/tasks/nope/run", ""); rec.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", rec.Code)
		}
	})

	t.Run("conflict is 409", func(t *testing.T) {
		release := make(chan struct{})
		started := make(chan struct{})
		if err := tm.AddTask("busy", Every().Minute(), func(ctx context.Context) error {
			close(started)
			<-release
			return nil
		}); err != nil {
			t.Fatalf("AddTask() failed: %v", err)
		}
		if rec := post("/tasks/api/tasks/busy/run", ""); rec.Code != http.StatusOK {
			t.Fatalf("first run status = %d", rec.Code)
		}
		<-started
		if rec := post("/tasks/api/tasks/busy/run", ""); rec.Code != http.StatusConflict {
			t.Fatalf("second run status = %d, want 409: %s", rec.Code, rec.Body.String())
		}
		close(release)
	})

	t.Run("remove", func(t *testing.T) {
		if rec := post("/tasks/api/tasks/action-test/remove", ""); rec.Code != http.StatusOK {
			t.Fatalf("remove status = %d", rec.Code)
		}
		if _, err := tm.GetTask("action-test"); err == nil {
			t.Fatal("task still exists after remove")
		}
	})
}

func TestWebHandlerSpecialTaskNames(t *testing.T) {
	tm := New()
	defer tm.Stop()
	name := "weird name & task"
	if err := tm.AddTask(name, Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	handler := tm.WebHandler("")

	req := httptest.NewRequest(http.MethodPost, "/api/tasks/"+url.PathEscape(name)+"/pause", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
	}
	info, err := tm.GetTask(name)
	if err != nil {
		t.Fatalf("GetTask() failed: %v", err)
	}
	if info.Enabled {
		t.Fatal("task still enabled after pause via escaped path")
	}
}

func TestWebHandlerPreview(t *testing.T) {
	tm := New()
	defer tm.Stop()
	handler := tm.WebHandler("")

	t.Run("valid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/schedule/preview?expr="+url.QueryEscape("@every 90s"), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d: %s", rec.Code, rec.Body.String())
		}
		var result struct {
			OK      bool        `json:"ok"`
			Expr    string      `json:"expr"`
			Meaning string      `json:"meaning"`
			Next    []time.Time `json:"next"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !result.OK || result.Expr != "@every 90s" || result.Meaning != "every 90s" || len(result.Next) != 3 {
			t.Fatalf("unexpected preview: %+v", result)
		}
		gap := result.Next[1].Sub(result.Next[0])
		if gap != 90*time.Second {
			t.Fatalf("fire gap = %v, want 90s", gap)
		}
	})

	t.Run("out-of-range step is rejected", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/schedule/preview?expr="+url.QueryEscape("*/90 * * * * *"), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), "seconds step") {
			t.Fatalf("error should mention seconds step: %s", rec.Body.String())
		}
	})

	t.Run("five-field is normalized", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/api/schedule/preview?expr="+url.QueryEscape("*/5 * * * *"), nil)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		var result struct {
			OK   bool   `json:"ok"`
			Expr string `json:"expr"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !result.OK || result.Expr != "0 */5 * * * *" {
			t.Fatalf("unexpected preview: %+v", result)
		}
	})
}
