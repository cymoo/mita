package mita

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestWebHandlerRoutesAndAssets(t *testing.T) {
	tests := []struct {
		name      string
		baseURL   string
		indexPath string
		apiPath   string
		cssPath   string
	}{
		{
			name:      "root",
			baseURL:   "",
			indexPath: "/",
			apiPath:   "/api",
			cssPath:   "/assets/styles.css",
		},
		{
			name:      "mounted",
			baseURL:   "/tasks",
			indexPath: "/tasks/",
			apiPath:   "/tasks/api",
			cssPath:   "/tasks/assets/styles.css",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tm := New()
			defer tm.Stop()
			if err := tm.AddTask("web-test", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
				t.Fatalf("AddTask() failed: %v", err)
			}
			handler := tm.WebHandler(tt.baseURL)

			indexReq := httptest.NewRequest(http.MethodGet, tt.indexPath, nil)
			indexRec := httptest.NewRecorder()
			handler.ServeHTTP(indexRec, indexReq)
			if indexRec.Code != http.StatusOK {
				t.Fatalf("index status = %d, want %d", indexRec.Code, http.StatusOK)
			}
			if body := indexRec.Body.String(); !strings.Contains(body, "mita") {
				t.Fatalf("index body missing expected content")
			}

			apiReq := httptest.NewRequest(http.MethodGet, tt.apiPath, nil)
			apiRec := httptest.NewRecorder()
			handler.ServeHTTP(apiRec, apiReq)
			if apiRec.Code != http.StatusOK {
				t.Fatalf("api status = %d, want %d", apiRec.Code, http.StatusOK)
			}
			if !strings.Contains(apiRec.Body.String(), "web-test") {
				t.Fatalf("api response missing task name")
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

func TestWebHandlerAPIReturnsJSON(t *testing.T) {
	tm := New()
	defer tm.Stop()
	if err := tm.AddTask("api-task", Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	handler := tm.WebHandler("")

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
			Enabled      bool   `json:"enabled"`
			RunningCount int    `json:"running_count"`
		} `json:"tasks"`
		Stats struct {
			TotalTasks int `json:"total_tasks"`
		} `json:"stats"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].Name != "api-task" {
		t.Fatalf("unexpected tasks in API response: %+v", resp.Tasks)
	}
	if resp.Tasks[0].RunningCount != 0 {
		t.Fatalf("running_count = %d, want 0", resp.Tasks[0].RunningCount)
	}
	if resp.Stats.TotalTasks != 1 {
		t.Fatalf("total_tasks = %d, want 1", resp.Stats.TotalTasks)
	}
}

func TestWebHandlerActionReturnsJSON(t *testing.T) {
	tm := New()
	defer tm.Stop()

	taskName := "action-test"
	if err := tm.AddTask(taskName, Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
		t.Fatalf("AddTask() failed: %v", err)
	}
	handler := tm.WebHandler("/tasks")

	t.Run("valid action returns ok=true", func(t *testing.T) {
		form := url.Values{}
		form.Set("name", taskName)
		form.Set("action", "disable")

		req := httptest.NewRequest(http.MethodPost, "/tasks/action", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
		}
		if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "application/json") {
			t.Fatalf("Content-Type = %q, want application/json", ct)
		}

		var result struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode JSON: %v", err)
		}
		if !result.OK {
			t.Fatalf("ok = false, want true; error = %q", result.Error)
		}
	})

	t.Run("unknown task returns ok=false", func(t *testing.T) {
		form := url.Values{}
		form.Set("name", "no-such-task")
		form.Set("action", "run")

		req := httptest.NewRequest(http.MethodPost, "/tasks/action", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		var result struct {
			OK    bool   `json:"ok"`
			Error string `json:"error"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode JSON: %v", err)
		}
		if result.OK {
			t.Fatalf("ok = true, want false for unknown task")
		}
		if result.Error == "" {
			t.Fatalf("expected non-empty error message")
		}
	})

	t.Run("missing params returns ok=false", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPost, "/tasks/action", strings.NewReader(""))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		var result struct {
			OK bool `json:"ok"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode JSON: %v", err)
		}
		if result.OK {
			t.Fatalf("ok = true, want false for missing params")
		}
	})

	t.Run("task name with special chars is handled safely", func(t *testing.T) {
		specialName := "line\r\nbreak & task"
		if err := tm.AddTask(specialName, Every().Minute(), func(ctx context.Context) error { return nil }); err != nil {
			t.Fatalf("AddTask() failed: %v", err)
		}

		form := url.Values{}
		form.Set("name", specialName)
		form.Set("action", "disable")

		req := httptest.NewRequest(http.MethodPost, "/tasks/action", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		if strings.ContainsAny(rec.Header().Get("Content-Type"), "\r\n") {
			t.Fatalf("Content-Type header contains CRLF")
		}
		var result struct {
			OK bool `json:"ok"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&result); err != nil {
			t.Fatalf("failed to decode JSON response: %v", err)
		}
		if !result.OK {
			t.Fatalf("ok = false for valid task with special name")
		}
	})
}

func TestWebHandlerRejectsUnexpectedRootPath(t *testing.T) {
	tm := New()
	defer tm.Stop()
	handler := tm.WebHandler("")

	req := httptest.NewRequest(http.MethodGet, "/not-a-real-path", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}
