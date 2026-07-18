package mita

import (
	"errors"
	"net/http"
	"time"

	webui "github.com/cymoo/mita/ui"
)

// WebHandler creates an HTTP handler for the task manager web interface.
// The baseURL parameter should be the URL prefix where the handler is mounted (e.g., "/tasks").
// Returns a ServeMux that can be integrated into your HTTP server.
//
// The interface exposes destructive actions (run/pause/remove/reschedule) and
// has no built-in authentication; wrap it with authentication middleware
// before exposing it beyond localhost.
func (tm *TaskManager) WebHandler(baseURL string) *http.ServeMux {
	return webui.NewHandler(baseURL, webAdapter{tm: tm})
}

type webAdapter struct {
	tm *TaskManager
}

func (a webAdapter) ListTasks() []webui.Task {
	infos := a.tm.ListTasks()
	result := make([]webui.Task, len(infos))
	for i, info := range infos {
		result[i] = webui.Task{
			Name:             info.Name,
			Schedule:         info.Schedule,
			Enabled:          info.Enabled,
			Running:          info.Running(),
			RunningCount:     info.RunningCount,
			AddedAt:          info.AddedAt,
			LastRun:          info.LastRun,
			NextRun:          info.NextRun,
			RunCount:         info.RunCount,
			ErrorCount:       info.ErrorCount,
			SkipCount:        info.SkipCount,
			LastError:        info.LastError,
			Timeout:          info.Timeout,
			AllowOverlapping: info.AllowOverlapping,
		}
	}
	return result
}

func (a webAdapter) Stats() webui.Stats {
	stats := a.tm.Stats()
	return webui.Stats{
		TotalTasks:       stats.TotalTasks,
		EnabledTasks:     stats.EnabledTasks,
		RunningTasks:     stats.RunningTasks,
		TotalRuns:        stats.TotalRuns,
		TotalErrors:      stats.TotalErrors,
		TotalSkips:       stats.TotalSkips,
		MaxConcurrent:    stats.MaxConcurrent,
		AllowOverlapping: stats.AllowOverlapping,
		StartedAt:        stats.StartedAt,
	}
}

func (a webAdapter) RecentEvents(limit int) []webui.Event {
	events := a.tm.RecentEvents(limit)
	result := make([]webui.Event, len(events))
	for i, ev := range events {
		result[i] = webui.Event{
			At:       ev.At,
			Kind:     string(ev.Kind),
			TaskName: ev.TaskName,
			Message:  ev.Message,
			Duration: ev.Duration,
		}
	}
	return result
}

func (a webAdapter) UpcomingRuns(within time.Duration, perTaskLimit int) map[string][]time.Time {
	return a.tm.UpcomingRuns(within, perTaskLimit)
}

func (a webAdapter) EnableTask(name string) error {
	return webError(a.tm.EnableTask(name))
}

func (a webAdapter) DisableTask(name string) error {
	return webError(a.tm.DisableTask(name))
}

func (a webAdapter) RunTaskNow(name string) error {
	_, err := a.tm.RunTaskNow(name)
	return webError(err)
}

func (a webAdapter) RemoveTask(name string) error {
	return webError(a.tm.RemoveTask(name))
}

func (a webAdapter) UpdateSchedule(name, expr string) error {
	return webError(a.tm.UpdateSchedule(name, Cron(expr)))
}

func (a webAdapter) PreviewSchedule(expr string, n int) (string, []time.Time, error) {
	normalized, fires, err := a.tm.PreviewSchedule(Cron(expr), n)
	return normalized, fires, webError(err)
}

// webError attaches an HTTP status code to manager errors so the ui package
// (which cannot import mita) can answer with meaningful status codes.
func webError(err error) error {
	if err == nil {
		return nil
	}
	var code int
	switch {
	case errors.Is(err, ErrTaskNotFound):
		code = http.StatusNotFound
	case errors.Is(err, ErrTaskRunning), errors.Is(err, ErrMaxConcurrencyReached), errors.Is(err, ErrTaskExists):
		code = http.StatusConflict
	case errors.Is(err, ErrTaskManagerStopped):
		code = http.StatusServiceUnavailable
	default:
		// Everything else the manager returns is input-shaped
		// (invalid schedule, empty name, ...).
		code = http.StatusBadRequest
	}
	return &webui.StatusError{Code: code, Err: err}
}
