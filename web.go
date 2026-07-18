package mita

import (
	"net/http"

	webui "github.com/cymoo/mita/ui"
)

// WebHandler creates an HTTP handler for the task manager web interface.
// The baseURL parameter should be the URL prefix where the handler is mounted (e.g., "/tasks").
// Returns a ServeMux that can be integrated into your HTTP server.
//
// The interface exposes destructive actions (run/disable/remove) and has no
// built-in authentication; wrap it with authentication middleware before
// exposing it beyond localhost.
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
			Name:         info.Name,
			Schedule:     info.Schedule,
			Enabled:      info.Enabled,
			Running:      info.Running(),
			RunningCount: info.RunningCount,
			LastRun:      info.LastRun,
			NextRun:      info.NextRun,
			RunCount:     info.RunCount,
			ErrorCount:   info.ErrorCount,
			LastError:    info.LastError,
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
		MaxConcurrent:    stats.MaxConcurrent,
		AllowOverlapping: stats.AllowOverlapping,
	}
}

func (a webAdapter) EnableTask(name string) error {
	return a.tm.EnableTask(name)
}

func (a webAdapter) DisableTask(name string) error {
	return a.tm.DisableTask(name)
}

func (a webAdapter) RunTaskNow(name string) error {
	_, err := a.tm.RunTaskNow(name)
	return err
}

func (a webAdapter) RemoveTask(name string) error {
	return a.tm.RemoveTask(name)
}
