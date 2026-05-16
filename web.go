package mita

import (
	"net/http"

	webui "github.com/cymoo/mita/ui"
)

// WebHandler creates an HTTP handler for the task manager web interface.
// The baseURL parameter should be the URL prefix where the handler is mounted (e.g., "/tasks").
// Returns a ServeMux that can be integrated into your HTTP server.
func (tm *TaskManager) WebHandler(baseURL string) *http.ServeMux {
	return webui.NewHandler(baseURL, webAdapter{tm: tm})
}

type webAdapter struct {
	tm *TaskManager
}

func (a webAdapter) ListTasks() []webui.Task {
	tasks := a.tm.ListTasks()
	result := make([]webui.Task, len(tasks))
	for i, task := range tasks {
		result[i] = webui.Task{
			Name:         task.Name,
			Schedule:     task.Schedule,
			Enabled:      task.Enabled,
			Running:      task.Running,
			RunningCount: task.RunningCount,
			LastRun:      task.LastRun,
			NextRun:      task.NextRun,
			RunCount:     task.RunCount,
			ErrorCount:   task.ErrorCount,
			LastError:    task.LastError,
		}
	}
	return result
}

func (a webAdapter) Stats() webui.Stats {
	raw := a.tm.GetStats()
	return webui.Stats{
		TotalTasks:       intStat(raw["total_tasks"]),
		EnabledTasks:     intStat(raw["enabled_tasks"]),
		RunningTasks:     intStat(raw["running_tasks"]),
		TotalRuns:        int64Stat(raw["total_runs"]),
		TotalErrors:      int64Stat(raw["total_errors"]),
		MaxConcurrent:    intStat(raw["max_concurrent"]),
		AllowOverlapping: boolStat(raw["allow_overlapping"]),
	}
}

func (a webAdapter) EnableTask(name string) error {
	return a.tm.EnableTask(name)
}

func (a webAdapter) DisableTask(name string) error {
	return a.tm.DisableTask(name)
}

func (a webAdapter) RunTaskNow(name string) error {
	return a.tm.RunTaskNow(name)
}

func (a webAdapter) RemoveTask(name string) error {
	return a.tm.RemoveTask(name)
}

func intStat(value any) int {
	switch v := value.(type) {
	case int:
		return v
	case int64:
		return int(v)
	default:
		return 0
	}
}

func int64Stat(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	default:
		return 0
	}
}

func boolStat(value any) bool {
	v, _ := value.(bool)
	return v
}
