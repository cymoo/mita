# Copilot Instructions

## Build, Test & Format

```bash
# Build the library
go build ./...

# Run all tests
go test ./...

# Run a single test
go test -run '^TestTaskExecution$' .

# Format modified Go files
gofmt -w <files>

# Run the example app (web UI at http://localhost:8080/tasks)
go run _examples/main.go
```

## Architecture

`mita` is a small Go module (`github.com/cymoo/mita`) for scheduled task management. Public scheduler APIs live in the root `mita` package:

- **`mita.go`** — Core: `TaskManager`, `TaskInfo`, lifecycle hooks, context injection, execution/concurrency logic.
- **`schedule.go`** — `Schedule` interface, `ScheduleBuilder` fluent API, and `CronSchedule` raw expressions.
- **`web.go` + `ui/`** — `web.go` is a thin root-package adapter for `tm.WebHandler(baseURL)`. The `ui` subpackage owns HTTP routing/data/actions and embeds `ui/index.html` and `ui/styles.css` with `go:embed`.

The only external dependency is `github.com/robfig/cron/v3`, used as the underlying cron engine.

**Lifecycle:** `New(opts...)` creates the manager → `AddTask(...)` registers tasks → `Start()` begins cron scheduling → `Stop()` gracefully shuts down (30 s timeout). `AddTask` can be called before or after `Start()`, but `Stop()` is terminal.

**`IsRunning()`** returns `true` only after `Start()` and before `Stop()`. `Start()` and `Stop()` are idempotent; `Start()` after `Stop()` does not restart the manager.

**Concurrency model:**
- A `sync.RWMutex` guards all reads/writes to `tasks` map and `TaskInfo` fields. Read operations use `RLock`; mutations use `Lock`.
- Optional global concurrency cap uses a channel-based semaphore (`chan struct{}`).
- Per-task overlap prevention is checked via `TaskInfo.RunningCount` under the write lock. `TaskInfo.Running` is derived from `RunningCount > 0`.
- When the global concurrency cap is full, scheduled executions are skipped and manual runs return `ErrMaxConcurrencyReached`; there is no hidden queue.

**Web UI routes** (all mounted under the `baseURL` prefix):

| Route | Handler |
|---|---|
| `GET /` | Task list page |
| `GET /stats` | Aggregated stats page |
| `POST /action` | Enable/disable/run/remove a task |
| `GET /api` | JSON task data (used by the list page) |
| `GET /assets/styles.css` | Embedded CSS |

## Key Conventions

**Cron expression format is 6 fields (seconds included):**
```
second minute hour day month weekday
```
This differs from standard 5-field cron. `Every()` and `Cron()` both produce this format.

**`ScheduleBuilder` panics (not errors) on invalid builder inputs** (e.g., non-positive interval, hour out of 0–23). Raw `Cron(...)` expressions are validated when `AddTask` calls into `robfig/cron`.

**Context value keys use `CtxtKey` internally; prefer `ContextValue` for retrieval:**
```go
// Injecting
mita.WithContextValue("db", dbConn)

// Retrieving inside a task
db := mita.ContextValue(ctx, "db").(*sql.DB)
```
The task name is stored under a private `taskNameKey`; retrieve it with `mita.GetTaskName(ctx)`.

**Lifecycle hooks are global manager options:** `WithOnTaskStart` and `WithOnTaskComplete` are synchronous observability hooks. Hook panics are recovered and logged; task panics are recovered, recorded as failures, and surfaced as `TaskCompleteEvent.Error`.

**Manual execution has async and blocking forms:** `RunTaskNow(name)` submits and returns after admission; `RunTaskNowAndWait(ctx, name)` blocks until completion and returns the task error. The blocking form uses `ctx` for cancellation/deadline only; context values still come from `WithContextValue` / `WithContextInjector`.

**`contextInjector` runs after static values are applied** and must not hold any locks (it is called outside the mutex). Static values are snapshot-copied before injection to avoid holding the lock during context building.

**Test pattern:** table-driven tests, always `defer tm.Stop()` for cleanup. Tests that exercise execution call `tm.Start()` explicitly; configuration-only tests do not.

**`GetTask` and `ListTasks` return copies** of `TaskInfo` structs (value copy via `infoCopy := *info`) so callers cannot mutate internal state.
