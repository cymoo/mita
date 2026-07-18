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

- **`mita.go`** — Core: `TaskManager`, `TaskInfo` snapshots, lifecycle hooks, context injection, execution/concurrency logic.
- **`schedule.go`** — `Schedule` interface, `ScheduleBuilder` fluent API, `CronSchedule` raw expressions, and cron-expression normalization.
- **`web.go` + `ui/`** — `web.go` is a thin root-package adapter for `tm.WebHandler(baseURL)`. The `ui` subpackage owns HTTP routing/data/actions and embeds `ui/index.html` and `ui/styles.css` with `go:embed`.

The only external dependency is `github.com/robfig/cron/v3`, used as the underlying cron engine.

**Lifecycle:** `New(opts...)` creates the manager → `AddTask(...)` registers tasks → `Start()` begins cron scheduling → `Stop()` gracefully shuts down. `Start`, `Stop`, and `StopContext` return errors (`ErrTaskManagerStarted` / `ErrTaskManagerStopped`). `AddTask` can be called before or after `Start()`, but `Stop()` is terminal: `Start()` after `Stop()` returns `ErrTaskManagerStopped`.

**Shutdown is two-phase:** `Stop()` first waits up to the shutdown timeout (`WithShutdownTimeout`, default `DefaultShutdownTimeout` = 30 s) for running tasks to finish *without* canceling them; only after the deadline does it cancel task contexts and wait a short grace period. `StopContext(ctx)` uses the caller's context as the deadline instead.

**`IsStarted()`** returns `true` only after `Start()` and before `Stop()`.

**Concurrency model:**
- Internal task state lives in unexported `taskEntry` structs guarded by a `sync.RWMutex`. Read operations use `RLock`; mutations use `Lock`.
- Optional global concurrency cap uses a channel-based semaphore (`chan struct{}`).
- Overlap prevention is checked via `taskEntry.runningCount` under the write lock; per-task `WithTaskOverlapping` overrides the manager-level `WithAllowOverlapping`.
- When the global concurrency cap is full, scheduled executions are skipped and manual runs return `ErrMaxConcurrencyReached`; there is no hidden queue. Skips due to overlap or the cap fire the `WithOnTaskSkip` hook.
- `completeExecution` must not call into `cron` (each `cron.Entry` lookup snapshots the whole entry list); `NextRun` is resolved only in `GetTask`/`ListTasks`, and `ListTasks` uses a single `cron.Entries()` call.

**Web UI routes** (all mounted under the `baseURL` prefix, Go 1.22 method patterns):

| Route | Handler |
|---|---|
| `GET /{$}` | Single-page dashboard (light/dark, vanilla JS in `index.html`) |
| `GET /assets/styles.css` | Embedded CSS |
| `GET /api/state?window=SECONDS` | Snapshot: stats, tasks (+ upcoming fire times), recent events |
| `GET /api/schedule/preview?expr=` | Validate an expression, return normalized form + next fires |
| `POST /api/tasks/{name}/{action}` | `run`/`pause`/`resume`/`remove`/`schedule` (JSON `{"expr":...}`) |

Status codes come from `ui.StatusError`, which the `web.go` adapter wraps around
manager errors (`webError`): 404 not-found, 409 running/concurrency/exists,
503 stopped, 400 everything else. The `ui` package never imports `mita`
(mita → ui is the only direction), which is why the adapter does the mapping.

The manager feeds the UI via `RecentEvents` (a bounded in-manager ring buffer of
`Event`s recorded on completion/failure/skip/lifecycle), `UpcomingRuns` (bulk
next-fire times, one cron snapshot), and `PreviewSchedule`. `humanizeExpr` in
`ui/format.go` renders common expressions as phrases and falls back to the raw
expression.

The web UI has no built-in auth; destructive actions are exposed, so it must be wrapped with auth middleware in real deployments.

## Key Conventions

**Cron expression format is 6 fields (seconds included):**
```
second minute hour day month weekday
```
Standard 5-field expressions are also accepted and normalized by prepending a `0` seconds column (`normalizeCronExpr`). Descriptors (`@hourly`, `@every 90s`) pass through unchanged.

**`ScheduleBuilder` records errors instead of panicking.** Invalid inputs (interval out of range, hour out of 0–23, mixing `Interval` with field methods) are surfaced via `Err()` and rejected by `AddTask`/`UpdateSchedule` (via the optional `interface{ Err() error }` check in `scheduleExpr`). Step intervals are range-validated (`Seconds`/`Minutes` 1–59, `Hours` 1–23, `Days` 1–31) because cron silently misfires on out-of-range steps (`*/90` in seconds runs every 60 s). `Interval(d)` emits `@every` for exact fixed-length periods.

**Context keys are private types:** the task name uses `taskNameKey struct{}` and user values use `valueKey string`, so user keys can never shadow the task name. Inject with `WithContextValue`/`SetContextValue`, retrieve with `mita.ContextValue(ctx, key)`; retrieve the task name with `mita.GetTaskName(ctx)`.

**Lifecycle hooks are global manager options:** `WithOnTaskStart`, `WithOnTaskComplete`, and `WithOnTaskSkip` are synchronous observability hooks. Hook panics are recovered and logged; task panics are recovered, recorded as failures, and surfaced as `TaskCompleteEvent.Error`. `TaskCompleteEvent.RunCount` reports the completing execution's own sequence number (matching its start event), not the latest global counter.

**Manual execution has async and blocking forms:** `RunTaskNow(name)` returns `(<-chan error, error)` — the channel receives the task result exactly once and may be ignored; `RunTaskNowAndWait(ctx, name)` blocks until completion and returns the task error. The blocking form uses `ctx` for cancellation/deadline only; context values still come from `WithContextValue` / `WithContextInjector`. Manual triggers are allowed on disabled tasks (disable only pauses the schedule).

**Per-task options:** `AddTask(name, schedule, task, opts...)` accepts `WithTaskTimeout` (context deadline per execution) and `WithTaskOverlapping`. `UpdateSchedule(name, schedule)` swaps the cron entry in place, preserving statistics and settings.

**`contextInjector` runs after static values are applied** and must not hold any locks (it is called outside the mutex). Static values are snapshot-copied before injection to avoid holding the lock during context building.

**Test pattern:** table-driven tests, always `defer tm.Stop()` for cleanup. Tests that exercise execution call `tm.Start()` explicitly; configuration-only tests do not. Tests that need Stop to cancel tasks use `WithShutdownTimeout` with a short duration.

**`GetTask` and `ListTasks` return value snapshots** (`TaskInfo`, with derived `Running()` method) so callers cannot mutate internal state. `ListTasks` is sorted by name. `Stats()` returns a typed `Stats` struct.
