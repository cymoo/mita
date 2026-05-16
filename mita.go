package mita

import (
	"context"
	"errors"
	"fmt"
	"log"
	"maps"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
)

// CtxtKey is a custom type for context keys to avoid collisions.
type CtxtKey string

const (
	// taskNameKey is the context key for storing the task name.
	taskNameKey CtxtKey = "taskName"
)

// Task represents a function that performs work within a given context.
// It should return an error if the task execution fails.
type Task func(ctx context.Context) error

// Public task manager errors. Use errors.Is to check wrapped errors returned by APIs.
var (
	ErrTaskNotFound          = errors.New("task not found")
	ErrTaskDisabled          = errors.New("task disabled")
	ErrTaskRunning           = errors.New("task already running")
	ErrMaxConcurrencyReached = errors.New("max concurrency reached")
	ErrTaskManagerStopped    = errors.New("task manager stopped")
	ErrNilContext            = errors.New("nil context")
)

// TaskTrigger identifies how a task execution was triggered.
type TaskTrigger string

const (
	TriggerScheduled TaskTrigger = "scheduled"
	TriggerManual    TaskTrigger = "manual"
)

type managerState int

const (
	managerCreated managerState = iota
	managerStarted
	managerStopped
)

// TaskStartEvent is emitted immediately before a task function starts.
type TaskStartEvent struct {
	TaskName     string
	Trigger      TaskTrigger
	StartedAt    time.Time
	Context      context.Context
	RunCount     int64
	RunningCount int
}

// TaskCompleteEvent is emitted after a task function finishes and internal state is updated.
type TaskCompleteEvent struct {
	TaskName     string
	Trigger      TaskTrigger
	StartedAt    time.Time
	FinishedAt   time.Time
	Duration     time.Duration
	Error        error
	RunCount     int64
	ErrorCount   int64
	RunningCount int
}

// TaskInfo holds metadata and statistics about a scheduled task.
type TaskInfo struct {
	Name         string       // Unique identifier for the task
	Schedule     string       // Cron expression for the task schedule
	Task         Task         // The actual task function to execute
	EntryID      cron.EntryID // Cron entry ID for this task
	AddedAt      time.Time    // When the task was added to the manager
	LastRun      time.Time    // Last execution time
	NextRun      time.Time    // Next scheduled execution time
	RunCount     int64        // Total number of executions
	ErrorCount   int64        // Total number of failed executions
	LastError    string       // Most recent error message (empty if last run succeeded)
	Enabled      bool         // Whether the task is enabled for execution
	Running      bool         // Whether the task is currently executing
	RunningCount int          // Number of currently executing instances
}

// TaskManager orchestrates scheduled task execution with concurrent control,
// error tracking, and flexible configuration options.
type TaskManager struct {
	cron             *cron.Cron                                                 // Underlying cron scheduler
	tasks            map[string]*TaskInfo                                       // Map of task name to task info
	mu               sync.RWMutex                                               // Protects tasks map and task info
	ctx              context.Context                                            // Manager lifecycle context
	cancel           context.CancelFunc                                         // Function to cancel the manager context
	logger           *log.Logger                                                // Logger for task execution events
	wg               sync.WaitGroup                                             // Tracks running tasks for graceful shutdown
	maxConcurrent    int                                                        // Maximum concurrent tasks (0 = unlimited)
	semaphore        chan struct{}                                              // Channel-based semaphore for concurrency control
	allowOverlapping bool                                                       // Whether same task can run concurrently
	contextValues    map[string]any                                             // Static values to inject into task contexts
	contextInjector  func(ctx context.Context, taskName string) context.Context // Dynamic context injection
	onTaskStart      func(TaskStartEvent)                                       // Called before a task starts
	onTaskComplete   func(TaskCompleteEvent)                                    // Called after a task completes
	state            managerState                                               // Manager lifecycle state
}

type taskExecution struct {
	name         string
	trigger      TaskTrigger
	task         Task
	startedAt    time.Time
	runCount     int64
	runningCount int
	acquiredSlot bool
}

// Option is a functional option for configuring TaskManager.
type Option func(*TaskManager)

// WithLogger sets a custom logger for the task manager.
// If not provided, log.Default() will be used.
func WithLogger(logger *log.Logger) Option {
	return func(tm *TaskManager) {
		if logger != nil {
			tm.logger = logger
		}
	}
}

// WithLocation sets the timezone for cron schedule interpretation.
func WithLocation(loc *time.Location) Option {
	return func(tm *TaskManager) {
		if loc != nil {
			tm.cron = cron.New(cron.WithLocation(loc), cron.WithSeconds())
		}
	}
}

// WithMaxConcurrent sets the maximum number of tasks that can run concurrently.
// A value of 0 means unlimited concurrency. Negative values are treated as 0.
// When the limit is reached, scheduled executions are skipped and manual runs return ErrMaxConcurrencyReached.
func WithMaxConcurrent(max int) Option {
	return func(tm *TaskManager) {
		if max < 0 {
			max = 0
		}
		tm.maxConcurrent = max
		if max > 0 {
			tm.semaphore = make(chan struct{}, max)
		}
	}
}

// WithAllowOverlapping controls whether the same task can run multiple instances concurrently.
// By default, overlapping is not allowed (false).
func WithAllowOverlapping(allow bool) Option {
	return func(tm *TaskManager) {
		tm.allowOverlapping = allow
	}
}

// WithContextValue adds a static key-value pair that will be injected into all task contexts.
func WithContextValue(key string, value any) Option {
	return func(tm *TaskManager) {
		if key == "" {
			return
		}
		if tm.contextValues == nil {
			tm.contextValues = make(map[string]any)
		}
		tm.contextValues[key] = value
	}
}

// WithContextInjector sets a custom function to dynamically inject values into task contexts.
// The injector is called for each task execution and receives the base context and task name.
func WithContextInjector(injector func(ctx context.Context, taskName string) context.Context) Option {
	return func(tm *TaskManager) {
		tm.contextInjector = injector
	}
}

// WithOnTaskStart sets a hook called immediately before each task execution starts.
func WithOnTaskStart(hook func(TaskStartEvent)) Option {
	return func(tm *TaskManager) {
		tm.onTaskStart = hook
	}
}

// WithOnTaskComplete sets a hook called after each task execution finishes.
func WithOnTaskComplete(hook func(TaskCompleteEvent)) Option {
	return func(tm *TaskManager) {
		tm.onTaskComplete = hook
	}
}

// New creates a new TaskManager with the given options.
// The manager must be started with Start() before tasks will execute.
func New(opts ...Option) *TaskManager {
	ctx, cancel := context.WithCancel(context.Background())

	tm := &TaskManager{
		cron:             cron.New(cron.WithSeconds()), // Support second-level scheduling
		tasks:            make(map[string]*TaskInfo),
		ctx:              ctx,
		cancel:           cancel,
		logger:           log.Default(),
		allowOverlapping: false, // Default: prevent overlapping executions
	}

	// Apply functional options
	for _, opt := range opts {
		opt(tm)
	}

	return tm
}

// AddTask registers a new task with the given name and schedule.
// Returns an error if a task with the same name already exists or if the schedule is invalid.
func (tm *TaskManager) AddTask(name string, schedule Schedule, task Task) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}
	if schedule == nil {
		return fmt.Errorf("schedule cannot be nil")
	}
	if task == nil {
		return fmt.Errorf("task function cannot be nil")
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.state == managerStopped {
		return fmt.Errorf("cannot add task %q: %w", name, ErrTaskManagerStopped)
	}

	// Check if task already exists
	if _, exists := tm.tasks[name]; exists {
		return fmt.Errorf("task '%s' already exists", name)
	}

	// Wrap task to add statistics and error handling
	wrappedTask := tm.wrapTask(name)

	// Add to cron scheduler
	entryID, err := tm.cron.AddFunc(schedule.String(), wrappedTask)
	if err != nil {
		return fmt.Errorf("failed to add task '%s': %w", name, err)
	}

	// Store task information
	tm.tasks[name] = &TaskInfo{
		Name:     name,
		Schedule: schedule.String(),
		Task:     task,
		EntryID:  entryID,
		AddedAt:  time.Now(),
		Enabled:  true,
	}

	tm.logger.Printf("Task '%s' added with schedule: %s", name, schedule)
	return nil
}

// wrapTask wraps a task function with execution tracking, error handling,
// concurrency control, and overlap prevention.
func (tm *TaskManager) wrapTask(name string) func() {
	return func() {
		exec, err := tm.beginExecution(name, TriggerScheduled)
		if err != nil {
			if !errors.Is(err, ErrTaskNotFound) &&
				!errors.Is(err, ErrTaskDisabled) &&
				!errors.Is(err, ErrTaskManagerStopped) {
				tm.logger.Printf("Task '%s' skipped: %v", name, err)
			}
			return
		}
		tm.runExecution(exec)
	}
}

func (tm *TaskManager) beginExecution(name string, trigger TaskTrigger) (*taskExecution, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.state == managerStopped {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskManagerStopped)
	}

	info := tm.tasks[name]
	if info == nil {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}
	if !info.Enabled {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskDisabled)
	}
	if !tm.allowOverlapping && info.RunningCount > 0 {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskRunning)
	}

	acquiredSlot := false
	if tm.semaphore != nil {
		select {
		case tm.semaphore <- struct{}{}:
			acquiredSlot = true
		default:
			return nil, fmt.Errorf("task %q: %w", name, ErrMaxConcurrencyReached)
		}
	}

	now := time.Now()
	info.RunningCount++
	info.Running = true
	info.LastRun = now
	info.RunCount++
	tm.wg.Add(1)

	return &taskExecution{
		name:         name,
		trigger:      trigger,
		task:         info.Task,
		startedAt:    now,
		runCount:     info.RunCount,
		runningCount: info.RunningCount,
		acquiredSlot: acquiredSlot,
	}, nil
}

func (tm *TaskManager) runExecution(exec *taskExecution) {
	_ = tm.runExecutionWithContext(exec, nil)
}

func (tm *TaskManager) runExecutionWithContext(exec *taskExecution, callerCtx context.Context) error {
	defer func() {
		if exec.acquiredSlot {
			<-tm.semaphore
		}
		tm.wg.Done()
	}()

	ctx, cleanup, err := tm.safeTaskContext(exec.name, callerCtx)
	if err != nil {
		finishedAt := time.Now()
		completeEvent := tm.completeExecution(exec, finishedAt, err)
		tm.logger.Printf("Task '%s' failed before start after %v: %v", exec.name, completeEvent.Duration, err)
		tm.fireTaskComplete(completeEvent)
		return err
	}
	defer cleanup()

	tm.fireTaskStart(TaskStartEvent{
		TaskName:     exec.name,
		Trigger:      exec.trigger,
		StartedAt:    exec.startedAt,
		Context:      ctx,
		RunCount:     exec.runCount,
		RunningCount: exec.runningCount,
	})

	err = tm.callTask(exec.task, ctx)
	finishedAt := time.Now()
	completeEvent := tm.completeExecution(exec, finishedAt, err)

	if err != nil {
		tm.logger.Printf("Task '%s' failed after %v: %v", exec.name, completeEvent.Duration, err)
	} else {
		tm.logger.Printf("Task '%s' completed successfully in %v", exec.name, completeEvent.Duration)
	}

	tm.fireTaskComplete(completeEvent)
	return err
}

func (tm *TaskManager) callTask(task Task, ctx context.Context) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicAsError(r)
		}
	}()
	return task(ctx)
}

func (tm *TaskManager) safeTaskContext(name string, callerCtx context.Context) (ctx context.Context, cleanup func(), err error) {
	defer func() {
		if r := recover(); r != nil {
			err = panicAsError(r)
			cleanup = func() {}
		}
	}()
	base := tm.getTaskContext(name)
	ctx, cleanup = withCallerCancellation(base, callerCtx)
	return ctx, cleanup, nil
}

func withCallerCancellation(base context.Context, caller context.Context) (context.Context, func()) {
	if caller == nil || caller.Done() == nil {
		return base, func() {}
	}

	ctx, cancel := context.WithCancel(base)
	done := make(chan struct{})
	go func() {
		select {
		case <-caller.Done():
			cancel()
		case <-base.Done():
			cancel()
		case <-done:
		}
	}()

	return ctx, func() {
		cancel()
		close(done)
	}
}

func panicAsError(value any) error {
	if err, ok := value.(error); ok {
		return fmt.Errorf("task panic: %w", err)
	}
	return fmt.Errorf("task panic: %v", value)
}

func (tm *TaskManager) completeExecution(exec *taskExecution, finishedAt time.Time, taskErr error) TaskCompleteEvent {
	event := TaskCompleteEvent{
		TaskName:   exec.name,
		Trigger:    exec.trigger,
		StartedAt:  exec.startedAt,
		FinishedAt: finishedAt,
		Duration:   finishedAt.Sub(exec.startedAt),
		Error:      taskErr,
		RunCount:   exec.runCount,
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if info, ok := tm.tasks[exec.name]; ok {
		if taskErr != nil {
			info.ErrorCount++
			info.LastError = taskErr.Error()
		} else {
			info.LastError = ""
		}
		if info.RunningCount > 0 {
			info.RunningCount--
		}
		info.Running = info.RunningCount > 0
		entry := tm.cron.Entry(info.EntryID)
		info.NextRun = entry.Next

		event.RunCount = info.RunCount
		event.ErrorCount = info.ErrorCount
		event.RunningCount = info.RunningCount
	}

	return event
}

func (tm *TaskManager) fireTaskStart(event TaskStartEvent) {
	if tm.onTaskStart == nil {
		return
	}
	defer tm.recoverHookPanic("OnTaskStart", event.TaskName)
	tm.onTaskStart(event)
}

func (tm *TaskManager) fireTaskComplete(event TaskCompleteEvent) {
	if tm.onTaskComplete == nil {
		return
	}
	defer tm.recoverHookPanic("OnTaskComplete", event.TaskName)
	tm.onTaskComplete(event)
}

func (tm *TaskManager) recoverHookPanic(hookName, taskName string) {
	if r := recover(); r != nil {
		tm.logger.Printf("%s hook for task '%s' panicked: %v", hookName, taskName, r)
	}
}

// RemoveTask removes a task from the manager.
// Returns an error if the task does not exist.
func (tm *TaskManager) RemoveTask(name string) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	info, exists := tm.tasks[name]
	if !exists {
		return fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	tm.cron.Remove(info.EntryID)
	delete(tm.tasks, name)
	tm.logger.Printf("Task '%s' removed", name)
	return nil
}

// EnableTask enables a previously disabled task.
// The task will resume executing on its schedule.
func (tm *TaskManager) EnableTask(name string) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	info, exists := tm.tasks[name]
	if !exists {
		return fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	info.Enabled = true
	tm.logger.Printf("Task '%s' enabled", name)
	return nil
}

// DisableTask disables a task without removing it.
// The task will not execute but can be re-enabled later.
func (tm *TaskManager) DisableTask(name string) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	info, exists := tm.tasks[name]
	if !exists {
		return fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	info.Enabled = false
	tm.logger.Printf("Task '%s' disabled", name)
	return nil
}

// GetTask returns a copy of the task information for the given task name.
// Returns an error if the task does not exist.
func (tm *TaskManager) GetTask(name string) (*TaskInfo, error) {
	if name == "" {
		return nil, fmt.Errorf("task name cannot be empty")
	}

	tm.mu.RLock()
	defer tm.mu.RUnlock()

	info, exists := tm.tasks[name]
	if !exists {
		return nil, fmt.Errorf("task %q: %w", name, ErrTaskNotFound)
	}

	// Update next run time and return a copy
	entry := tm.cron.Entry(info.EntryID)
	infoCopy := *info
	infoCopy.NextRun = entry.Next
	infoCopy.Running = infoCopy.RunningCount > 0

	return &infoCopy, nil
}

// ListTasks returns a copy of all task information.
// The returned slice can be safely modified without affecting the manager.
func (tm *TaskManager) ListTasks() []*TaskInfo {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	tasks := make([]*TaskInfo, 0, len(tm.tasks))
	for _, info := range tm.tasks {
		entry := tm.cron.Entry(info.EntryID)
		infoCopy := *info
		infoCopy.NextRun = entry.Next
		infoCopy.Running = infoCopy.RunningCount > 0
		tasks = append(tasks, &infoCopy)
	}

	return tasks
}

// Start begins the task scheduler.
// Tasks will start executing according to their schedules.
func (tm *TaskManager) Start() {
	tm.mu.Lock()
	if tm.state != managerCreated {
		tm.mu.Unlock()
		return
	}
	tm.state = managerStarted
	tm.cron.Start()
	tm.mu.Unlock()
	tm.logger.Println("Task manager started")
}

// Stop gracefully shuts down the task manager.
// It stops accepting new task executions and waits for running tasks to complete
// or times out after 30 seconds.
func (tm *TaskManager) Stop() {
	tm.mu.Lock()
	if tm.state == managerStopped {
		tm.mu.Unlock()
		return
	}
	tm.state = managerStopped
	tm.logger.Println("Stopping task manager...")

	// Cancel the context to stop new executions
	tm.cancel()

	// Stop the cron scheduler and get its context
	cronCtx := tm.cron.Stop()
	tm.mu.Unlock()

	// Wait for all running tasks to complete
	done := make(chan struct{})
	go func() {
		tm.wg.Wait()
		close(done)
	}()

	// Wait for completion or timeout
	select {
	case <-done:
		tm.logger.Println("All tasks completed gracefully")
	case <-cronCtx.Done():
		// Cron stopped, still wait a bit for tasks
		select {
		case <-done:
			tm.logger.Println("All tasks completed after cron stop")
		case <-time.After(30 * time.Second):
			tm.logger.Println("Timeout waiting for tasks to complete")
		}
	case <-time.After(30 * time.Second):
		tm.logger.Println("Timeout waiting for tasks to complete")
	}
}

// RunTaskNow immediately executes a task outside of its regular schedule.
// The execution is asynchronous and subject to the same concurrency limits
// and overlap rules as scheduled executions.
func (tm *TaskManager) RunTaskNow(name string) error {
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}

	exec, err := tm.beginExecution(name, TriggerManual)
	if err != nil {
		return err
	}

	go tm.runExecution(exec)
	return nil
}

// RunTaskNowAndWait immediately executes a task and blocks until it completes.
// The provided context controls cancellation for this manual execution; its values
// are not injected into the task context. Use WithContextValue or WithContextInjector
// for task context values.
func (tm *TaskManager) RunTaskNowAndWait(ctx context.Context, name string) error {
	if ctx == nil {
		return ErrNilContext
	}
	if name == "" {
		return fmt.Errorf("task name cannot be empty")
	}
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("task %q: %w", name, err)
	}

	exec, err := tm.beginExecution(name, TriggerManual)
	if err != nil {
		return err
	}

	return tm.runExecutionWithContext(exec, ctx)
}

// SetContextValue adds or updates a static context value that will be
// injected into all task contexts.
func (tm *TaskManager) SetContextValue(key string, value any) {
	if key == "" {
		return
	}

	tm.mu.Lock()
	defer tm.mu.Unlock()

	if tm.contextValues == nil {
		tm.contextValues = make(map[string]any)
	}
	tm.contextValues[key] = value
}

// GetContextValue retrieves a static context value.
// Returns nil if the key does not exist.
func (tm *TaskManager) GetContextValue(key string) any {
	if key == "" {
		return nil
	}

	tm.mu.RLock()
	defer tm.mu.RUnlock()

	if tm.contextValues == nil {
		return nil
	}
	return tm.contextValues[key]
}

// getTaskContext creates a context for task execution with injected values.
// It includes the task name, static context values, and any custom injections.
func (tm *TaskManager) getTaskContext(name string) context.Context {
	ctx := tm.ctx

	// Inject task name using typed key
	ctx = context.WithValue(ctx, taskNameKey, name)

	// Inject static context values (copy to avoid holding lock during injection)
	var contextValues map[string]any
	tm.mu.RLock()
	if len(tm.contextValues) > 0 {
		contextValues = make(map[string]any, len(tm.contextValues))
		maps.Copy(contextValues, tm.contextValues)
	}
	tm.mu.RUnlock()

	// Apply static values without holding lock
	for key, value := range contextValues {
		ctx = context.WithValue(ctx, CtxtKey(key), value)
	}

	// Call custom injector without holding any locks
	if tm.contextInjector != nil {
		ctx = tm.contextInjector(ctx, name)
	}

	return ctx
}

// GetTaskName extracts the task name from a task context.
// Returns empty string if the context doesn't contain a task name.
func GetTaskName(ctx context.Context) string {
	if name, ok := ctx.Value(taskNameKey).(string); ok {
		return name
	}
	return ""
}

// ContextValue retrieves a value injected with WithContextValue or SetContextValue.
func ContextValue(ctx context.Context, key string) any {
	if key == "" {
		return nil
	}
	return ctx.Value(CtxtKey(key))
}

// GetStats returns aggregated statistics about the task manager and all tasks.
func (tm *TaskManager) GetStats() map[string]interface{} {
	tm.mu.RLock()
	defer tm.mu.RUnlock()

	totalTasks := len(tm.tasks)
	enabledTasks := 0
	runningTasks := 0
	totalRuns := int64(0)
	totalErrors := int64(0)

	for _, info := range tm.tasks {
		if info.Enabled {
			enabledTasks++
		}
		if info.RunningCount > 0 {
			runningTasks++
		}
		totalRuns += info.RunCount
		totalErrors += info.ErrorCount
	}

	return map[string]interface{}{
		"total_tasks":       totalTasks,
		"enabled_tasks":     enabledTasks,
		"running_tasks":     runningTasks,
		"total_runs":        totalRuns,
		"total_errors":      totalErrors,
		"max_concurrent":    tm.maxConcurrent,
		"allow_overlapping": tm.allowOverlapping,
	}
}

// IsRunning checks whether the task manager is currently running.
func (tm *TaskManager) IsRunning() bool {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return tm.state == managerStarted
}
