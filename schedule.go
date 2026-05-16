package mita

import (
	"fmt"
	"time"
)

// Schedule represents a task scheduling expression.
type Schedule interface {
	String() string
}

// CronSchedule wraps a standard cron expression string.
type CronSchedule struct {
	expression string
}

// Cron creates a Schedule from a cron expression string.
// The expression should be in the format: "second minute hour day month weekday"
func Cron(expr string) *CronSchedule {
	return &CronSchedule{expression: expr}
}

// String returns the cron expression.
func (c *CronSchedule) String() string {
	return c.expression
}

// ScheduleBuilder provides a fluent API for building cron schedules.
type ScheduleBuilder struct {
	second  string
	minute  string
	hour    string
	day     string
	month   string
	weekday string
}

// String converts the builder to a cron expression string.
func (s *ScheduleBuilder) String() string {
	return fmt.Sprintf("%s %s %s %s %s %s",
		s.second, s.minute, s.hour, s.day, s.month, s.weekday)
}

// Every creates a new ScheduleBuilder with default values (runs every minute).
func Every() *ScheduleBuilder {
	return &ScheduleBuilder{
		second:  "0",
		minute:  "*",
		hour:    "*",
		day:     "*",
		month:   "*",
		weekday: "*",
	}
}

// Second configures the schedule to run every second.
func (s *ScheduleBuilder) Second() *ScheduleBuilder {
	s.second = "*"
	s.minute = "*"
	s.hour = "*"
	return s
}

// Minute configures the schedule to run every minute (at 0 seconds).
func (s *ScheduleBuilder) Minute() *ScheduleBuilder {
	s.second = "0"
	s.minute = "*"
	s.hour = "*"
	return s
}

// Hour configures the schedule to run every hour (at 0 minutes, 0 seconds).
func (s *ScheduleBuilder) Hour() *ScheduleBuilder {
	s.second = "0"
	s.minute = "0"
	s.hour = "*"
	return s
}

// Day configures the schedule to run every day (at midnight).
func (s *ScheduleBuilder) Day() *ScheduleBuilder {
	s.second = "0"
	s.minute = "0"
	s.hour = "0"
	s.day = "*"
	return s
}

// Seconds configures the schedule to run at the specified second interval.
// For example, Seconds(30) runs every 30 seconds.
// Interval must be positive, otherwise the method panics.
func (s *ScheduleBuilder) Seconds(interval int) *ScheduleBuilder {
	if interval <= 0 {
		panic("interval must be positive")
	}
	s.second = fmt.Sprintf("*/%d", interval)
	s.minute = "*"
	s.hour = "*"
	return s
}

// Minutes configures the schedule to run at the specified minute interval.
// For example, Minutes(15) runs every 15 minutes.
// Interval must be positive, otherwise the method panics.
func (s *ScheduleBuilder) Minutes(interval int) *ScheduleBuilder {
	if interval <= 0 {
		panic("interval must be positive")
	}
	s.second = "0"
	s.minute = fmt.Sprintf("*/%d", interval)
	s.hour = "*"
	return s
}

// Hours configures the schedule to run at the specified hour interval.
// For example, Hours(6) runs every 6 hours.
// Interval must be positive, otherwise the method panics.
func (s *ScheduleBuilder) Hours(interval int) *ScheduleBuilder {
	if interval <= 0 {
		panic("interval must be positive")
	}
	s.second = "0"
	s.minute = "0"
	s.hour = fmt.Sprintf("*/%d", interval)
	return s
}

// Days configures the schedule to run at the specified day interval.
// For example, Days(2) runs every 2 days at midnight.
// Interval must be positive, otherwise the method panics.
func (s *ScheduleBuilder) Days(interval int) *ScheduleBuilder {
	if interval <= 0 {
		panic("interval must be positive")
	}
	s.second = "0"
	s.minute = "0"
	s.hour = "0"
	s.day = fmt.Sprintf("*/%d", interval)
	return s
}

// At specifies a specific time of day for the schedule.
// For example, At(14, 30) runs at 2:30 PM.
// Hour must be 0-23 and minute must be 0-59, otherwise the method panics.
func (s *ScheduleBuilder) At(hour, minute int) *ScheduleBuilder {
	if hour < 0 || hour > 23 {
		panic("hour must be between 0 and 23")
	}
	if minute < 0 || minute > 59 {
		panic("minute must be between 0 and 59")
	}
	s.second = "0"
	s.minute = fmt.Sprintf("%d", minute)
	s.hour = fmt.Sprintf("%d", hour)
	return s
}

// OnWeekday restricts the schedule to a specific day of the week.
// For example, OnWeekday(time.Monday) runs only on Mondays.
func (s *ScheduleBuilder) OnWeekday(weekday time.Weekday) *ScheduleBuilder {
	s.weekday = fmt.Sprintf("%d", weekday)
	return s
}

// OnDay restricts the schedule to a specific day of the month.
// For example, OnDay(15) runs on the 15th of each month.
// Day must be between 1 and 31, otherwise the method panics.
func (s *ScheduleBuilder) OnDay(day int) *ScheduleBuilder {
	if day < 1 || day > 31 {
		panic("day must be between 1 and 31")
	}
	s.day = fmt.Sprintf("%d", day)
	return s
}
