package mita

import (
	"testing"
	"time"
)

func TestScheduleBuilder(t *testing.T) {
	tests := []struct {
		name     string
		schedule Schedule
		want     string
	}{
		{
			name:     "every second",
			schedule: Every().Second(),
			want:     "* * * * * *",
		},
		{
			name:     "every minute",
			schedule: Every().Minute(),
			want:     "0 * * * * *",
		},
		{
			name:     "every hour",
			schedule: Every().Hour(),
			want:     "0 0 * * * *",
		},
		{
			name:     "every day",
			schedule: Every().Day(),
			want:     "0 0 0 * * *",
		},
		{
			name:     "every 5 seconds",
			schedule: Every().Seconds(5),
			want:     "*/5 * * * * *",
		},
		{
			name:     "every 15 minutes",
			schedule: Every().Minutes(15),
			want:     "0 */15 * * * *",
		},
		{
			name:     "every 6 hours",
			schedule: Every().Hours(6),
			want:     "0 0 */6 * * *",
		},
		{
			name:     "at specific time",
			schedule: Every().Day().At(14, 30),
			want:     "0 30 14 * * *",
		},
		{
			name:     "on specific weekday",
			schedule: Every().Day().OnWeekday(time.Monday),
			want:     "0 0 0 * * 1",
		},
		{
			name:     "on specific day of month",
			schedule: Every().Day().OnDay(15),
			want:     "0 0 0 15 * *",
		},
		{
			name:     "cron expression",
			schedule: Cron("0 0 12 * * *"),
			want:     "0 0 12 * * *",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.schedule.String()
			if got != tt.want {
				t.Errorf("Schedule.String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestScheduleBuilderPanics(t *testing.T) {
	tests := []struct {
		name string
		fn   func()
	}{
		{
			name: "negative seconds interval",
			fn:   func() { Every().Seconds(-1) },
		},
		{
			name: "zero minutes interval",
			fn:   func() { Every().Minutes(0) },
		},
		{
			name: "invalid hour in At",
			fn:   func() { Every().At(24, 0) },
		},
		{
			name: "invalid minute in At",
			fn:   func() { Every().At(12, 60) },
		},
		{
			name: "invalid day",
			fn:   func() { Every().OnDay(32) },
		},
		{
			name: "zero day",
			fn:   func() { Every().OnDay(0) },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Error("Expected panic, but didn't get one")
				}
			}()
			tt.fn()
		})
	}
}
