package ui

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

var (
	reStep  = regexp.MustCompile(`^\*/(\d+)$`)
	reDigit = regexp.MustCompile(`^\d+$`)
)

// humanizeExpr renders common schedule expressions as a short human phrase
// ("every 30s", "daily 02:00"). Expressions it doesn't recognize are returned
// unchanged, and the frontend falls back to monospace display.
func humanizeExpr(expr string) string {
	expr = strings.TrimSpace(expr)

	if d, ok := strings.CutPrefix(expr, "@every "); ok {
		return "every " + d
	}
	switch expr {
	case "@hourly":
		return "hourly"
	case "@daily", "@midnight":
		return "daily 00:00"
	case "@weekly":
		return "weekly"
	case "@monthly":
		return "monthly"
	case "@yearly", "@annually":
		return "yearly"
	}

	f := strings.Fields(expr)
	if len(f) != 6 {
		return expr
	}
	sec, min, hour, dom, mon, dow := f[0], f[1], f[2], f[3], f[4], f[5]
	if dom != "*" || mon != "*" || dow != "*" {
		return expr
	}

	if m := reStep.FindStringSubmatch(sec); m != nil && min == "*" && hour == "*" {
		return "every " + m[1] + "s"
	}
	if sec == "*" && min == "*" && hour == "*" {
		return "every second"
	}
	if sec == "0" {
		if m := reStep.FindStringSubmatch(min); m != nil && hour == "*" {
			if m[1] == "1" {
				return "every minute"
			}
			return "every " + m[1] + " min"
		}
		if min == "*" && hour == "*" {
			return "every minute"
		}
		if min == "0" {
			if m := reStep.FindStringSubmatch(hour); m != nil {
				if m[1] == "1" {
					return "hourly"
				}
				return "every " + m[1] + "h"
			}
			if hour == "*" {
				return "hourly"
			}
		}
		if reDigit.MatchString(min) && reDigit.MatchString(hour) {
			h, _ := strconv.Atoi(hour)
			m, _ := strconv.Atoi(min)
			return fmt.Sprintf("daily %02d:%02d", h, m)
		}
	}
	return expr
}
