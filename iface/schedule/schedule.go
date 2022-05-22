package schedule

import "time"

type IScheduler interface {
	Next(prev time.Time) time.Time
}
