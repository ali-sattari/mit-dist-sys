package mr

import "time"

func periodicJobs(dur time.Duration, f func()) func() {
	ticker := time.NewTicker(dur)
	return func() {
		for {
			select {
			case <-ticker.C:
				f()
			}
		}
	}
}
