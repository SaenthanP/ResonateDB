package sim

import "time"

type RealClock struct{}

func (c RealClock) Now() time.Time {
	return time.Now()
}

func (c RealClock) NewTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}
