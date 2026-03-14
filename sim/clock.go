package sim

import "time"

type RealClock struct{}

func (c RealClock) Now() time.Time {
	return time.Now()
}

func (c RealClock) NewTicker(d time.Duration) *time.Ticker {
	return time.NewTicker(d)
}

type SimClock struct {
	now time.Time
}

func NewSimClock(seed int64) *SimClock {
	return &SimClock{now: time.Unix(seed, 0)}
}

func (c *SimClock) Now() time.Time {
	return c.now
}
func (c *SimClock) Advance(d time.Duration) {
	c.now.Add(d)
}

// this ticker is never read
func (c *SimClock) NewTicker(d time.Duration) *time.Ticker {
	return &time.Ticker{C: make(chan time.Time)}
}
