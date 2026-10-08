package ratelimiter

type Labels struct{ Tier, Reason string }

// Metrics receives aggregate observations with bounded labels.
type Metrics interface {
	AddCounter(name string, labels Labels, delta uint64)
	SetGauge(name string, value float64)
	Observe(name string, seconds float64)
}
