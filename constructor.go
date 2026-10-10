package ratelimiter

// NewLimiter validates and starts an embedded limiter. Construction does not
// wait for Redis; each key initializes asynchronously before normal admission.
func NewLimiter(cfg Config) (*Limiter, error) {
	c, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	b, err := newRedisBackend(c)
	if err != nil {
		return nil, err
	}
	m := c.Metrics
	if m == nil {
		m = NewPrometheusMetrics()
	}
	l := newLimiterCore(c.core(), c.Parent, realClock{}, b, m)
	l.exposeMetrics = c.Metrics == nil
	l.events = newEventLogger(c.Logger, l.clock, l.addCounter)
	l.start()
	return l, nil
}
