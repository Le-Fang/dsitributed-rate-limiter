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
	l := newLimiterCore(c.core(), c.Parent, realClock{}, b, c.Metrics)
	l.start()
	return l, nil
}
