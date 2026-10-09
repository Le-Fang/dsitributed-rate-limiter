package ratelimiter

// NewLimiter validates and wires an embedded limiter without network I/O.
// This intermediate implementation remains cold: background initialization and
// synchronization are added in the scheduler task before production use.
func NewLimiter(cfg Config) (*Limiter, error) {
	c, err := normalizeConfig(cfg)
	if err != nil {
		return nil, err
	}
	b, err := newRedisBackend(c)
	if err != nil {
		return nil, err
	}
	return newLimiterCore(c.core(), c.Parent, realClock{}, b, c.Metrics), nil
}
