package pool

import "sync/atomic"

type sessionIDService struct {
	sid int64
}

func newSessionIDService() *sessionIDService {
	return &sessionIDService{
		sid: 0,
	}
}

func (c *sessionIDService) sessionID() int64 {
	return atomic.AddInt64(&c.sid, 1)
}
