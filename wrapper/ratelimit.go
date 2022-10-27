package wrapper

import (
	"container/list"
	acceptor "github.com/gotechbook/gotechbook-framework-acceptor"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	metrics "github.com/gotechbook/gotechbook-framework-metrics"
	"time"
)

var _ acceptor.Conn = (*RateLimiter)(nil)

type RateLimiter struct {
	acceptor.Conn
	reporters    []metrics.Reporter
	limit        int
	interval     time.Duration
	times        list.List
	forceDisable bool
}

func NewRateLimiter(reporters []metrics.Reporter, conn acceptor.Conn, limit int, interval time.Duration, forceDisable bool) *RateLimiter {
	r := &RateLimiter{
		Conn:         conn,
		reporters:    reporters,
		limit:        limit,
		interval:     interval,
		forceDisable: forceDisable,
	}
	r.times.Init()
	return r
}
func (r *RateLimiter) GetNextMessage() (msg []byte, err error) {
	if r.forceDisable {
		return r.Conn.GetNextMessage()
	}
	for {
		msg, err := r.Conn.GetNextMessage()
		if err != nil {
			return nil, err
		}

		now := time.Now()
		if r.shouldRateLimit(now) {
			logger.Log.Errorf("Data=%s, Error=%s", msg, ErrRateLimitExceeded)
			metrics.ReportExceededRateLimiting(r.reporters)
			continue
		}

		return msg, err
	}
}
func (r *RateLimiter) shouldRateLimit(now time.Time) bool {
	if r.times.Len() < r.limit {
		r.times.PushBack(now)
		return false
	}

	front := r.times.Front()
	if diff := now.Sub(front.Value.(time.Time)); diff < r.interval {
		return true
	}

	front.Value = now
	r.times.MoveToBack(front)
	return false
}
