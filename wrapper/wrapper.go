package wrapper

import (
	"errors"
	acceptor "github.com/gotechbook/gotechbook-framework-acceptor"
)

var (
	ErrRateLimitExceeded = errors.New("rate limit limit exceeded")
)

type Wrapper interface {
	Wrap(acceptor.Acceptor) acceptor.Acceptor
}

func WithWrappers(a acceptor.Acceptor, wrappers ...Wrapper) acceptor.Acceptor {
	for _, w := range wrappers {
		a = w.Wrap(a)
	}
	return a
}
