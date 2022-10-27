package session

import (
	"context"
)

var DefaultSessionPool Pool

func GetSessionByUID(uid string) Session {
	return DefaultSessionPool.GetSessionByUID(uid)
}

func GetSessionByID(id int64) Session {
	return DefaultSessionPool.GetSessionByID(id)
}

func OnSessionBind(f func(ctx context.Context, s Session) error) {
	DefaultSessionPool.OnSessionBind(f)
}

// OnAfterSessionBind adds a method to be called when session is bound and after all sessionBind callbacks
func OnAfterSessionBind(f func(ctx context.Context, s Session) error) {
	DefaultSessionPool.OnAfterSessionBind(f)
}

// OnSessionClose adds a method that will be called when every session closes
func OnSessionClose(f func(s Session)) {
	DefaultSessionPool.OnSessionClose(f)
}

// CloseAll calls Close on all sessions
func CloseAll() {
	DefaultSessionPool.CloseAll()
}
