package pool

import (
	"context"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"github.com/gotechbook/gotechbook-framework/network"
	"github.com/gotechbook/gotechbook-framework/session"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

var _ session.Pool = (*sessionPoolImpl)(nil)

func NewPool() session.Pool {
	return &sessionPoolImpl{
		sessionBindCallbacks:  make([]func(ctx context.Context, s session.Session) error, 0),
		afterBindCallbacks:    make([]func(ctx context.Context, s session.Session) error, 0),
		SessionCloseCallbacks: make([]func(s session.Session), 0),
		sessionIDSvc:          newSessionIDService(),
	}
}

type sessionPoolImpl struct {
	sessionBindCallbacks  []func(ctx context.Context, s session.Session) error
	afterBindCallbacks    []func(ctx context.Context, s session.Session) error
	SessionCloseCallbacks []func(s session.Session)
	sessionsByUID         sync.Map
	sessionsByID          sync.Map
	sessionIDSvc          *sessionIDService
	SessionCount          int64
}

func (pool *sessionPoolImpl) NewSession(entity network.Network, frontend bool, UID ...string) session.Session {
	s := &sessionImpl{
		id:               pool.sessionIDSvc.sessionID(),
		entity:           entity,
		data:             make(map[string]interface{}),
		handshakeData:    nil,
		lastTime:         time.Now().Unix(),
		OnCloseCallbacks: []func(){},
		IsFrontend:       frontend,
		pool:             pool,
	}
	if frontend {
		pool.sessionsByID.Store(s.id, s)
		atomic.AddInt64(&pool.SessionCount, 1)
	}
	if len(UID) > 0 {
		s.uid = UID[0]
	}
	return s
}
func (pool *sessionPoolImpl) GetSessionCount() int64 {
	return pool.SessionCount
}
func (pool *sessionPoolImpl) GetSessionCloseCallbacks() []func(s session.Session) {
	return pool.SessionCloseCallbacks
}
func (pool *sessionPoolImpl) GetSessionByUID(uid string) session.Session {
	// TODO: Block this operation in backend servers
	if val, ok := pool.sessionsByUID.Load(uid); ok {
		return val.(session.Session)
	}
	return nil
}
func (pool *sessionPoolImpl) GetSessionByID(id int64) session.Session {
	// TODO: Block this operation in backend servers
	if val, ok := pool.sessionsByID.Load(id); ok {
		return val.(session.Session)
	}
	return nil
}
func (pool *sessionPoolImpl) OnSessionBind(f func(ctx context.Context, s session.Session) error) {
	// Prevents the same function to be added twice in onSessionBind
	sf1 := reflect.ValueOf(f)
	for _, fun := range pool.sessionBindCallbacks {
		sf2 := reflect.ValueOf(fun)
		if sf1.Pointer() == sf2.Pointer() {
			return
		}
	}
	pool.sessionBindCallbacks = append(pool.sessionBindCallbacks, f)
}
func (pool *sessionPoolImpl) OnAfterSessionBind(f func(ctx context.Context, s session.Session) error) {
	// Prevents the same function to be added twice in onSessionBind
	sf1 := reflect.ValueOf(f)
	for _, fun := range pool.afterBindCallbacks {
		sf2 := reflect.ValueOf(fun)
		if sf1.Pointer() == sf2.Pointer() {
			return
		}
	}
	pool.afterBindCallbacks = append(pool.afterBindCallbacks, f)
}
func (pool *sessionPoolImpl) OnSessionClose(f func(s session.Session)) {
	sf1 := reflect.ValueOf(f)
	for _, fun := range pool.SessionCloseCallbacks {
		sf2 := reflect.ValueOf(fun)
		if sf1.Pointer() == sf2.Pointer() {
			return
		}
	}
	pool.SessionCloseCallbacks = append(pool.SessionCloseCallbacks, f)
}
func (pool *sessionPoolImpl) CloseAll() {
	logger.Log.Debugf("closing all sessions, %d sessions", pool.SessionCount)
	pool.sessionsByID.Range(func(_, value interface{}) bool {
		s := value.(session.Session)
		s.Close()
		return true
	})
	logger.Log.Debug("finished closing sessions")
}
