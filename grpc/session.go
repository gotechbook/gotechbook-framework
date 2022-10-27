package grpc

import (
	"context"
	"github.com/gotechbook/gotechbook-framework/modules"
	"github.com/gotechbook/gotechbook-framework/session"
)

type UniqueSession struct {
	modules.Base
	server      *modules.Server
	rpcClient   RPCClient
	sessionPool session.Pool
}

func NewUniqueSession(server *modules.Server, rpcServer RPCServer, rpcClient RPCClient, sessionPool session.Pool) *UniqueSession {
	return &UniqueSession{
		server:      server,
		rpcClient:   rpcClient,
		sessionPool: sessionPool,
	}
}

func (u *UniqueSession) OnUserBind(uid, fid string) {
	if u.server.ID == fid {
		return
	}
	oldSession := u.sessionPool.GetSessionByUID(uid)
	if oldSession != nil {
		oldSession.Kick(context.Background())
	}
}
func (u *UniqueSession) Init() error {
	u.sessionPool.OnSessionBind(func(ctx context.Context, s session.Session) error {
		oldSession := u.sessionPool.GetSessionByUID(s.UID())
		if oldSession != nil {
			return oldSession.Kick(ctx)
		}
		err := u.rpcClient.BroadcastSessionBind(s.UID())
		return err
	})
	return nil
}
