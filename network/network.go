package network

import (
	"context"
	"github.com/gotechbook/gotechbook-framework-proto/proto"
	"net"
)

type Network interface {
	Push(route string, v interface{}) error
	ResponseMID(ctx context.Context, mid uint, v interface{}, isError ...bool) error
	Close() error
	Kick(ctx context.Context) error
	RemoteAddr() net.Addr
	SendRequest(ctx context.Context, serverID, route string, v interface{}) (*proto.Response, error)
}
