package grpc

import (
	"context"
	"errors"
	"github.com/gotechbook/gotechbook-framework-proto/proto"
	"github.com/gotechbook/gotechbook-framework/message"
	"github.com/gotechbook/gotechbook-framework/modules"
	"github.com/gotechbook/gotechbook-framework/route"
	"github.com/gotechbook/gotechbook-framework/session"
)

var (
	ErrNotImplemented         = errors.New("method not implemented")
	ErrNoBindingStorageModule = errors.New("for sending remote pushes or using unique session module while using grpc you need to pass it a BindingStorage")
	ErrNoConnectionToServer   = errors.New("rpc client has no connection to the chosen server")
)
var (
	StartTimeKey        = "req-start-time"
	RouteKey            = "req-route"
	PeerIDKey           = "peer.id"
	PeerServiceKey      = "peer.service"
	RegionKey           = "region"
	GRPCExternalHostKey = "grpc-external-host"
	GRPCHostKey         = "grpcHost"
	GRPCExternalPortKey = "grpc-external-port"
	GRPCPortKey         = "grpcPort"
)

type RPCClient interface {
	Send(route string, data []byte) error
	SendPush(userID string, frontendSv *modules.Server, push *proto.Push) error
	SendKick(userID string, serverType string, kick *proto.KickMsg) error
	BroadcastSessionBind(uid string) error
	Call(ctx context.Context, rpcType proto.RPCType, route *route.Route, session session.Session, msg *message.Message, server *modules.Server) (*proto.Response, error)
	modules.Module
}

type RPCServer interface {
	SetServer(server proto.AppServer)
	modules.Module
}
