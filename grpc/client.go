package grpc

import (
	"context"
	"fmt"
	config "github.com/gotechbook/gotechbook-framework-config"
	gContext "github.com/gotechbook/gotechbook-framework-context"
	errors "github.com/gotechbook/gotechbook-framework-errors"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	metrics "github.com/gotechbook/gotechbook-framework-metrics"
	"github.com/gotechbook/gotechbook-framework-proto/proto"
	tracing "github.com/gotechbook/gotechbook-framework-tracing"
	"github.com/gotechbook/gotechbook-framework/message"
	"github.com/gotechbook/gotechbook-framework/modules"
	"github.com/gotechbook/gotechbook-framework/route"
	"github.com/gotechbook/gotechbook-framework/session"
	"github.com/gotechbook/gotechbook-framework/storage"
	"github.com/opentracing/opentracing-go"
	"google.golang.org/grpc"
	"sync"
	"time"
)

var _ RPCClient = (*GClient)(nil)

var _ SDListener = (*GClient)(nil)

type GClient struct {
	bindingStorage   storage.BindingStorage
	clientMap        sync.Map
	dialTimeout      time.Duration
	infoRetriever    InfoRetriever
	lazy             bool
	metricsReporters []metrics.Reporter
	reqTimeout       time.Duration
	server           *modules.Server
}

func (g *GClient) AddServer(server *modules.Server) {
	var host, port, portKey string
	var ok bool

	host, portKey = g.getServerHost(server)
	if host == "" {
		logger.Log.Errorf("[grpc client] server %s has no grpcHost specified in metadata", server.ID)
		return
	}

	if port, ok = server.Metadata[portKey]; !ok {
		logger.Log.Errorf("[grpc client] server %s has no %s specified in metadata", server.ID, portKey)
		return
	}

	address := fmt.Sprintf("%s:%s", host, port)
	client := &grpcClient{address: address}
	if !g.lazy {
		if err := client.connect(); err != nil {
			logger.Log.Errorf("[grpc client] unable to connect to server %s at %s: %v", server.ID, address, err)
		}
	}
	g.clientMap.Store(server.ID, client)
	logger.Log.Debugf("[grpc client] added server %s at %s", server.ID, address)
}
func (g *GClient) RemoveServer(server *modules.Server) {
	if c, ok := g.clientMap.Load(server.ID); ok {
		c.(*grpcClient).disconnect()
		g.clientMap.Delete(server.ID)
		logger.Log.Debugf("[grpc client] removed server %s", server.ID)
	}
}
func (g *GClient) Send(route string, data []byte) error {
	return ErrNotImplemented
}
func (g *GClient) SendPush(userID string, frontendSv *modules.Server, push *proto.Push) error {
	var svID string
	var err error
	if frontendSv.ID != "" {
		svID = frontendSv.ID
	} else {
		if g.bindingStorage == nil {
			return ErrNoBindingStorageModule
		}
		svID, err = g.bindingStorage.GetUserFrontendID(userID, frontendSv.Type)
		if err != nil {
			return err
		}
	}
	if c, ok := g.clientMap.Load(svID); ok {
		ctxT, done := context.WithTimeout(context.Background(), g.reqTimeout)
		defer done()
		err := c.(*grpcClient).pushToUser(ctxT, push)
		return err
	}
	return ErrNoConnectionToServer
}
func (g *GClient) SendKick(userID string, serverType string, kick *proto.KickMsg) error {
	var svID string
	var err error

	if g.bindingStorage == nil {
		return ErrNoBindingStorageModule
	}

	svID, err = g.bindingStorage.GetUserFrontendID(userID, serverType)
	if err != nil {
		return err
	}

	if c, ok := g.clientMap.Load(svID); ok {
		ctxT, done := context.WithTimeout(context.Background(), g.reqTimeout)
		defer done()
		err := c.(*grpcClient).sendKick(ctxT, kick)
		return err
	}
	return ErrNoConnectionToServer
}
func (g *GClient) BroadcastSessionBind(uid string) error {
	if g.bindingStorage == nil {
		return ErrNoBindingStorageModule
	}
	fid, _ := g.bindingStorage.GetUserFrontendID(uid, g.server.Type)
	if fid != "" {
		if c, ok := g.clientMap.Load(fid); ok {
			msg := &proto.BindMsg{
				Uid: uid,
				Fid: g.server.ID,
			}
			ctxT, done := context.WithTimeout(context.Background(), g.reqTimeout)
			defer done()
			err := c.(*grpcClient).sessionBindRemote(ctxT, msg)
			return err
		}
	}
	return nil
}
func (g *GClient) Call(ctx context.Context, rpcType proto.RPCType, route *route.Route, session session.Session, msg *message.Message, server *modules.Server) (*proto.Response, error) {
	c, ok := g.clientMap.Load(server.ID)
	if !ok {
		return nil, ErrNoConnectionToServer
	}
	parent, err := tracing.ExtractSpan(ctx)
	if err != nil {
		logger.Log.Warnf("[grpc client] failed to retrieve parent span: %s", err.Error())
	}
	tags := opentracing.Tags{
		"span.kind":       "client",
		"local.id":        g.server.ID,
		"peer.serverType": server.Type,
		"peer.id":         server.ID,
	}
	ctx = tracing.StartSpan(ctx, "GRPC RPC Call", tags, parent)
	defer tracing.FinishSpan(ctx, err)
	req, err := build(ctx, rpcType, route, session, msg, g.server)
	if err != nil {
		return nil, err
	}
	ctxT, done := context.WithTimeout(ctx, g.reqTimeout)
	defer done()
	if g.metricsReporters != nil {
		startTime := time.Now()
		ctxT = gContext.AddToPropagateCtx(ctxT, StartTimeKey, startTime.UnixNano())
		ctxT = gContext.AddToPropagateCtx(ctxT, RouteKey, route.String())
		defer metrics.ReportTimingFromCtx(ctxT, g.metricsReporters, "rpc", err)
	}
	res, err := c.(*grpcClient).call(ctxT, &req)
	if err != nil {
		return nil, err
	}
	if res.Error != nil {
		if res.Error.Code == "" {
			res.Error.Code = errors.ErrUnknownCode
		}
		err = &errors.Error{
			Code:     res.Error.Code,
			Msg:      res.Error.Msg,
			Metadata: res.Error.Metadata,
		}
		return nil, err
	}
	return res, nil
}
func (g *GClient) Init() error {
	return nil
}
func (g *GClient) AfterInit() {
}
func (g *GClient) BeforeShutdown() {
}
func (g *GClient) Shutdown() error {
	return nil
}
func (g *GClient) getServerHost(sv *modules.Server) (host, portKey string) {
	var (
		serverRegion, hasRegion   = sv.Metadata[RegionKey]
		externalHost, hasExternal = sv.Metadata[GRPCExternalHostKey]
		internalHost, _           = sv.Metadata[GRPCHostKey]
	)
	hasRegion = hasRegion && serverRegion != ""
	hasExternal = hasExternal && externalHost != ""

	if !hasRegion {
		if hasExternal {
			logger.Log.Warnf("[grpc client] server %s has no region specified in metadata, using external host", sv.ID)
			return externalHost, GRPCExternalPortKey
		}
		logger.Log.Warnf("[grpc client] server %s has no region nor external host specified in metadata, using internal host", sv.ID)
		return internalHost, GRPCPortKey
	}
	if g.infoRetriever.Region() == serverRegion || !hasExternal {
		logger.Log.Infof("[grpc client] server %s is in same region or external host not provided, using internal host", sv.ID)
		return internalHost, GRPCPortKey
	}
	logger.Log.Infof("[grpc client] server %s is in other region, using external host", sv.ID)
	return externalHost, GRPCExternalPortKey
}
func NewGClient(config config.Cluster, server *modules.Server, metricsReporters []metrics.Reporter, bindingStorage storage.BindingStorage, infoRetriever InfoRetriever) (*GClient, error) {
	g := &GClient{
		bindingStorage:   bindingStorage,
		infoRetriever:    infoRetriever,
		metricsReporters: metricsReporters,
		server:           server,
	}
	g.dialTimeout = config.GoTechBookFrameworkClusterRpcClientGrpcDialTimeout
	g.lazy = config.GoTechBookFrameworkClusterRpcClientGrpcLazyConnection
	g.reqTimeout = config.GoTechBookFrameworkClusterRpcClientGrpcRequestTimeout
	return g, nil
}

type grpcClient struct {
	address   string
	cli       proto.AppClient
	conn      *grpc.ClientConn
	connected bool
	lock      sync.Mutex
}

func (gc *grpcClient) connect() error {
	gc.lock.Lock()
	defer gc.lock.Unlock()
	if gc.connected {
		return nil
	}
	conn, err := grpc.Dial(
		gc.address,
		grpc.WithInsecure(),
	)
	if err != nil {
		return err
	}
	c := proto.NewAppClient(conn)
	gc.cli = c
	gc.conn = conn
	gc.connected = true
	return nil
}
func (gc *grpcClient) disconnect() {
	gc.lock.Lock()
	if gc.connected {
		gc.conn.Close()
		gc.connected = false
	}
	gc.lock.Unlock()
}
func (gc *grpcClient) pushToUser(ctx context.Context, push *proto.Push) error {
	if !gc.connected {
		if err := gc.connect(); err != nil {
			return err
		}
	}
	_, err := gc.cli.PushToUser(ctx, push)
	return err
}
func (gc *grpcClient) call(ctx context.Context, req *proto.Request) (*proto.Response, error) {
	if !gc.connected {
		if err := gc.connect(); err != nil {
			return nil, err
		}
	}
	return gc.cli.Call(ctx, req)
}
func (gc *grpcClient) sessionBindRemote(ctx context.Context, req *proto.BindMsg) error {
	if !gc.connected {
		if err := gc.connect(); err != nil {
			return err
		}
	}
	_, err := gc.cli.SessionBindRemote(ctx, req)
	return err
}
func (gc *grpcClient) sendKick(ctx context.Context, req *proto.KickMsg) error {
	if !gc.connected {
		if err := gc.connect(); err != nil {
			return err
		}
	}
	_, err := gc.cli.KickUser(ctx, req)
	return err
}

func build(ctx context.Context, rpcType proto.RPCType, route *route.Route, session session.Session, msg *message.Message, thisServer *modules.Server) (proto.Request, error) {
	req := proto.Request{
		Type: rpcType,
		Msg: &proto.Msg{
			Route: route.String(),
			Data:  msg.Data,
		},
	}
	ctx, err := tracing.InjectSpan(ctx)
	if err != nil {
		logger.Log.Errorf("failed to inject span: %s", err)
	}
	ctx = gContext.AddToPropagateCtx(ctx, PeerIDKey, thisServer.ID)
	ctx = gContext.AddToPropagateCtx(ctx, PeerServiceKey, thisServer.Type)
	req.Metadata, err = gContext.Encode(ctx)
	if err != nil {
		return req, err
	}
	if thisServer.Frontend {
		req.FrontendID = thisServer.ID
	}
	switch msg.Type {
	case message.Request:
		req.Msg.Type = proto.MsgType_MsgRequest
	case message.Notify:
		req.Msg.Type = proto.MsgType_MsgNotify
	}
	if rpcType == proto.RPCType_Sys {
		mid := uint(0)
		if msg.Type == message.Request {
			mid = msg.ID
		}
		req.Msg.Id = uint64(mid)
		req.Session = &proto.Session{
			Id:   session.ID(),
			Uid:  session.UID(),
			Data: session.GetDataEncoded(),
		}
	}
	return req, nil
}
