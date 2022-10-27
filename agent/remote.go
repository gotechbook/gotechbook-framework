package agent

import (
	"context"
	protobuf "github.com/golang/protobuf/proto"
	acceptor "github.com/gotechbook/gotechbook-framework-acceptor"
	encoding "github.com/gotechbook/gotechbook-framework-encoding"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	"github.com/gotechbook/gotechbook-framework-net/constants"
	"github.com/gotechbook/gotechbook-framework-proto/proto"
	"github.com/gotechbook/gotechbook-framework/discovery"
	"github.com/gotechbook/gotechbook-framework/grpc"
	"github.com/gotechbook/gotechbook-framework/message"
	"github.com/gotechbook/gotechbook-framework/modules"
	"github.com/gotechbook/gotechbook-framework/network"
	"github.com/gotechbook/gotechbook-framework/route"
	"github.com/gotechbook/gotechbook-framework/session"
	"net"
)

var _ network.Network = (*Remote)(nil)

type Remote struct {
	Session          session.Session
	chDie            chan struct{}
	messageEncoder   message.Encoder
	codec            acceptor.Codec
	frontendID       string
	reply            string
	rpcClient        grpc.RPCClient
	encoding         encoding.Encoding
	serviceDiscovery discovery.Discovery
}

func NewRemote(sess *proto.Session, reply string, rpcClient grpc.RPCClient, codec acceptor.Codec, encoding encoding.Encoding, serviceDiscovery discovery.Discovery, frontendID string,
	messageEncoder message.Encoder, sessionPool session.Pool) (*Remote, error) {
	a := &Remote{
		chDie:            make(chan struct{}),
		reply:            reply,
		encoding:         encoding,
		codec:            codec,
		rpcClient:        rpcClient,
		serviceDiscovery: serviceDiscovery,
		frontendID:       frontendID,
		messageEncoder:   messageEncoder,
	}

	// binding session
	s := sessionPool.NewSession(a, false, sess.GetUid())
	s.SetFrontendData(frontendID, sess.GetId())
	err := s.SetDataEncoded(sess.GetData())
	if err != nil {
		return nil, err
	}
	a.Session = s
	return a, nil
}

func (a *Remote) Kick(ctx context.Context) error {
	if a.Session.UID() == "" {
		return ErrNoUIDBind
	}
	b, err := protobuf.Marshal(&proto.KickMsg{
		UserId: a.Session.UID(),
	})
	if err != nil {
		return err
	}
	_, err = a.SendRequest(ctx, a.frontendID, session.KickRoute, b)
	return err
}
func (a *Remote) Push(route string, v interface{}) error {
	switch d := v.(type) {
	case []byte:
		logger.Log.Debugf("Type=Push, ID=%d, UID=%s, Route=%s, Data=%d bytes", a.Session.ID(), a.Session.UID(), route, len(d))
	default:
		logger.Log.Debugf("Type=Push, ID=%d, UID=%s, Route=%s, Data=%+v", a.Session.ID(), a.Session.UID(), route, v)
	}
	sv, err := a.serviceDiscovery.GetServer(a.frontendID)
	if err != nil {
		return err
	}
	return a.sendPush(
		pendingMessage{typ: message.Push, route: route, payload: v},
		a.Session.UID(), sv,
	)
}
func (a *Remote) ResponseMID(ctx context.Context, mid uint, v interface{}, isError ...bool) error {
	err := false
	if len(isError) > 0 {
		err = isError[0]
	}
	if mid <= 0 {
		return constants.ErrSessionOnNotify
	}
	switch d := v.(type) {
	case []byte:
		logger.Log.Debugf("Type=Response, ID=%d, MID=%d, Data=%d bytes", a.Session.ID(), mid, len(d))
	default:
		logger.Log.Infof("Type=Response, ID=%d, MID=%d, Data=%+v", a.Session.ID(), mid, v)
	}

	return a.send(pendingMessage{ctx: ctx, typ: message.Response, mid: mid, payload: v, err: err}, a.reply)
}
func (a *Remote) Close() error         { return nil }
func (a *Remote) RemoteAddr() net.Addr { return nil }
func (a *Remote) SendRequest(ctx context.Context, serverID, reqRoute string, v interface{}) (*proto.Response, error) {
	r, err := route.Decode(reqRoute)
	if err != nil {
		return nil, err
	}
	payload, err := marshal(a.encoding, v)
	if err != nil {
		return nil, err
	}
	msg := &message.Message{
		Route: reqRoute,
		Data:  payload,
	}
	server, err := a.serviceDiscovery.GetServer(serverID)
	if err != nil {
		return nil, err
	}
	return a.rpcClient.Call(ctx, proto.RPCType_User, r, nil, msg, server)
}
func (a *Remote) serialize(m pendingMessage) ([]byte, error) {
	payload, err := marshal(a.encoding, m.payload)
	if err != nil {
		return nil, err
	}
	msg := &message.Message{
		Type:  m.typ,
		Data:  payload,
		Route: m.route,
		ID:    m.mid,
		Err:   m.err,
	}
	em, err := a.messageEncoder.Encode(msg)
	if err != nil {
		return nil, err
	}
	p, err := a.codec.Encode(acceptor.Data, em)
	if err != nil {
		return nil, err
	}
	return p, err
}
func (a *Remote) send(m pendingMessage, to string) (err error) {
	p, err := a.serialize(m)
	if err != nil {
		return err
	}
	res := &proto.Response{
		Data: p,
	}
	bt, err := protobuf.Marshal(res)
	if err != nil {
		return err
	}
	return a.rpcClient.Send(to, bt)
}
func (a *Remote) sendPush(m pendingMessage, userID string, sv *modules.Server) (err error) {
	payload, err := marshal(a.encoding, m.payload)
	if err != nil {
		return err
	}
	push := &proto.Push{
		Route: m.route,
		Uid:   a.Session.UID(),
		Data:  payload,
	}
	return a.rpcClient.SendPush(userID, sv, push)
}
