package session

import (
	"context"
	"errors"
	"github.com/gotechbook/gotechbook-framework/network"
	"net"
)

type Session interface {
	GetOnCloseCallbacks() []func()
	GetIsFrontend() bool
	GetSubscriptions() interface{}
	SetOnCloseCallbacks(callbacks []func())
	SetIsFrontend(isFrontend bool)
	SetSubscriptions(subscriptions interface{})
	Push(route string, v interface{}) error
	ResponseMID(ctx context.Context, mid uint, v interface{}, err ...bool) error
	ID() int64
	UID() string
	GetData() map[string]interface{}
	SetData(data map[string]interface{}) error
	GetDataEncoded() []byte
	SetDataEncoded(encodedData []byte) error
	SetFrontendData(frontendID string, frontendSessionID int64)
	Bind(ctx context.Context, uid string) error
	Kick(ctx context.Context) error
	OnClose(c func()) error
	Close()
	RemoteAddr() net.Addr
	Remove(key string) error
	Set(key string, value interface{}) error
	HasKey(key string) bool
	Get(key string) interface{}
	Int(key string) int
	Int8(key string) int8
	Int16(key string) int16
	Int32(key string) int32
	Int64(key string) int64
	Uint(key string) uint
	Uint8(key string) uint8
	Uint16(key string) uint16
	Uint32(key string) uint32
	Uint64(key string) uint64
	Float32(key string) float32
	Float64(key string) float64
	String(key string) string
	Value(key string) interface{}
	PushToFront(ctx context.Context) error
	Clear()
	SetHandshakeData(data *HandshakeData)
	GetHandshakeData() *HandshakeData
}

type Pool interface {
	NewSession(entity network.Network, frontend bool, UID ...string) Session
	GetSessionCount() int64
	GetSessionCloseCallbacks() []func(s Session)
	GetSessionByUID(uid string) Session
	GetSessionByID(id int64) Session
	OnSessionBind(f func(ctx context.Context, s Session) error)
	OnAfterSessionBind(f func(ctx context.Context, s Session) error)
	OnSessionClose(f func(s Session))
	CloseAll()
}

type HandshakeClientData struct {
	Platform    string `json:"platform"`
	LibVersion  string `json:"libVersion"`
	BuildNumber string `json:"clientBuildNumber"`
	Version     string `json:"clientVersion"`
}

type HandshakeData struct {
	Sys  HandshakeClientData    `json:"sys"`
	User map[string]interface{} `json:"user,omitempty"`
}

const (
	SessionPushRoute = "sys.pushsession"
	SessionBindRoute = "sys.bindsession"
	KickRoute        = "sys.kick"
)

var (
	ErrIllegalUID                  = errors.New("illegal uid")
	ErrSessionAlreadyBound         = errors.New("session is already bound to an uid")
	ErrOnCloseBackend              = errors.New("onclose callbacks are not allowed on backend servers")
	ErrFrontSessionCantPushToFront = errors.New("frontend session can't push to front")
)
