package pool

import (
	"context"
	"encoding/json"
	"github.com/golang/protobuf/proto"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	gProto "github.com/gotechbook/gotechbook-framework-proto/proto"
	"github.com/gotechbook/gotechbook-framework/network"
	"github.com/gotechbook/gotechbook-framework/session"
	"net"
	"sync"
	"sync/atomic"
)

var _ session.Session = (*sessionImpl)(nil)

type sessionImpl struct {
	sync.RWMutex                             // protect data
	id                int64                  // session global unique id
	uid               string                 // binding user id
	lastTime          int64                  // last heartbeat time
	entity            network.Network        // low-level network entity
	data              map[string]interface{} // session data store
	handshakeData     *session.HandshakeData // handshake data received by the client
	encodedData       []byte                 // session data encoded as a byte array
	OnCloseCallbacks  []func()               //onClose callbacks
	IsFrontend        bool                   // if session is a frontend session
	frontendID        string                 // the id of the frontend that owns the session
	frontendSessionID int64                  // the id of the session on the frontend server
	Subscriptions     interface{}            // subscription created on bind when using nats rpc server
	pool              *sessionPoolImpl
}

func (s *sessionImpl) GetOnCloseCallbacks() []func() {
	return s.OnCloseCallbacks
}
func (s *sessionImpl) GetIsFrontend() bool {
	return s.IsFrontend
}
func (s *sessionImpl) GetSubscriptions() interface{} {
	return s.Subscriptions
}
func (s *sessionImpl) SetOnCloseCallbacks(callbacks []func()) {
	s.OnCloseCallbacks = callbacks
}
func (s *sessionImpl) SetIsFrontend(isFrontend bool) {
	s.IsFrontend = isFrontend
}
func (s *sessionImpl) SetSubscriptions(subscriptions interface{}) {
	s.Subscriptions = subscriptions
}
func (s *sessionImpl) Push(route string, v interface{}) error {
	return s.entity.Push(route, v)
}
func (s *sessionImpl) ResponseMID(ctx context.Context, mid uint, v interface{}, err ...bool) error {
	return s.entity.ResponseMID(ctx, mid, v, err...)
}
func (s *sessionImpl) ID() int64 {
	return s.id
}
func (s *sessionImpl) UID() string {
	return s.uid
}
func (s *sessionImpl) GetData() map[string]interface{} {
	s.RLock()
	defer s.RUnlock()

	return s.data
}
func (s *sessionImpl) SetData(data map[string]interface{}) error {
	s.Lock()
	defer s.Unlock()

	s.data = data
	return s.updateEncodedData()
}
func (s *sessionImpl) GetDataEncoded() []byte {
	return s.encodedData
}
func (s *sessionImpl) SetDataEncoded(encodedData []byte) error {
	if len(encodedData) == 0 {
		return nil
	}
	var data map[string]interface{}
	err := json.Unmarshal(encodedData, &data)
	if err != nil {
		return err
	}
	return s.SetData(data)
}
func (s *sessionImpl) SetFrontendData(frontendID string, frontendSessionID int64) {
	s.frontendID = frontendID
	s.frontendSessionID = frontendSessionID
}
func (s *sessionImpl) Bind(ctx context.Context, uid string) error {
	if uid == "" {
		return session.ErrIllegalUID
	}

	if s.UID() != "" {
		return session.ErrSessionAlreadyBound
	}

	s.uid = uid
	for _, cb := range s.pool.sessionBindCallbacks {
		err := cb(ctx, s)
		if err != nil {
			s.uid = ""
			return err
		}
	}

	for _, cb := range s.pool.afterBindCallbacks {
		err := cb(ctx, s)
		if err != nil {
			s.uid = ""
			return err
		}
	}

	// if code running on frontend server
	if s.IsFrontend {
		s.pool.sessionsByUID.Store(uid, s)
	} else {
		// If frontentID is set this means it is a remote call and the current server
		// is not the frontend server that received the user request
		err := s.bindInFront(ctx)
		if err != nil {
			logger.Log.Error("error while trying to push session to front: ", err)
			s.uid = ""
			return err
		}
	}
	return nil
}
func (s *sessionImpl) Kick(ctx context.Context) error {
	err := s.entity.Kick(ctx)
	if err != nil {
		return err
	}
	return s.entity.Close()
}
func (s *sessionImpl) OnClose(c func()) error {
	if !s.IsFrontend {
		return session.ErrOnCloseBackend
	}
	s.OnCloseCallbacks = append(s.OnCloseCallbacks, c)
	return nil
}
func (s *sessionImpl) Close() {
	atomic.AddInt64(&s.pool.SessionCount, -1)
	s.pool.sessionsByID.Delete(s.ID())
	s.pool.sessionsByUID.Delete(s.UID())
	// TODO: this logic should be moved to nats rpc server
	//if s.IsFrontend && s.Subscriptions != nil && len(s.Subscriptions) > 0 {
	//	// if the user is bound to an userid and nats rpc server is being used we need to unsubscribe
	//	for _, sub := range s.Subscriptions {
	//		err := sub.Unsubscribe()
	//		if err != nil {
	//			logger.Log.Errorf("error unsubscribing to user's messages channel: %s, this can cause performance and leak issues", err.Error())
	//		} else {
	//			logger.Log.Debugf("successfully unsubscribed to user's %s messages channel", s.UID())
	//		}
	//	}
	//}
	s.entity.Close()
}
func (s *sessionImpl) RemoteAddr() net.Addr {
	return s.entity.RemoteAddr()
}
func (s *sessionImpl) Remove(key string) error {
	s.Lock()
	defer s.Unlock()

	delete(s.data, key)
	return s.updateEncodedData()
}
func (s *sessionImpl) Set(key string, value interface{}) error {
	s.Lock()
	defer s.Unlock()

	s.data[key] = value
	return s.updateEncodedData()
}
func (s *sessionImpl) HasKey(key string) bool {
	s.RLock()
	defer s.RUnlock()

	_, has := s.data[key]
	return has
}
func (s *sessionImpl) Get(key string) interface{} {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return nil
	}
	return v
}
func (s *sessionImpl) Int(key string) int {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Int8(key string) int8 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int8)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Int16(key string) int16 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int16)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Int32(key string) int32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int32)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Int64(key string) int64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(int64)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Uint(key string) uint {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Uint8(key string) uint8 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint8)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Uint16(key string) uint16 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint16)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Uint32(key string) uint32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint32)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Uint64(key string) uint64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(uint64)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Float32(key string) float32 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(float32)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) Float64(key string) float64 {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return 0
	}

	value, ok := v.(float64)
	if !ok {
		return 0
	}
	return value
}
func (s *sessionImpl) String(key string) string {
	s.RLock()
	defer s.RUnlock()

	v, ok := s.data[key]
	if !ok {
		return ""
	}

	value, ok := v.(string)
	if !ok {
		return ""
	}
	return value
}
func (s *sessionImpl) Value(key string) interface{} {
	s.RLock()
	defer s.RUnlock()

	return s.data[key]
}
func (s *sessionImpl) PushToFront(ctx context.Context) error {
	if s.IsFrontend {
		return session.ErrFrontSessionCantPushToFront
	}
	return s.sendRequestToFront(ctx, session.SessionPushRoute, true)
}
func (s *sessionImpl) Clear() {
	s.Lock()
	defer s.Unlock()

	s.uid = ""
	s.data = map[string]interface{}{}
	s.updateEncodedData()
}
func (s *sessionImpl) SetHandshakeData(data *session.HandshakeData) {
	s.Lock()
	defer s.Unlock()

	s.handshakeData = data
}
func (s *sessionImpl) GetHandshakeData() *session.HandshakeData {
	return s.handshakeData
}
func (s *sessionImpl) sendRequestToFront(ctx context.Context, route string, includeData bool) error {
	sessionData := &gProto.Session{
		Id:  s.frontendSessionID,
		Uid: s.uid,
	}
	if includeData {
		sessionData.Data = s.encodedData
	}
	b, err := proto.Marshal(sessionData)
	if err != nil {
		return err
	}
	res, err := s.entity.SendRequest(ctx, s.frontendID, route, b)
	if err != nil {
		return err
	}
	logger.Log.Debugf("%s Got response: %+v", route, res)
	return nil
}
func (s *sessionImpl) bindInFront(ctx context.Context) error {
	return s.sendRequestToFront(ctx, session.SessionBindRoute, false)
}
func (s *sessionImpl) updateEncodedData() error {
	var b []byte
	b, err := json.Marshal(s.data)
	if err != nil {
		return err
	}
	s.encodedData = b
	return nil
}
