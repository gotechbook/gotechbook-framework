package agent

import (
	"context"
	"encoding/json"
	e "errors"
	"fmt"
	acceptor "github.com/gotechbook/gotechbook-framework-acceptor"
	encoding "github.com/gotechbook/gotechbook-framework-encoding"
	errors "github.com/gotechbook/gotechbook-framework-errors"
	logger "github.com/gotechbook/gotechbook-framework-logger"
	metrics "github.com/gotechbook/gotechbook-framework-metrics"
	"github.com/gotechbook/gotechbook-framework-proto/proto"
	tracing "github.com/gotechbook/gotechbook-framework-tracing"
	utils "github.com/gotechbook/gotechbook-framework-utils"
	"github.com/gotechbook/gotechbook-framework/message"
	"github.com/gotechbook/gotechbook-framework/session"
	"github.com/opentracing/opentracing-go"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrBrokenPipe         = e.New("broken low-level pipe")
	ErrCloseClosedSession = e.New("close closed session")
	ErrSessionOnNotify    = e.New("current session working on notify mode")
	ErrNoUIDBind          = e.New("you have to bind an UID to the session to do that")
)

const (
	IPVersionKey = "ipversion"
	IPv4         = "ipv4"
	IPv6         = "ipv6"
)

const (
	_ int32 = iota
	// StatusStart status
	StatusStart
	// StatusHandshake status
	StatusHandshake
	// StatusWorking status
	StatusWorking
	// StatusClosed status
	StatusClosed
)

const handlerType = "handler"

type Factory interface {
	CreateAgent(conn net.Conn) Agent
}
type Agent interface {
	GetSession() session.Session
	Push(route string, v interface{}) error
	ResponseMID(ctx context.Context, mid uint, v interface{}, isError ...bool) error
	Close() error
	RemoteAddr() net.Addr
	String() string
	GetStatus() int32
	Kick(ctx context.Context) error
	SetLastAt()
	SetStatus(state int32)
	Handle()
	IPVersion() string
	SendHandshakeResponse() error
	SendRequest(ctx context.Context, serverID, route string, v interface{}) (*proto.Response, error)
	AnswerWithError(ctx context.Context, mid uint, err error)
}

var _ Agent = (*agentImpl)(nil)

var (
	hbd  []byte
	hrd  []byte
	once sync.Once
)

type agentImpl struct {
	Session            session.Session
	sessionPool        session.Pool
	appDieChan         chan bool
	chDie              chan struct{}
	chSend             chan pendingWrite
	chStopHeartbeat    chan struct{}
	chStopWrite        chan struct{}
	closeMutex         sync.Mutex
	conn               net.Conn
	codec              acceptor.Codec
	heartbeatTimeout   time.Duration
	lastAt             int64
	messageEncoder     message.Encoder
	messagesBufferSize int
	metricsReporters   []metrics.Reporter
	serializer         encoding.Encoding // message serializer
	state              int32             // current agent state
}
type pendingMessage struct {
	ctx     context.Context
	typ     message.Type
	route   string
	mid     uint
	payload interface{}
	err     bool
}
type pendingWrite struct {
	ctx  context.Context
	data []byte
	err  error
}

func (a *agentImpl) GetSession() session.Session {
	return a.Session
}
func (a *agentImpl) Push(route string, v interface{}) error {
	if a.GetStatus() == StatusClosed {
		return errors.New(ErrBrokenPipe, errors.ErrClientClosedRequest)
	}
	switch d := v.(type) {
	case []byte:
		logger.Log.Debugf("Type=Push, ID=%d, UID=%s, Route=%s, Data=%d bytes", a.Session.ID(), a.Session.UID(), route, len(d))
	default:
		logger.Log.Debugf("Type=Push, ID=%d, UID=%s, Route=%s, Data=%+v", a.Session.ID(), a.Session.UID(), route, v)
	}
	return a.send(pendingMessage{typ: message.Push, route: route, payload: v})
}
func (a *agentImpl) ResponseMID(ctx context.Context, mid uint, v interface{}, isError ...bool) error {
	err := false
	if len(isError) > 0 {
		err = isError[0]
	}
	if a.GetStatus() == StatusClosed {
		return errors.New(ErrBrokenPipe, errors.ErrClientClosedRequest)
	}
	if mid <= 0 {
		return ErrSessionOnNotify
	}
	switch d := v.(type) {
	case []byte:
		logger.Log.Debugf("Type=Response, ID=%d, UID=%s, MID=%d, Data=%dbytes",
			a.Session.ID(), a.Session.UID(), mid, len(d))
	default:
		logger.Log.Infof("Type=Response, ID=%d, UID=%s, MID=%d, Data=%+v",
			a.Session.ID(), a.Session.UID(), mid, v)
	}
	return a.send(pendingMessage{ctx: ctx, typ: message.Response, mid: mid, payload: v, err: err})
}
func (a *agentImpl) Close() error {
	a.closeMutex.Lock()
	defer a.closeMutex.Unlock()
	if a.GetStatus() == StatusClosed {
		return ErrCloseClosedSession
	}
	a.SetStatus(StatusClosed)

	logger.Log.Debugf("Session closed, ID=%d, UID=%s, IP=%s", a.Session.ID(), a.Session.UID(), a.conn.RemoteAddr())

	select {
	case <-a.chDie:
		// expect
	default:
		close(a.chStopWrite)
		close(a.chStopHeartbeat)
		close(a.chDie)
		a.onSessionClosed(a.Session)
	}
	metrics.ReportNumberOfConnectedClients(a.metricsReporters, a.sessionPool.GetSessionCount())
	return a.conn.Close()
}
func (a *agentImpl) RemoteAddr() net.Addr {
	return a.conn.RemoteAddr()
}
func (a *agentImpl) String() string {
	return fmt.Sprintf("Remote=%s, LastTime=%d", a.conn.RemoteAddr().String(), atomic.LoadInt64(&a.lastAt))
}
func (a *agentImpl) GetStatus() int32 {
	return atomic.LoadInt32(&a.state)
}
func (a *agentImpl) Kick(ctx context.Context) error {
	// packet encode
	p, err := a.codec.Encode(acceptor.Kick, nil)
	if err != nil {
		return err
	}
	_, err = a.conn.Write(p)
	return err
}
func (a *agentImpl) SetLastAt() {
	atomic.StoreInt64(&a.lastAt, time.Now().Unix())
}
func (a *agentImpl) SetStatus(state int32) {
	atomic.StoreInt32(&a.state, state)
}
func (a *agentImpl) Handle() {
	defer func() {
		a.Close()
		logger.Log.Debugf("Session handle goroutine exit, SessionID=%d, UID=%s", a.Session.ID(), a.Session.UID())
	}()
	go a.write()
	go a.heartbeat()
	<-a.chDie // agent closed signal
}
func (a *agentImpl) IPVersion() string {
	version := IPv4

	ipPort := a.RemoteAddr().String()
	if strings.Count(ipPort, ":") > 1 {
		version = IPv6
	}
	return version
}
func (a *agentImpl) SendHandshakeResponse() error {
	_, err := a.conn.Write(hrd)
	return err
}
func (a *agentImpl) SendRequest(ctx context.Context, serverID, route string, v interface{}) (*proto.Response, error) {
	return nil, nil
}
func (a *agentImpl) AnswerWithError(ctx context.Context, mid uint, err error) {
	var e error
	defer func() {
		if e != nil {
			tracing.FinishSpan(ctx, e)
			metrics.ReportTimingFromCtx(ctx, a.metricsReporters, handlerType, e)
		}
	}()
	if ctx != nil && err != nil {
		s := opentracing.SpanFromContext(ctx)
		if s != nil {
			tracing.LogError(s, err.Error())
		}
	}
	p, e := GetErrorPayload(a.serializer, err)
	if e != nil {
		logger.Log.Errorf("error answering the user with an error: %s", e.Error())
		return
	}
	e = a.Session.ResponseMID(ctx, mid, p, true)
	if e != nil {
		logger.Log.Errorf("error answering the user with an error: %s", e.Error())
	}
}
func (a *agentImpl) send(pendingMsg pendingMessage) (err error) {
	defer func() {
		if e := recover(); e != nil {
			err = errors.New(ErrBrokenPipe, errors.ErrClientClosedRequest)
		}
	}()
	a.reportChannelSize()
	m, err := a.getMessageFromPendingMessage(pendingMsg)
	if err != nil {
		return err
	}
	p, err := a.packetEncodeMessage(m)
	if err != nil {
		return err
	}

	pWrite := pendingWrite{
		ctx:  pendingMsg.ctx,
		data: p,
	}

	if pendingMsg.err {
		pWrite.err = GetErrorFromPayload(a.serializer, m.Data)
	}
	select {
	case a.chSend <- pWrite:
	case <-a.chDie:
	}
	return
}
func (a *agentImpl) reportChannelSize() {
	chSendCapacity := a.messagesBufferSize - len(a.chSend)
	if chSendCapacity == 0 {
		logger.Log.Warnf("chSend is at maximum capacity")
	}
	for _, mr := range a.metricsReporters {
		if err := mr.ReportGauge(metrics.ChannelCapacity, map[string]string{"channel": "agent_chsend"}, float64(chSendCapacity)); err != nil {
			logger.Log.Warnf("failed to report chSend channel : %s", err.Error())
		}
	}
}
func (a *agentImpl) getMessageFromPendingMessage(pm pendingMessage) (*message.Message, error) {
	payload, err := marshal(a.serializer, pm.payload)
	if err != nil {
		payload, err = GetErrorPayload(a.serializer, err)
		if err != nil {
			return nil, err
		}
	}
	m := &message.Message{
		Type:  pm.typ,
		Data:  payload,
		Route: pm.route,
		ID:    pm.mid,
		Err:   pm.err,
	}
	return m, nil
}
func (a *agentImpl) packetEncodeMessage(m *message.Message) ([]byte, error) {
	em, err := a.messageEncoder.Encode(m)
	if err != nil {
		return nil, err
	}
	p, err := a.codec.Encode(acceptor.Data, em)
	if err != nil {
		return nil, err
	}
	return p, nil
}
func (a *agentImpl) onSessionClosed(s session.Session) {
	defer func() {
		if err := recover(); err != nil {
			logger.Log.Errorf("go-tech-book-framework/onSessionClosed: %v", err)
		}
	}()
	for _, fn1 := range s.GetOnCloseCallbacks() {
		fn1()
	}
	for _, fn2 := range a.sessionPool.GetSessionCloseCallbacks() {
		fn2(s)
	}
}
func (a *agentImpl) write() {
	// clean func
	defer func() {
		a.Close()
	}()

	for {
		select {
		case pWrite := <-a.chSend:
			// close agent if low-level Conn broken
			if _, err := a.conn.Write(pWrite.data); err != nil {
				tracing.FinishSpan(pWrite.ctx, err)
				metrics.ReportTimingFromCtx(pWrite.ctx, a.metricsReporters, handlerType, err)
				logger.Log.Errorf("Failed to write in conn: %s", err.Error())
				return
			}
			var e error
			tracing.FinishSpan(pWrite.ctx, e)
			metrics.ReportTimingFromCtx(pWrite.ctx, a.metricsReporters, handlerType, pWrite.err)
		case <-a.chStopWrite:
			return
		}
	}
}
func (a *agentImpl) heartbeat() {
	ticker := time.NewTicker(a.heartbeatTimeout)
	defer func() {
		ticker.Stop()
		a.Close()
	}()

	for {
		select {
		case <-ticker.C:
			deadline := time.Now().Add(-2 * a.heartbeatTimeout).Unix()
			if atomic.LoadInt64(&a.lastAt) < deadline {
				logger.Log.Debugf("Session heartbeat timeout, LastTime=%d, Deadline=%d", atomic.LoadInt64(&a.lastAt), deadline)
				return
			}
			// chSend is never closed so we need this to don't block if agent is already closed
			select {
			case a.chSend <- pendingWrite{data: hbd}:
			case <-a.chDie:
				return
			case <-a.chStopHeartbeat:
				return
			}
		case <-a.chDie:
			return
		case <-a.chStopHeartbeat:
			return
		}
	}
}

var _ Factory = (*agentFactoryImpl)(nil)

type agentFactoryImpl struct {
	sessionPool        session.Pool
	appDieChan         chan bool
	codec              acceptor.Codec
	heartbeatTimeout   time.Duration
	messageEncoder     message.Encoder
	messagesBufferSize int
	metricsReporters   []metrics.Reporter
	serializer         encoding.Encoding
}

func (a *agentFactoryImpl) CreateAgent(conn net.Conn) Agent {
	return newAgent(conn, a.codec, a.serializer, a.heartbeatTimeout, a.messagesBufferSize, a.appDieChan, a.messageEncoder, a.metricsReporters, a.sessionPool)
}
func newAgent(conn net.Conn, codec acceptor.Codec, encoding encoding.Encoding, heartbeatTime time.Duration, messagesBufferSize int, dieChan chan bool, messageEncoder message.Encoder, metricsReporters []metrics.Reporter, sessionPool session.Pool) Agent {
	encodingName := encoding.Name()
	once.Do(func() {
		hbdEncode(heartbeatTime, codec, messageEncoder.IsCompressionEnabled(), encodingName)
	})
	a := &agentImpl{
		appDieChan:         dieChan,
		chDie:              make(chan struct{}),
		chSend:             make(chan pendingWrite, messagesBufferSize),
		chStopHeartbeat:    make(chan struct{}),
		chStopWrite:        make(chan struct{}),
		messagesBufferSize: messagesBufferSize,
		conn:               conn,
		codec:              codec,
		heartbeatTimeout:   heartbeatTime,
		lastAt:             time.Now().Unix(),
		serializer:         encoding,
		state:              StatusStart,
		messageEncoder:     messageEncoder,
		metricsReporters:   metricsReporters,
		sessionPool:        sessionPool,
	}
	// binding session
	s := sessionPool.NewSession(a, true)
	metrics.ReportNumberOfConnectedClients(metricsReporters, sessionPool.GetSessionCount())
	a.Session = s
	return a
}
func NewFactory(appDieChan chan bool, codec acceptor.Codec, serializer encoding.Encoding, heartbeatTimeout time.Duration, messageEncoder message.Encoder, messagesBufferSize int, sessionPool session.Pool, metricsReporters []metrics.Reporter) Factory {
	return &agentFactoryImpl{
		appDieChan:         appDieChan,
		codec:              codec,
		heartbeatTimeout:   heartbeatTimeout,
		messageEncoder:     messageEncoder,
		messagesBufferSize: messagesBufferSize,
		sessionPool:        sessionPool,
		metricsReporters:   metricsReporters,
		serializer:         serializer,
	}
}
func hbdEncode(heartbeatTimeout time.Duration, codec acceptor.Codec, dataCompression bool, serializerName string) {
	hData := map[string]interface{}{
		"code": 200,
		"sys": map[string]interface{}{
			"heartbeat":  heartbeatTimeout.Seconds(),
			"dict":       message.GetDictionary(),
			"serializer": serializerName,
		},
	}
	data, err := json.Marshal(hData)
	if err != nil {
		panic(err)
	}
	if dataCompression {
		compressedData, err := utils.DeflateData(data)
		if err != nil {
			panic(err)
		}
		if len(compressedData) < len(data) {
			data = compressedData
		}
	}
	hrd, err = codec.Encode(acceptor.Handshake, data)
	if err != nil {
		panic(err)
	}
	hbd, err = codec.Encode(acceptor.Heartbeat, nil)
	if err != nil {
		panic(err)
	}
}
func marshal(e encoding.Encoding, v interface{}) ([]byte, error) {
	if data, ok := v.([]byte); ok {
		return data, nil
	}
	data, err := e.Marshal(v)
	if err != nil {
		return nil, err
	}
	return data, nil
}
func GetErrorFromPayload(encoding encoding.Encoding, payload []byte) error {
	return &errors.Error{Code: errors.ErrUnknownCode}
}
func GetErrorPayload(serializer encoding.Encoding, err error) ([]byte, error) {
	code := errors.ErrUnknownCode
	msg := err.Error()
	metadata := map[string]string{}
	if val, ok := err.(*errors.Error); ok {
		code = val.Code
		metadata = val.Metadata
	}
	errPayload := &proto.Error{
		Code: code,
		Msg:  msg,
	}
	if len(metadata) > 0 {
		errPayload.Metadata = metadata
	}
	return marshal(serializer, errPayload)
}
