package message

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

type Type byte

const (
	Request  Type = 0x00
	Notify   Type = 0x01
	Response Type = 0x02
	Push     Type = 0x03
)

const (
	errorMask            = 0x20
	gzipMask             = 0x10
	msgRouteCompressMask = 0x01
	msgTypeMask          = 0x07
	msgRouteLengthMask   = 0xFF
	msgHeadLength        = 0x02
)

var types = map[Type]string{
	Request:  "Request",
	Notify:   "Notify",
	Response: "Response",
	Push:     "Push",
}

var (
	routesCodesMutex = sync.RWMutex{}
	routes           = make(map[string]uint16) // route map to code
	codes            = make(map[uint16]string) // code map to route
)

var (
	ErrWrongMessageType  = errors.New("wrong message type")
	ErrInvalidMessage    = errors.New("invalid message")
	ErrRouteInfoNotFound = errors.New("route info not found in dictionary")
)

type Message struct {
	Type       Type   // message type
	ID         uint   // unique id, zero while notify mode
	Route      string // route for locating service
	Data       []byte // payload
	compressed bool   // is message compressed
	Err        bool   // is an error message
}

func New(err ...bool) *Message {
	m := &Message{}
	if len(err) > 0 {
		m.Err = err[0]
	}
	return m
}

func (m *Message) String() string {
	return fmt.Sprintf("Type: %s, ID: %d, Route: %s, Compressed: %t, Error: %t, Data: %v, BodyLength: %d", types[m.Type], m.ID, m.Route, m.compressed, m.Err, m.Data, len(m.Data))
}
func routable(t Type) bool {
	return t == Request || t == Notify || t == Push
}
func invalidType(t Type) bool {
	return t < Request || t > Push
}
func SetDictionary(dict map[string]uint16) error {
	if dict == nil {
		return nil
	}
	routesCodesMutex.Lock()
	defer routesCodesMutex.Unlock()

	for route, code := range dict {
		r := strings.TrimSpace(route)
		// duplication check
		if _, ok := routes[r]; ok {
			return fmt.Errorf("duplicated route(route: %s, code: %d)", r, code)
		}
		if _, ok := codes[code]; ok {
			return fmt.Errorf("duplicated route(route: %s, code: %d)", r, code)
		}
		routes[r] = code
		codes[code] = r
	}

	return nil
}
func GetDictionary() map[string]uint16 {
	routesCodesMutex.RLock()
	defer routesCodesMutex.RUnlock()
	dict := make(map[string]uint16)
	for k, v := range routes {
		dict[k] = v
	}
	return dict
}

func (t *Type) String() string {
	return types[*t]
}
