package message

import (
	"encoding/binary"
	utils "github.com/gotechbook/gotechbook-framework-utils"
)

type Encoder interface {
	IsCompressionEnabled() bool
	Encode(message *Message) ([]byte, error)
}

type MessagesEncoder struct {
	DataCompression bool
}

func NewMessagesEncoder(dataCompression bool) *MessagesEncoder {
	me := &MessagesEncoder{dataCompression}
	return me
}

func (e *MessagesEncoder) IsCompressionEnabled() bool {
	return e.DataCompression
}
func (e *MessagesEncoder) Encode(message *Message) ([]byte, error) {
	if invalidType(message.Type) {
		return nil, ErrWrongMessageType
	}

	buf := make([]byte, 0)
	flag := byte(message.Type) << 1

	routesCodesMutex.RLock()
	code, compressed := routes[message.Route]
	routesCodesMutex.RUnlock()
	if compressed {
		flag |= msgRouteCompressMask
	}

	if message.Err {
		flag |= errorMask
	}

	buf = append(buf, flag)

	if message.Type == Request || message.Type == Response {
		n := message.ID
		// variant length encode
		for {
			b := byte(n % 128)
			n >>= 7
			if n != 0 {
				buf = append(buf, b+128)
			} else {
				buf = append(buf, b)
				break
			}
		}
	}

	if routable(message.Type) {
		if compressed {
			buf = append(buf, byte((code>>8)&0xFF))
			buf = append(buf, byte(code&0xFF))
		} else {
			buf = append(buf, byte(len(message.Route)))
			buf = append(buf, []byte(message.Route)...)
		}
	}

	if e.DataCompression {
		d, err := utils.DeflateData(message.Data)
		if err != nil {
			return nil, err
		}

		if len(d) < len(message.Data) {
			message.Data = d
			buf[0] |= gzipMask
		}
	}

	buf = append(buf, message.Data...)
	return buf, nil
}
func (e *MessagesEncoder) Decode(data []byte) (*Message, error) {
	return Decode(data)
}
func Decode(data []byte) (*Message, error) {
	if len(data) < msgHeadLength {
		return nil, ErrInvalidMessage
	}
	m := New()
	flag := data[0]
	offset := 1
	m.Type = Type((flag >> 1) & msgTypeMask)

	if invalidType(m.Type) {
		return nil, ErrWrongMessageType
	}
	if m.Type == Request || m.Type == Response {
		id := uint(0)
		for i := offset; i < len(data); i++ {
			b := data[i]
			id += uint(b&0x7F) << uint(7*(i-offset))
			if b < 128 {
				offset = i + 1
				break
			}
		}
		m.ID = id
	}

	m.Err = flag&errorMask == errorMask

	if routable(m.Type) {
		if flag&msgRouteCompressMask == 1 {
			m.compressed = true
			code := binary.BigEndian.Uint16(data[offset:(offset + 2)])
			routesCodesMutex.RLock()
			route, ok := codes[code]
			routesCodesMutex.RUnlock()
			if !ok {
				return nil, ErrRouteInfoNotFound
			}
			m.Route = route
			offset += 2
		} else {
			m.compressed = false
			rl := data[offset]
			offset++
			m.Route = string(data[offset:(offset + int(rl))])
			offset += int(rl)
		}
	}

	m.Data = data[offset:]
	var err error
	if flag&gzipMask == gzipMask {
		m.Data, err = utils.InflateData(m.Data)
		if err != nil {
			return nil, err
		}
	}
	return m, nil
}
