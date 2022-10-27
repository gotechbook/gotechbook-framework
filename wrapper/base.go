package wrapper

import acceptor "github.com/gotechbook/gotechbook-framework-acceptor"

var _ acceptor.Acceptor = (*Base)(nil)

type Base struct {
	acceptor.Acceptor
	connChan chan acceptor.Conn
	wrapConn func(acceptor.Conn) acceptor.Conn
}

func NewBaseWrapper(wrapConn func(acceptor.Conn) acceptor.Conn) Base {
	return Base{
		connChan: make(chan acceptor.Conn),
		wrapConn: wrapConn,
	}
}

func (b *Base) ListenAndServe() {
	go b.pipe()
	b.Acceptor.ListenAndServe()
}
func (b *Base) GetConnChan() chan acceptor.Conn {
	return b.connChan
}
func (b *Base) pipe() {
	for conn := range b.Acceptor.GetConnChan() {
		b.connChan <- b.wrapConn(conn)
	}
}
