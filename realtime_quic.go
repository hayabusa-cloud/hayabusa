package hybs

import (
	"context"
	"github.com/lucas-clemente/quic-go"
)

type quicConnection struct {
	quic.Session
}

func (c *quicConnection) AcceptStream(ctx context.Context) (rtStreamInterface, error) {
	var stream, err = c.Session.AcceptStream(ctx)
	return &quicStream{stream}, err
}
func (c *quicConnection) CloseWithError(code ErrorCode) error {
	return c.Session.CloseWithError(quic.ErrorCode(code), code.String())
}

type quicStream struct {
	quic.Stream
}

func (s *quicStream) CancelRead(code ErrorCode) {
	s.Stream.CancelRead(quic.ErrorCode(code))
}

type quicListener struct {
	quic.Listener
}

func (l *quicListener) Accept(ctx context.Context) (rtConnInterface, error) {
	var conn, err = l.Listener.Accept(ctx)
	return &quicConnection{conn}, err
}
