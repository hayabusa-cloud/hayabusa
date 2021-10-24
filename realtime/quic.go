package realtime

import (
	"context"

	"github.com/lucas-clemente/quic-go"
)

//lint:file-ignore U1000 reserved implement

type QuicConfig struct {
}
type quicConnection struct {
	quic.Session
}

func (c *quicConnection) AcceptStream(ctx context.Context) (streamInterface, error) {
	var stream, err = c.Session.AcceptStream(ctx)
	return &quicStream{stream}, err
}
func (c *quicConnection) CloseWithError(code ErrorCode) error {
	return c.Session.CloseWithError(quic.ErrorCode(code), "")
}

type quicStream struct {
	quic.Stream
}

func (s *quicStream) CancelRead(code ErrorCode) {
	s.Stream.CancelRead(quic.ErrorCode(code))
}

type QuicListener struct {
	quic.Listener
}

func (l *QuicListener) Accept(ctx context.Context) (connectionInterface, error) {
	var conn, err = l.Listener.Accept(ctx)
	return &quicConnection{conn}, err
}
