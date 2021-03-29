package hybs

import (
	"context"
	"github.com/xtaci/kcp-go"
	"time"
)

type kcpConnection struct {
	writeTimeout time.Duration
	readTimeout  time.Duration
	*kcp.UDPSession
}

const (
	kcpTurboLevelSlow    = 0
	kcpTurboLevelNormal  = 1
	kcpTurboLevelFaster  = 2
	kcpTurboLevelFastest = 3
	kcpTurboLevelNum     = kcpTurboLevelFastest + 1
	kcpTurboLevelDefault = kcpTurboLevelFaster
)

type realtimeKCPConfig struct {
	Mtu        int `yaml:"mtu" default:"736"`
	SndWnd     int `yaml:"snd_wnd" default:"64"`
	RcvWnd     int `yaml:"rcv_wnd" default:"64"`
	TurboLevel int `yaml:"turbo_level" default:"-1"`
	// if TurboLevel is filled in, the following configuration will be ignored
	NoDelay  bool          `yaml:"no_delay" default:"true"`
	Interval time.Duration `yaml:"interval" default:"25ms"`
	Resend   int           `yaml:"resend" default:"2"`
	Nc       bool          `yaml:"nc" default:""true"`
}

func (c *kcpConnection) AcceptStream(ctx context.Context) (rtStreamInterface, error) {
	return c, nil
}
func (c *kcpConnection) Close() error {
	return c.UDPSession.Close()
}
func (c *kcpConnection) CloseWithError(ErrorCode) error {
	return c.UDPSession.Close()
}
func (c *kcpConnection) Read(p []byte) (n int, err error) {
	c.UDPSession.SetReadDeadline(time.Now().Add(c.readTimeout))
	return c.UDPSession.Read(p)
}
func (c *kcpConnection) Write(p []byte) (n int, err error) {
	c.UDPSession.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	return c.UDPSession.Write(p)
}

type kcpListener struct {
	*kcp.Listener
	config *realtimeConfig
}

func (l *kcpListener) Accept(context.Context) (rtConnInterface, error) {
	l.Listener.SetDeadline(time.Now().Add(time.Second))
	var udpSession, err = l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	// turbo level can be setting by config file
	var turboLevelArgsTable = [kcpTurboLevelNum][4]int{
		// turbo level=0 is for turn-based game etc. no needs for high-performance
		// when turbo level=0, divides the time frame at 100 millisecond intervals
		{1, 100, 2, 1},
		// when turbo level=1, divides the time frame at 50 millisecond intervals
		{1, 50, 2, 1},
		// turbo level=2 is for action, racing games etc.
		// for use scenarios that require high performance
		// when turbo level=2, divides the time frame at 25 millisecond intervals
		{1, 25, 2, 1},
		// turbo level=3 is for FPS games etc.
		// for use scenarios that require high performance
		// when turbo level=3, divides the time frame at 10 millisecond intervals
		{1, 10, 2, 1}}
	var nodelay, interval, resend, nc int
	if l.config.NoDelay {
		nodelay = 1
	} else {
		nodelay = 0
	}
	interval = int(l.config.Interval.Seconds())
	resend = l.config.Resend
	if l.config.Nc {
		nc = 1
	} else {
		nc = 0
	}
	if l.config.TurboLevel < kcpTurboLevelNum && l.config.TurboLevel >= 0 {
		var args = turboLevelArgsTable[l.config.TurboLevel]
		nodelay, interval, resend, nc = args[0], args[1], args[2], args[3]
	}
	udpSession.(*kcp.UDPSession).SetMtu(l.config.Mtu)
	// wnd size=64 is a suitable result by experiment
	udpSession.(*kcp.UDPSession).SetWindowSize(l.config.SndWnd, l.config.RcvWnd)
	udpSession.(*kcp.UDPSession).SetNoDelay(nodelay, interval, resend, nc)
	return &kcpConnection{
		writeTimeout: l.config.WriteTimeout,
		readTimeout:  l.config.ReadTimeout,
		UDPSession:   udpSession.(*kcp.UDPSession),
	}, err
}
