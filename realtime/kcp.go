package realtime

import (
	"context"
	"time"

	"github.com/xtaci/kcp-go"
)

const (
	kcpTurboLevelSlow    = 0
	kcpTurboLevelNormal  = 1
	kcpTurboLevelFaster  = 2
	kcpTurboLevelFastest = 3
	kcpTurboLevelNum     = kcpTurboLevelFastest + 1
	kcpTurboLevelDefault = kcpTurboLevelFaster
)

type KcpConfig struct {
	Mtu        int `yaml:"mtu" default:"736"`
	SndWnd     int `yaml:"snd_wnd" default:"128"`
	RcvWnd     int `yaml:"rcv_wnd" default:"128"`
	TurboLevel int `yaml:"turbo_level" default:"-1"`
	// if TurboLevel is filled in, the following configuration will be ignored
	NoDelay  bool          `yaml:"no_delay" default:"false"`
	Interval time.Duration `yaml:"interval" default:"25ms"`
	Resend   int           `yaml:"resend" default:"2"`
	Nc       bool          `yaml:"nc" default:"false"`
}

type kcpConnection struct {
	writeTimeout time.Duration
	readTimeout  time.Duration
	*kcp.UDPSession
}

func (c *kcpConnection) AcceptStream(ctx context.Context) (streamInterface, error) {
	return c, nil
}
func (c *kcpConnection) Close() error {
	return c.UDPSession.Close()
}
func (c *kcpConnection) CloseWithError(ErrorCode) error {
	return c.UDPSession.Close()
}
func (c *kcpConnection) Read(p []byte) (n int, err error) {
	err = c.UDPSession.SetReadDeadline(time.Now().Add(c.readTimeout))
	if err != nil {
		return 0, err
	}
	return c.UDPSession.Read(p)
}
func (c *kcpConnection) Write(p []byte) (n int, err error) {
	err = c.UDPSession.SetWriteDeadline(time.Now().Add(c.writeTimeout))
	if err != nil {
		return 0, err
	}
	return c.UDPSession.Write(p)
}

type KcpListener struct {
	*kcp.Listener
	Config *ServerConfig
}

func (l *KcpListener) Accept(context.Context) (connectionInterface, error) {
	err := l.Listener.SetDeadline(time.Now().Add(time.Second))
	if err != nil {
		return nil, err
	}
	udpSession, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	// turbo level can be setting by config file
	var turboLevelArgsTable = [kcpTurboLevelNum][4]int{
		// turbo level=0 is for turn-based game etc. no needs for high-performance
		{0, 100, 2, 0},
		// turbo level=1 is the standard turbo level
		{0, 50, 2, 0},
		// turbo level=2 is for action, racing games etc.
		// for use scenarios that require high performance
		{0, 25, 2, 0},
		// turbo level=3 is for FPS games etc.
		// for use scenarios that require high performance
		{0, 10, 2, 0}}
	var nodelay, interval, resend, nc int
	if l.Config.NoDelay {
		nodelay = 1
	} else {
		nodelay = 0
	}
	interval = int(l.Config.Interval.Seconds())
	resend = l.Config.Resend
	if l.Config.Nc {
		nc = 1
	} else {
		nc = 0
	}
	if l.Config.TurboLevel < kcpTurboLevelNum && l.Config.TurboLevel >= 0 {
		var args = turboLevelArgsTable[l.Config.TurboLevel]
		nodelay, interval, resend, nc = args[0], args[1], args[2], args[3]
	}
	udpSession.(*kcp.UDPSession).SetStreamMode(false)
	udpSession.(*kcp.UDPSession).SetMtu(l.Config.Mtu)
	// wnd size=64 is a suitable result by experiment
	udpSession.(*kcp.UDPSession).SetWindowSize(l.Config.SndWnd, l.Config.RcvWnd)
	udpSession.(*kcp.UDPSession).SetNoDelay(nodelay, interval, resend, nc)
	return &kcpConnection{
		writeTimeout: l.Config.WriteTimeout,
		readTimeout:  l.Config.ReadTimeout,
		UDPSession:   udpSession.(*kcp.UDPSession),
	}, err
}
