package realtime

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/hayabusa-cloud/hayabusa/engine"
	"github.com/hayabusa-cloud/hayabusa/plugins"
	"github.com/hayabusa-cloud/hayabusa/utils"
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"testing"
	"time"
)

func NewTestRealtimeServer(t *testing.T, testEngine engine.Interface) Server {
	config := &ServerConfig{
		ID:           "test-realtime-server",
		Network:      "udp",
		Address:      "localhost:9998",
		ByteOrder:    "big",
		WriteTimeout: 100 * time.Millisecond,
		ReadTimeout:  time.Second,
		Heartbeat:    5 * time.Second,
		Protocol:     "kcp",
		KcpConfig: KcpConfig{
			Mtu:        540,
			SndWnd:     128,
			RcvWnd:     128,
			TurboLevel: 3,
			NoDelay:    false,
			Interval:   10,
			Resend:     2,
			Nc:         false,
		},
	}

	listener := &KcpListener{Config: config}

	var server *serverImpl = nil
	server = &serverImpl{
		engine:          testEngine,
		listener:        listener,
		config:          config,
		controllers:     make(controllerConfig, 0),
		lastCreatedRoom: 0,
		roomNum:         0,
		roomPool: &sync.Pool{New: func() interface{} {
			var newRoom = createRoom(server)
			newRoom.server = server
			return newRoom
		}},
		bufferPool: &sync.Pool{New: func() interface{} {
			return make([]byte, udpSocketBufferLength)
		}},
		ctxValuePool: &sync.Pool{New: func() interface{} {
			return &ctxValueTable{}
		}},
		requestPool: &sync.Pool{New: func() interface{} {
			var ret = &message{server: server}
			ret.payload = server.bufferPool.Get().([]byte)
			return ret
		}},
		responsePool: &sync.Pool{New: func() interface{} {
			var ret = &message{server: server}
			ret.payload = server.bufferPool.Get().([]byte)
			return ret
		}},
		networkByteOrder: binary.BigEndian,
		isClosing:        false,
		logEntry:         logrus.NewEntry(logrus.New()),
	}
	if strings.HasPrefix(config.ByteOrder, "little") {
		server.networkByteOrder = binary.LittleEndian
		fmt.Println("set network byte order=little endian")
	}
	server.connectionManager.init(server)

	return server
}

type testEngine struct {
	c context.Context
	l *logrus.Entry
	m utils.KVPairs
}

func (eng *testEngine) Printf(format string, args ...interface{}) {
	fmt.Printf(format, args...)
}
func (eng *testEngine) Infof(format string, args ...interface{}) {
	eng.l.Infof(format, args...)
}
func (eng *testEngine) Warnf(format string, args ...interface{}) {
	eng.l.Warnf(format, args...)
}
func (eng *testEngine) Errorf(format string, args ...interface{}) {
	eng.l.Errorf(format, args...)
}
func (eng *testEngine) Fatalf(format string, args ...interface{}) {
	eng.l.Fatalf(format, args...)
}
func (eng *testEngine) WithField(key string, value interface{}) *logrus.Entry {
	return eng.l.WithField(key, value)
}
func (eng *testEngine) MasterData() plugins.MasterDataManager {
	panic("implement me")
}
func (eng *testEngine) Cache(id string) *plugins.Cache {
	panic("implement me")
}
func (eng *testEngine) Redis(id string) *plugins.Redis {
	panic("implement me")
}
func (eng *testEngine) Mongo(id string) *plugins.Mongo {
	panic("implement me")
}
func (eng *testEngine) MySQL(id string) *plugins.MySQL {
	panic("implement me")
}
func (eng *testEngine) Sqlite(id string) *plugins.Sqlite {
	panic("implement me")
}
func (eng *testEngine) LBClient(id string) *plugins.LBClient {
	panic("implement me")
}
func (eng *testEngine) AppName() string {
	return "test-app"
}
func (eng *testEngine) Version() string {
	return "0.0.0"
}
func (eng *testEngine) Env() string {
	return "DEV"
}
func (eng *testEngine) Context() context.Context {
	return eng.c
}
func (eng *testEngine) UserValue(key string) (value interface{}, ok bool) {
	return eng.m.Get(key)
}
func (eng *testEngine) SetUserValue(key string, value interface{}) {
	eng.m.Set(key, value)
}
func (eng *testEngine) Now() time.Time {
	return time.Now()
}
func (eng *testEngine) DebugMode() bool {
	return true
}

func NewTestEngine() *testEngine {
	return &testEngine{
		l: logrus.NewEntry(logrus.New()),
		c: context.Background(),
		m: make(utils.KVPairs, 0),
	}
}
