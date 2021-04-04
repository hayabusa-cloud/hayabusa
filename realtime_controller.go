package hybs

const (
	rtHandlerTableSizeBit = 16
	rtHandlerTableSize    = 1 << rtHandlerTableSizeBit
)

type rtHandlerTable [rtHandlerTableSize]RealtimeHandler
type rtEventModuleTable [rtHandlerTableSize]uint8

// RealtimeHandler is function type for processing socket requests
type RealtimeHandler func(ctx RealtimeCtx)

// Combine returns a new RealtimeHandler that processes multiple businesses in sequence
func (h RealtimeHandler) Combine(next ...RealtimeHandler) RealtimeHandler {
	if len(next) < 1 {
		return h
	}
	return func(ctx RealtimeCtx) {
		h(ctx)
		for _, n := range next {
			if n == nil {
				continue
			}
			n(ctx)
		}
	}
}

// RealtimeMiddleware is function type which receives a RealtimeHandler and
// returns a new RealtimeHandler type closure included middleware logic
type RealtimeMiddleware func(h RealtimeHandler) RealtimeHandler

// Apply returns a new RealtimeHandler included middleware logic
func (m RealtimeMiddleware) Apply(h RealtimeHandler) RealtimeHandler {
	return m(h)
}

// Left returns a new RealtimeMiddleware first including process logic in m, and then includes inner
func (m RealtimeMiddleware) Left(inner RealtimeMiddleware) RealtimeMiddleware {
	return func(h RealtimeHandler) RealtimeHandler {
		return m.Apply(inner.Apply(h))
	}
}

// Right returns a new RealtimeMiddleware first including process logic in outer, and then includes m
func (m RealtimeMiddleware) Right(outer RealtimeMiddleware) RealtimeMiddleware {
	return func(h RealtimeHandler) RealtimeHandler {
		return outer.Apply(m.Apply(h))
	}
}

var (
	rtMiddlewareZero RealtimeMiddleware = func(h RealtimeHandler) RealtimeHandler { return func(ctx RealtimeCtx) {} }
	rtMiddlewareIE   RealtimeMiddleware = func(h RealtimeHandler) RealtimeHandler { return h }
)

// <event code, mask code>
const (
	rtMiddlewareTableSizeBit = 16
	rtMiddlewareTableSize    = 1 << rtMiddlewareTableSizeBit
)

type rtMiddlewareTable [rtMiddlewareTableSize][rtMiddlewareTableSizeBit + 1][]RealtimeMiddleware

func (mgr *rtMiddlewareTable) Use(code uint16, mask uint8, m RealtimeMiddleware) {
	seg := (code >> (0x10 - mask)) << (0x10 - mask)
	if mgr[seg][mask] == nil {
		mgr[seg][mask] = make([]RealtimeMiddleware, 0)
	}
	mgr[seg][mask] = append(mgr[seg][mask], m)
}

type rtMiddlewareDefineInfo struct {
	id string
	m  RealtimeMiddleware
}

var defaultMiddlewareDefines = make([]rtMiddlewareDefineInfo, 0, 0x100)

func RegisterRealtimeMiddleware(id string, m RealtimeMiddleware) {
	if m == nil {
		return
	}
	defaultMiddlewareDefines = append(defaultMiddlewareDefines, rtMiddlewareDefineInfo{
		id: id,
		m:  m,
	})
}

type rtHandlerDefineInfo struct {
	id string
	h  RealtimeHandler
}
type rtHandlerDefineList []rtHandlerDefineInfo

var (
	rtProcessorHandlers = make(map[uint8]rtHandlerDefineList)
)

// RegisterRealtimeHandler register a RealtimeHandler by specified id
func RegisterRealtimeHandler(id string, h RealtimeHandler) {
	if h == nil {
		return
	}
	// because of single goroutine model, processor id always be 0
	var s, ok = rtProcessorHandlers[0]
	if !ok {
		s = make(rtHandlerDefineList, 0, 0x10)
	}
	s = append(s, rtHandlerDefineInfo{
		id: id,
		h:  h,
	})
	rtProcessorHandlers[0] = s
}

type rtHandlerTree struct {
	children    [2]*rtHandlerTree
	middlewares []RealtimeMiddleware
	handlers    []RealtimeHandler
	depth       uint8
	redis       string
	cache       string
	csvMux      string
}

func rtHandlerTreeCreateInit() *rtHandlerTree {
	return &rtHandlerTree{
		children:    [2]*rtHandlerTree{nil, nil},
		middlewares: make([]RealtimeMiddleware, 0, 0x10),
		handlers:    make([]RealtimeHandler, 0, 0x10),
		depth:       0,
		redis:       "",
		cache:       "",
		csvMux:      "",
	}
}
func (tree *rtHandlerTree) peekOrCreate(code uint16, bits uint8) (node *rtHandlerTree) {
	if tree == nil {
		return tree
	}
	if bits > 16 {
		bits = 16
	}
	node = tree
	for node.depth < bits {
		var childIdx = (code >> (15 - node.depth)) & 1
		if node.children[childIdx] == nil {
			node.children[childIdx] = rtHandlerTreeCreateInit()
			node.children[childIdx].depth = node.depth + 1
		}
		node = node.children[childIdx]
	}
	return
}
func (tree *rtHandlerTree) insertHandler(h RealtimeHandler) {
	tree.handlers = append(tree.handlers, h)
}
func (tree *rtHandlerTree) insertMiddleware(m RealtimeMiddleware) {
	tree.middlewares = append(tree.middlewares, m)
}
func (tree *rtHandlerTree) setDefaultRedis(id string) {
	tree.redis = id
}
func (tree *rtHandlerTree) setDefaultCache(id string) {
	tree.cache = id
}
func (tree *rtHandlerTree) setDefaultCsvMux(mux string) {
	tree.csvMux = mux
}
func (tree *rtHandlerTree) search(code uint16) RealtimeHandler {
	var handlers = make([]RealtimeHandler, 0)
	var node = tree
	var middleware = rtMiddlewareIE
	var redis, cache, csvMux = "", "", ""
	for node != nil {
		for _, m := range node.middlewares {
			middleware = middleware.Left(m)
		}
		for _, h := range node.handlers {
			handlers = append(handlers, middleware.Apply(h))
		}
		if node.redis != "" {
			redis = node.redis
		}
		if node.cache != "" {
			cache = node.cache
		}
		if node.csvMux != "" {
			csvMux = node.csvMux
		}
		if node.depth >= 0x10 {
			break
		}
		node = node.children[(code>>(15-node.depth))&1]
	}
	if redis != "" {
		middleware = middleware.Left(rtBuiltinMiddlewareDefaultRedis(redis))
	}
	if cache != "" {
		middleware = middleware.Left(rtBuiltinMiddlewareDefaultCache(cache))
	}
	if csvMux != "" {
		middleware = middleware.Left(rtBuiltinMiddlewareDefaultCsvMux(csvMux))
	}
	if len(handlers) == 0 {
		return func(ctx RealtimeCtx) {}
	} else if len(handlers) == 1 {
		return handlers[0]
	} else {
		return handlers[0].Combine(handlers[1:]...)
	}
}

type rtOnConnectedDefine struct {
	id string
	h  func(ss RealtimeSession)
}
type rtOnDisconnectedDefine struct {
	id string
	h  func(ss RealtimeSession, errorCode ErrorCode)
}

var (
	defaultOnConnectedHandlers    = make([]rtOnConnectedDefine, 0, 0x10)
	defaultOnDisconnectedHandlers = make([]rtOnDisconnectedDefine, 0, 0x10)
)

func RegisterRealtimeOnConnected(id string, h func(ss RealtimeSession)) {
	defaultOnConnectedHandlers = append(defaultOnConnectedHandlers, rtOnConnectedDefine{
		id: id,
		h:  h,
	})
}
func RegisterRealtimeOnDisconnected(id string, h func(ss RealtimeSession, errorCode ErrorCode)) {
	defaultOnDisconnectedHandlers = append(defaultOnDisconnectedHandlers, rtOnDisconnectedDefine{
		id: id,
		h:  h,
	})
}

const rtHeartbeatLife int8 = 2

const (
	rtHeaderPV0  uint8 = 0 << 2 << 4
	rtHeaderPV1  uint8 = 1 << 2 << 4
	rtHeaderPV2  uint8 = 2 << 2 << 4
	rtHeaderPV3  uint8 = 3 << 2 << 4
	rtHeaderPDCC uint8 = 0 << 4
	rtHeaderPDSS uint8 = 1 << 4
	rtHeaderPDSC uint8 = 2 << 4
	rtHeaderPDCS uint8 = 3 << 4
)

const (
	rtHeaderCmdHello        uint8 = 0x0
	rtHeaderCmdClosing      uint8 = 0x1
	rtHeaderCmdClosed       uint8 = 0x2
	rtHeaderCmdEstablished  uint8 = 0x3
	rtHeaderCmdAuthenticate uint8 = 0x4
	rtHeaderCmdNTP          uint8 = 0x5
	rtHeaderCmdProxy        uint8 = 0x6
	rtHeaderCmdForward      uint8 = 0x7
	rtHeaderCmdBuiltin      uint8 = 0x8
	rtHeaderCmdOriginal     uint8 = 0x9

	// echo test only works on debug mode
	rtHeaderCmdEchoTest uint8 = 0xf
)

const (
	rTHeader0SSProxy          = rtHeaderPV0 | rtHeaderPDSS | rtHeaderCmdProxy
	rTHeader0SSForward        = rtHeaderPV0 | rtHeaderPDSS | rtHeaderCmdForward
	rTHeader0SCBuiltin        = rtHeaderPV0 | rtHeaderPDSC | rtHeaderCmdBuiltin
	RealtimeHeader0SCOriginal = rtHeaderPV0 | rtHeaderPDSC | rtHeaderCmdOriginal
)

const (
	rtPacketStatusHeader          uint8 = 0x00
	rtPacketStatusAuthToken       uint8 = 0x40
	rtPacketStatusNTP             uint8 = 0x50
	rtPacketStatusPayloadLength   uint8 = 0x80
	rtPacketStatusPayloadContents uint8 = 0x82
	rtPacketStatusEchoTest        uint8 = 0xf0
)

const (
	// hayabusa framework does only implement basic authentication
	RtAuthStatusNone uint8 = 0x0
	// implement authorization middleware in your own applications
	RtAuthStatusNormal uint8 = 0x1
	RtAuthStatusAdmin  uint8 = 0x2
)
