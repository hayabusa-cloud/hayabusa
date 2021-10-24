package realtime

const (
	handlerTableSizeBit = 16
	handlerTableSize    = 1 << handlerTableSizeBit
)

type handlerTable [handlerTableSize]Handler

//lint:ignore U1000 todo
// issue(discussion):
// single processor(goroutine) model or multi-processors model
// for a moment, use single goroutine model, because parallel
// processing is treasure island of bugs
type handlerProcessorMap [handlerTableSize]uint8

// Handler is function type for processing socket requests
type Handler func(ctx Ctx)

// Combine returns a new Handler that processes multiple businesses in sequence
func (h Handler) Combine(next ...Handler) Handler {
	if len(next) < 1 {
		return h
	}
	return func(ctx Ctx) {
		h(ctx)
		for _, n := range next {
			if n == nil {
				continue
			}
			n(ctx)
		}
	}
}

// Middleware is function type which receives a RealtimeHandler and
// returns a new RealtimeHandler type closure included middleware logic
type Middleware func(h Handler) Handler

// Apply returns a new RealtimeHandler included middleware logic
func (m Middleware) Apply(h Handler) Handler {
	return m(h)
}

// Left returns a new RealtimeMiddleware first including process logic in m, and then includes inner
func (m Middleware) Left(inner Middleware) Middleware {
	return func(h Handler) Handler {
		return m.Apply(inner.Apply(h))
	}
}

// Right returns a new RealtimeMiddleware first including process logic in outer, and then includes m
func (m Middleware) Right(outer Middleware) Middleware {
	return func(h Handler) Handler {
		return outer.Apply(m.Apply(h))
	}
}

var (
	middlewareZero Middleware = func(h Handler) Handler { return func(ctx Ctx) {} }
	middlewareIE   Middleware = func(h Handler) Handler { return h }
)

const (
	middlewareTableSizeBit = 16
	middlewareTableSize    = 1 << middlewareTableSizeBit
)

//lint:ignore U1000 todo
// <event code, mask code>
type middlewareTable [middlewareTableSize][middlewareTableSizeBit + 1][]Middleware

func (mt *middlewareTable) Use(code uint16, mask uint8, m Middleware) {
	seg := (code >> (0x10 - mask)) << (0x10 - mask)
	if mt[seg][mask] == nil {
		mt[seg][mask] = make([]Middleware, 0)
	}
	mt[seg][mask] = append(mt[seg][mask], m)
}

type middlewareTuple struct {
	id string
	m  Middleware
}

var defaultMiddlewareTupleList = make([]middlewareTuple, 0, 0x100)

func RegisterMiddleware(id string, m Middleware) {
	if m == nil {
		return
	}
	defaultMiddlewareTupleList = append(defaultMiddlewareTupleList, middlewareTuple{
		id: id,
		m:  m,
	})
}

type handlerTuple struct {
	id string
	h  Handler
}
type handlerTupleList []handlerTuple

var (
	// processor id -> handler list
	processorHandlers = make(map[uint8]handlerTupleList)
)

// RegisterHandler register a realtime.Handler by specified id
func RegisterHandler(id string, h Handler) {
	if h == nil {
		return
	}
	// because of single goroutine model, processor id always be 0
	var s, ok = processorHandlers[0]
	if !ok {
		s = make(handlerTupleList, 0, 0x10)
	}
	s = append(s, handlerTuple{
		id: id,
		h:  h,
	})
	processorHandlers[0] = s
}

type handlerTree struct {
	children    [2]*handlerTree
	middlewares []Middleware
	handlers    []Handler
	depth       uint8
	redis       string
	cache       string
	csvMux      string
}

func createNewHandlerTree() *handlerTree {
	return &handlerTree{
		children:    [2]*handlerTree{nil, nil},
		middlewares: make([]Middleware, 0, 0x10),
		handlers:    make([]Handler, 0, 0x10),
		depth:       0,
		redis:       "",
		cache:       "",
		csvMux:      "",
	}
}
func (tree *handlerTree) peekOrCreate(code uint16, bits uint8) (node *handlerTree) {
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
			node.children[childIdx] = createNewHandlerTree()
			node.children[childIdx].depth = node.depth + 1
		}
		node = node.children[childIdx]
	}
	return
}
func (tree *handlerTree) insertHandler(h Handler) {
	tree.handlers = append(tree.handlers, h)
}
func (tree *handlerTree) insertMiddleware(m Middleware) {
	tree.middlewares = append(tree.middlewares, m)
}
func (tree *handlerTree) setDefaultRedis(id string) {
	tree.redis = id
}
func (tree *handlerTree) setDefaultCache(id string) {
	tree.cache = id
}
func (tree *handlerTree) setDefaultCsvMux(mux string) {
	tree.csvMux = mux
}
func (tree *handlerTree) search(code uint16) Handler {
	var handlers = make([]Handler, 0)
	var node = tree
	var middleware = middlewareIE
	var redis, cache, csvMux = "", "", ""
	for node != nil {
		for _, m := range node.middlewares {
			middleware = middleware.Left(m)
		}
		for _, h := range node.handlers {
			handlers = append(handlers, middleware(h))
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
		middleware = middleware.Left(builtinMiddlewareDefaultRedis(redis))
	}
	if cache != "" {
		middleware = middleware.Left(builtinMiddlewareDefaultCache(cache))
	}
	if csvMux != "" {
		middleware = middleware.Left(builtinMiddlewareDefaultCsvMux(csvMux))
	}
	if len(handlers) == 0 {
		return func(ctx Ctx) {}
	} else if len(handlers) == 1 {
		return middleware(handlers[0])
	}
	return middleware(handlers[0].Combine(handlers[1:]...))
}

type onConnectedHandlerTuple struct {
	id string
	h  func(conn Connection)
}
type onDisconnectedHandlerTuple struct {
	id string
	h  func(conn Connection, errorCode ErrorCode)
}

var (
	defaultOnConnectedHandlers    = make([]onConnectedHandlerTuple, 0, 0x10)
	defaultOnDisconnectedHandlers = make([]onDisconnectedHandlerTuple, 0, 0x10)
)

func RegisterOnConnected(id string, h func(conn Connection)) {
	defaultOnConnectedHandlers = append(defaultOnConnectedHandlers, onConnectedHandlerTuple{
		id: id,
		h:  h,
	})
}
func RegisterOnDisconnected(id string, h func(conn Connection, errorCode ErrorCode)) {
	defaultOnDisconnectedHandlers = append(defaultOnDisconnectedHandlers, onDisconnectedHandlerTuple{
		id: id,
		h:  h,
	})
}

const (
	headerProtocolVer0 uint8 = 0 << 4
	headerProtocolVer1 uint8 = 1 << 4
	headerProtocolVer2 uint8 = 2 << 4
	headerProtocolVer3 uint8 = 3 << 4
	headerProtocolVer4 uint8 = 4 << 4
	headerProtocolVer5 uint8 = 5 << 4
	headerProtocolVer6 uint8 = 6 << 4
	headerProtocolVer7 uint8 = 7 << 4
)

const (
	headerCmdHello        uint8 = 0x0
	headerCmdClosing      uint8 = 0x1
	headerCmdClosed       uint8 = 0x2
	headerCmdEstablished  uint8 = 0x3
	headerCmdAuthenticate uint8 = 0x4
	headerCmdNTP          uint8 = 0x5
	headerCmdProxy        uint8 = 0x6
	headerCmdForward      uint8 = 0x7
	headerCmdBuiltin      uint8 = 0x8
	headerCmdOriginal     uint8 = 0x9

	// broadcast test only works on debug mode
	headerCmdBroadcastTest uint8 = 0xe
	// echo test only works on debug mode
	headerCmdEchoTest uint8 = 0xf
)

const (
	headerV0Proxy    = headerProtocolVer0 | headerCmdProxy
	headerV0Forward  = headerProtocolVer0 | headerCmdForward
	headerV0Builtin  = headerProtocolVer0 | headerCmdBuiltin
	HeaderV0Original = headerProtocolVer0 | headerCmdOriginal
)

const (
	packetStatusHeader          uint8 = 0x00
	packetStatusAuthToken       uint8 = 0x40
	packetStatusNTP             uint8 = 0x50
	packetStatusPayloadLength   uint8 = 0x80
	packetStatusPayloadContents uint8 = 0x82
	packetStatusBroadcastTest   uint8 = 0xe0
	packetStatusEchoTest        uint8 = 0xf0
)
