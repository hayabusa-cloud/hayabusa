package web

import (
	"net/http"

	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttprouter"
)

// Handler does process http API business logic
type Handler func(ctx Ctx)

var handler0 = func(ctx Ctx) {}

func rawHttp11Handler(server *Server, f Handler) fasthttprouter.Handle {
	return func(reqCtx *fasthttp.RequestCtx, params fasthttprouter.Params) {
		handleAt := server.engine.Now()
		ctx := createHttp11Ctx(server, reqCtx, params)
		ctx.handleAt = handleAt
		ctx.sysLoggerEntry = server.sysLogger.(*logrus.Entry).WithTime(handleAt)
		ctx.gameLoggerEntry = server.gameLogger.(*logrus.Entry).WithTime(handleAt)
		// default context values
		ctx.SetCtxValue("Server", server)
		ctx.SetCtxValue("Engine", server.engine)
		ctx.SetCtxValue("RequestID", reqCtx.ID())
		server.apiLock.RLock()
		defer server.apiLock.RUnlock()
		f(ctx)
	}
}

type http3HandlerImpl struct {
	server *Server
	f      Handler
}

func (h *http3HandlerImpl) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	handleAt := h.server.engine.Now()
	ctx := createHttp3Ctx(h.server, rw, req)
	ctx.handleAt = handleAt
	ctx.sysLoggerEntry = h.server.sysLogger.(*logrus.Logger).WithTime(handleAt)
	ctx.gameLoggerEntry = h.server.gameLogger.(*logrus.Logger).WithTime(handleAt)
	// default context values
	ctx.SetCtxValue("Server", h.server)
	ctx.SetCtxValue("Engine", h.server.engine)
	ctx.SetCtxValue("RequestID", uint64(handleAt.Unix()))
	h.server.apiLock.RLock()
	defer h.server.apiLock.RUnlock()
	h.f(ctx)
}
func rawHttp3Handler(svr *Server, f Handler) http.Handler {
	return &http3HandlerImpl{server: svr, f: f}
}
