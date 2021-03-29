package hybs

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-playground/pure/v5"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttprouter"
)

// RegisterService registers ServiceHandler function with serviceID
func RegisterService(serviceID string, f ServiceHandler) {
	appendServiceDefine(serviceID, f)
}

// RegisterMiddlewareFunc registers middlewareFunc function with serviceID
func RegisterMiddleware(middlewareID string, m Middleware) {
	appendMiddlewareDefine(middlewareID, m)
}

// HeaderReceivedHandler is function of process header
type HeaderReceivedHandler func(header *fasthttp.RequestHeader) fasthttp.RequestConfig

type controllerParamConfigItem struct {
	Name        string `yaml:"name" required:"true"`
	Description string
	Example     interface{}
	Allow       []string
	Deny        []string
	Value       interface{}
}

type controllerConfigBase struct {
	Allow          []string                    `yaml:"allow"`
	Deny           []string                    `yaml:"deny"`
	SlowQueryWarn  time.Duration               `yaml:"slow_query_warn" default:"0ms"`
	SlowQueryError time.Duration               `yaml:"slow_query_error" default:"0ms"`
	HeaderParams   []controllerParamConfigItem `yaml:"header_params"`
	PathParams     []controllerParamConfigItem `yaml:"path_params"`
	QueryArgs      []controllerParamConfigItem `yaml:"query_args"`
	FormArgs       []controllerParamConfigItem `yaml:"form_args"`
	ConstParams    []controllerParamConfigItem `yaml:"const_params"`
	Middlewares    []string                    `yaml:"middlewares"`
	Response       []struct {
		StatusCode  int    `yaml:"status_code" required:"true"`
		Description string `yaml:"description"`
		Fields      []struct {
			Name        string `yaml:"name"`
			Description string `yaml:"description"`
			Example     string `yaml:"example"`
		} `yaml:"fields"`
	} `yaml:"response"`
	Links []struct {
		Service string `yaml:"service"`
		Href    string `yaml:"href"`
	} `yaml:"links"`
	HideDocument bool `yaml:"hide_document" default:"false"`
}

type controllerConfigItem struct {
	Location    string               `yaml:"location"`
	ServerName  string               `yaml:"server_name" default:""`
	Description string               `yaml:"description"`
	Base        controllerConfigBase `yaml:",inline"`
	Services    []struct {
		ServiceID   string               `yaml:"service_id" required:"true"`
		Method      string               `yaml:"method"`
		Description string               `yaml:"description"`
		Base        controllerConfigBase `yaml:",inline"`
	}
}

type controllerConfig []controllerConfigItem

type serviceDefine struct {
	serviceID string
	handler   ServiceHandler
}

var serviceDefineList []*serviceDefine

func appendServiceDefine(funcID string, handler ServiceHandler) {
	serviceDefineList = append(serviceDefineList, &serviceDefine{
		serviceID: funcID,
		handler:   handler,
	})
}

type middlewareDefine struct {
	middlewareID string
	m            Middleware
}

var middlewareDefineList []*middlewareDefine

func appendMiddlewareDefine(middlewareID string, m Middleware) {
	middlewareDefineList = append(middlewareDefineList, &middlewareDefine{
		middlewareID: middlewareID,
		m:            m,
	})
}

type headerReceivedDefine struct {
	handlerID string
	h         HeaderReceivedHandler
}

var headerReceivedHandlerList []*headerReceivedDefine

func appendHeaderReceivedDefine(handlerID string, h HeaderReceivedHandler) {
	headerReceivedHandlerList = append(headerReceivedHandlerList, &headerReceivedDefine{
		handlerID: handlerID,
		h:         h,
	})
}

// RegisterHeaderReceivedHandler registers HeaderReceivedHandler function with serviceID
func RegisterHeaderReceivedHandler(funcID string, h HeaderReceivedHandler) {
	appendHeaderReceivedDefine(funcID, h)
}

type virtualRouter struct {
	rawRouter  *fasthttprouter.Router
	http3Mux   *pure.Mux
	serverName string
}

func builtinServiceAPIDocument(svr *hybsHttpServer) ServiceHandler {
	return func(ctx Ctx) {
		var docPath = ctx.ConstStringValue("doc_path")
		if ctx.FormString("location") == "" && ctx.FormString("method") == "" {
			builtinServiceDocListPage(svr, ctx.(*hybsCtx), docPath)
		} else {
			builtinServiceDocDetailsPage(svr, ctx.(*hybsCtx))
		}
	}
}

func init() {
	serviceDefineList = make([]*serviceDefine, 0)
	headerReceivedHandlerList = make([]*headerReceivedDefine, 0)
	middlewareDefineList = make([]*middlewareDefine, 0)
}

func builtinServiceDocListPage(svr *hybsHttpServer, ctx *hybsCtx, docPath string) {
	// generate html code of api document home page
	var tableDoc = "<tr><td>URI</td><td>METHOD</td><td>Description</td><td>Service Name</td></tr>\n"
	for _, controller := range svr.controllerConfig {
		// skip hidden APIs
		if controller.Base.HideDocument {
			continue
		}
		// skip APIs not belongs t6 root path
		if !strings.HasPrefix(controller.Location, docPath) {
			continue
		}
		// write html code
		var locationHref = fmt.Sprintf(`<a href="%s?location=%s">%s</a>`,
			ctx.RequestURI(), controller.Location, controller.Location)
		tableDoc += fmt.Sprintf("<tr><td>%s</td><td></td><td>%s</td><td></td></tr>\n",
			locationHref, controller.Description)
		for _, service := range controller.Services {
			var methodHref = fmt.Sprintf(`<a href="%s?location=%s&method=%s">%s</a>`,
				ctx.RequestURI(), controller.Location, service.Method, service.Method)
			var serviceHref = fmt.Sprintf(`<a href="%s?location=%s&method=%s">%s</a>`,
				ctx.RequestURI(), controller.Location, service.Method, service.ServiceID)
			tableDoc += fmt.Sprintf("<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>\n",
				locationHref, methodHref, service.Description, serviceHref)
		}
	}
	htmlDoc := fmt.Sprintf(`<!DOCTYPE html>
<html>
	<head>
		<title>Welcome to hayabusa HttpServer!</title>
	</head>
	<body>
		<h1>Welcome to hayabusa server</h1>
		<table><tr><td>App:%s</td><td>Env:%s</td><td>ServerID:%s</td></tr></table>
		<p>API Resource List:</p>
		<table border="1">
		%s
		</table>
	</body>
<html>
`, svr.engine.config.AppName, svr.engine.config.Env, svr.httpConfig.ID, tableDoc)
	bytes := []byte(htmlDoc)
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.SetResponseBody(bytes)
}

func builtinServiceDocDetailsPage(svr *hybsHttpServer, ctx *hybsCtx) {
	location, method := ctx.FormString("location"), ctx.FormString("method")
	// combine configuration items by tree path
	configList := svr.controllerConfigTree.combine(location)
	document := fmt.Sprintf("<h1>URI:%s</h1>", location)
	printParamConfigFn := func(params []controllerParamConfigItem) (doc string) {
		for _, param := range params {
			doc += fmt.Sprintf("<b><i>%s</i></b></br>", param.Name)
			if param.Description != "" {
				doc += fmt.Sprintf("%s</br>", param.Description)
			}
			for _, reg := range param.Allow {
				doc += fmt.Sprintf("Allow:%s</br>", reg)
			}
			for _, reg := range param.Deny {
				doc += fmt.Sprintf("Deny:%s</br>", reg)
			}
			if param.Example != nil {
				doc += fmt.Sprintf("<b>Example</b></br>%v</br>", param.Example)
			}
		}
		return
	}
	printConfigBaseFn := func(config *controllerConfigBase) (doc string) {
		if len(config.Allow) > 0 {
			doc += fmt.Sprintf("<h3>IP Address White List</h3>")
		}
		for _, allow := range config.Allow {
			doc += fmt.Sprintf("%s</br>", allow)
		}
		if len(config.Deny) > 0 {
			doc += fmt.Sprintf("<h3>IP Address Black List</h3>")
		}
		for _, deny := range config.Deny {
			doc += fmt.Sprintf("%s</br>", deny)
		}
		if len(config.HeaderParams) > 0 {
			doc += fmt.Sprintf("<h3>Header Parameters</h3>")
			doc += printParamConfigFn(config.PathParams)
		}
		if len(config.PathParams) > 0 {
			doc += fmt.Sprintf("<h3>Path Parameters</h3>")
			doc += printParamConfigFn(config.PathParams)
		}
		if len(config.QueryArgs) > 0 {
			doc += fmt.Sprintf("<h3>Query Parameters</h3>")
			doc += printParamConfigFn(config.QueryArgs)
		}
		if len(config.FormArgs) > 0 {
			doc += fmt.Sprintf("<h3>Form Parameters</h3>")
			doc += printParamConfigFn(config.FormArgs)
		}
		if len(config.Middlewares) > 0 {
			doc += fmt.Sprintf("<h3>Middlewares</h3>")
			for _, middlewareName := range config.Middlewares {
				doc += fmt.Sprintf("%s</br>", middlewareName)
			}
		}
		if len(config.Response) > 0 {
			doc += fmt.Sprintf("<h3>Response</h3>")
			for _, response := range config.Response {
				doc += fmt.Sprintf("<p><b>%d %s</b></p>", response.StatusCode, fasthttp.StatusMessage(response.StatusCode))
				if len(response.Description) > 0 {
					doc += fmt.Sprintf("%s</br>", response.Description)
				}
				for _, responseField := range response.Fields {
					doc += fmt.Sprintf("<b>Field:</b><i>%s</i></br>", responseField.Name)
					if responseField.Description != "" {
						doc += fmt.Sprintf("%s</br>", responseField.Description)
					}
					if responseField.Example != "" {
						doc += fmt.Sprintf("例：%v</br>", responseField.Example)
					}
				}
			}
		}
		if len(config.Links) > 0 {
			doc += fmt.Sprintf("<h3>Links</h3>")
		}
		for _, link := range config.Links {
			doc += fmt.Sprintf(`<b>%s</b>%s</br>`, link.Service, link.Href)
		}
		return
	}
	for current := configList; current != nil; current = current.next {
		if current.depth < 1 {
			continue
		}
		if current.next != nil && len(current.configItem.Base.Middlewares) < 1 {
			continue
		}
		controller := current.configItem
		document += fmt.Sprintf("<h2>Location:%s</h2>", controller.Location)
		if controller.Description != "" {
			document += fmt.Sprintf("%s</br>", controller.Description)
		}
		document += printConfigBaseFn(&controller.Base)
		if !strings.EqualFold(location, current.configItem.Location) {
			document += "<p></p>"
			continue
		}
		for _, service := range controller.Services {
			if method != "" && !strings.EqualFold(service.Method, method) {
				continue
			}
			document += fmt.Sprintf("<h2>%s</h2>", service.Method)
			document += fmt.Sprintf(`<h3>Service:%s</h3>`, service.ServiceID)
			document += fmt.Sprintf("%s</br>", service.Description)
			document += printConfigBaseFn(&service.Base)
		}
		document += "<p></p>"
	}

	document += fmt.Sprintf(`<a href="%s">←Back</a></br>`, ctx.Path())
	var htmlDoc = fmt.Sprintf(`<!DOCTYPE html>
<html>
	<head>
		<title>Welcome to hayabusa HttpServer!</title>
	</head>
	<body>
%s
	</body>
<html>
`, document)
	var bytes = []byte(htmlDoc)
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.SetResponseBody(bytes)
}
