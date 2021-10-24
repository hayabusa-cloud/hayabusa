package web

import (
	"fmt"
	"strings"

	"github.com/valyala/fasthttp"
)

func builtinServiceDocument(svr *Server) Handler {
	return func(c Ctx) {
		var docPath = c.ConstStringValue("doc_path")
		if c.FormString("location") == "" && c.FormString("method") == "" {
			builtinServiceDocHomePage(svr, c.(*ctx), docPath)
		} else {
			builtinServiceDocDetailsPage(svr, c.(*ctx))
		}
	}
}

func builtinServiceDocHomePage(svr *Server, ctx *ctx, docPath string) {
	// generate html code of api document home page
	var tableDoc = "<tr><td>URI</td><td>Method</td><td>Description</td><td>Service ID</td></tr>\n"
	for _, controller := range svr.controllerConfig {
		// skip hidden APIs
		if controller.Base.HideDocument {
			continue
		}
		// skip APIs not under root path
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
		<title>Welcome to Hayabusa Server</title>
	</head>
	<body>
		<h1>Welcome to Hayabusa Server</h1>
		<table><tr><td>App:%s</td><td>Env:%s</td><td>ServerID:%s</td></tr></table>
		<table border="1" width="864px">
		%s
		</table>
	</body>
<html>
`, svr.engine.AppName(), svr.engine.Env(), svr.config.ID, tableDoc)
	bytes := []byte(htmlDoc)
	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("text/html; charset=utf-8")
	ctx.SetResponseBody(bytes)
}

func builtinServiceDocDetailsPage(svr *Server, ctx *ctx) {
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
			doc += "<h3>IP Address White List</h3>"
		}
		for _, allow := range config.Allow {
			doc += fmt.Sprintf("%s</br>", allow)
		}
		if len(config.Deny) > 0 {
			doc += "<h3>IP Address Black List</h3>"
		}
		for _, deny := range config.Deny {
			doc += fmt.Sprintf("%s</br>", deny)
		}
		if len(config.HeaderParams) > 0 {
			doc += "<h3>Header Parameters</h3>"
			doc += printParamConfigFn(config.PathParams)
		}
		if len(config.PathParams) > 0 {
			doc += "<h3>Path Parameters</h3>"
			doc += printParamConfigFn(config.PathParams)
		}
		if len(config.QueryArgs) > 0 {
			doc += "<h3>Query Parameters</h3>"
			doc += printParamConfigFn(config.QueryArgs)
		}
		if len(config.FormArgs) > 0 {
			doc += "<h3>Form Parameters</h3>"
			doc += printParamConfigFn(config.FormArgs)
		}
		if len(config.Middlewares) > 0 {
			doc += "<h3>Middlewares</h3>"
			for _, middlewareName := range config.Middlewares {
				doc += fmt.Sprintf("%s</br>", middlewareName)
			}
		}
		if len(config.Response) > 0 {
			doc += "<h3>Response</h3>"
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
			doc += "<h3>Links</h3>"
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
		<title>Welcome to Hayabusa Server!</title>
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
