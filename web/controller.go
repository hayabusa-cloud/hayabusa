package web

import (
	"fmt"
	"time"

	"github.com/go-playground/pure/v5"

	"github.com/valyala/fasthttp"
	"github.com/valyala/fasthttprouter"
)

// RegisterService registers Handler function with serviceID
func RegisterService(serviceID string, f Handler) {
	registerServiceImpl(serviceID, f)
}

// RegisterMiddleware registers Middleware function with middlewareID
func RegisterMiddleware(middlewareID string, m Middleware) {
	registerMiddlewareImpl(middlewareID, m)
}

// HeaderReceivedHandler is function to process request header
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

type controllerConfigList struct {
	configItem *controllerConfigItem
	next       *controllerConfigList
	depth      uint16
}

func (list *controllerConfigList) add(configItem *controllerConfigItem) *controllerConfigList {
	parent, current := &controllerConfigList{next: list, depth: list.depth}, list
	for current != nil {
		parent = current
		current = current.next
	}
	current = &controllerConfigList{configItem: configItem, next: nil, depth: list.depth + 1}
	parent.next = current
	return current
}

type controllerConfigTree struct {
	children   [0x80]*controllerConfigTree
	configList *controllerConfigList
}

//lint:ignore U1000 reserving
func (tree *controllerConfigTree) seek(path []byte) (node *controllerConfigTree, err error) {
	if path == nil || len(path) < 1 {
		return nil, fmt.Errorf("path cannot be nil or empty")
	}
	node = tree
	for i := 0; i < len(path); i++ {
		if node == nil {
			return nil, fmt.Errorf("already seek to leaf:%s", path[:i])
		}
		if path[i] > 0x7F {
			return nil, fmt.Errorf("character used in path must be 0~07f:%s", path)
		}
		node = node.children[path[i]]
	}
	return node, nil
}

func (tree *controllerConfigTree) add(path []byte, configItem *controllerConfigItem) {
	if path == nil || configItem == nil {
		return
	}
	current := tree
	for i := 0; i < len(path); i++ {
		if path[i] > 0x7F {
			// ignore invalid character
			continue
		}
		if current.children[path[i]] == nil {
			current.children[path[i]] = &controllerConfigTree{
				children:   [0x80]*controllerConfigTree{nil},
				configList: nil,
			}
		}
		current = current.children[path[i]]
	}
	if current.configList == nil {
		current.configList = &controllerConfigList{configItem: configItem, next: nil, depth: 0}
		return
	}
	current.configList.add(configItem)
}

func (tree *controllerConfigTree) combine(path string) (ret *controllerConfigList) {
	if path == "" || len(path) < 1 {
		return
	}
	var node = tree
	for i := 0; i < len(path); i++ {
		if path[i] > 0x7F {
			return ret
		}
		node = node.children[path[i]]
		if node == nil {
			return ret
		}
		var list = node.configList
		for ; list != nil; list = list.next {
			if list.configItem == nil {
				continue
			}
			if ret != nil {
				ret.add(list.configItem)
				continue
			}
			ret = &controllerConfigList{
				configItem: list.configItem,
				next:       nil,
			}
		}
	}
	return ret
}

type serviceTuple struct {
	serviceID string
	handler   Handler
}

var serviceTupleList []*serviceTuple

func registerServiceImpl(funcID string, handler Handler) {
	serviceTupleList = append(serviceTupleList, &serviceTuple{
		serviceID: funcID,
		handler:   handler,
	})
}

type headerReceivedHandlerTuple struct {
	handlerID string
	h         HeaderReceivedHandler
}

var headerReceivedHandlerTupleList []*headerReceivedHandlerTuple

func registerHeaderReceivedHandlerImpl(handlerID string, h HeaderReceivedHandler) {
	headerReceivedHandlerTupleList = append(headerReceivedHandlerTupleList, &headerReceivedHandlerTuple{
		handlerID: handlerID,
		h:         h,
	})
}

// RegisterHeaderReceivedHandler registers HeaderReceivedHandler function with funcID
func RegisterHeaderReceivedHandler(funcID string, h HeaderReceivedHandler) {
	registerHeaderReceivedHandlerImpl(funcID, h)
}

type routerMultiplexer struct {
	http11Router *fasthttprouter.Router
	http3Mux     *pure.Mux
	serverName   string
}

func init() {
	serviceTupleList = make([]*serviceTuple, 0)
	headerReceivedHandlerTupleList = make([]*headerReceivedHandlerTuple, 0)
	middlewareTupleList = make([]*middlewareTuple, 0)
}
