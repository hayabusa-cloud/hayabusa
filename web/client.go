package web

import (
	"time"

	"github.com/hayabusa-cloud/hayabusa/utils"
	jsoniter "github.com/json-iterator/go"
	"github.com/valyala/fasthttp"
)

// Client represents http client for server to server communication
type Client struct {
	Timeout   time.Duration
	userValue utils.KVPairs
}

func (c *Client) Request(url string, method string, args *fasthttp.Args, respBody interface{}) (statusCode int, err error) {
	var req = fasthttp.AcquireRequest()
	defer fasthttp.ReleaseRequest(req)
	var resp = fasthttp.AcquireResponse()
	defer fasthttp.ReleaseResponse(resp)
	req.SetRequestURI(url)
	req.Header.SetMethod(method)
	if args != nil {
		req.SetBody(args.QueryString())
	}
	if c.Timeout > 0 {
		if err = fasthttp.DoTimeout(req, resp, c.Timeout); err != nil {
			return 0, err
		}
	} else {
		if err = fasthttp.Do(req, resp); err != nil {
			return 0, err
		}
	}
	statusCode = resp.StatusCode()
	if respBody == nil {
		return
	}
	if err = jsoniter.Unmarshal(resp.Body(), respBody); err != nil {
		return 0, err
	}
	return
}
func (c *Client) Get(url string, args *fasthttp.Args, respBody interface{}) (statusCode int, err error) {
	return c.Request(url, fasthttp.MethodGet, args, respBody)
}
func (c *Client) Post(url string, args *fasthttp.Args, respBody interface{}) (statusCode int, err error) {
	return c.Request(url, fasthttp.MethodPost, args, respBody)
}
func (c *Client) Put(url string, args *fasthttp.Args, respBody interface{}) (statusCode int, err error) {
	return c.Request(url, fasthttp.MethodPut, args, respBody)
}
func (c *Client) Delete(url string, respBody interface{}) (statusCode int, err error) {
	return c.Request(url, fasthttp.MethodDelete, nil, respBody)
}
func (c *Client) Do(req *fasthttp.Request, resp *fasthttp.Response) (err error) {
	if c.Timeout > 0 {
		return fasthttp.DoTimeout(req, resp, c.Timeout)
	}
	return fasthttp.Do(req, resp)
}

// DefaultHttpClient is the default http client for server to server communication
var DefaultHttpClient = &Client{
	Timeout:   time.Second * 2,
	userValue: make(utils.KVPairs, 0),
}
