package realtime

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestController_RegisterMiddlewaresHandlers(t *testing.T) {
	server := NewTestRealtimeServer(t, NewTestEngine()).(*serverImpl)
	var value = 0

	RegisterHandler("test-0001/16", func(ctx Ctx) {
		value |= 0x01
	})
	server.controllers = append(server.controllers, controllerConfigItem{
		Event:       "request",
		Code:        "0001",
		Description: "test-0001",
		Middlewares: make([]string, 0),
		Handler:     "test-0001/16",
	})

	RegisterHandler("test-0004/16", func(ctx Ctx) {
		value |= 0x02
	})
	server.controllers = append(server.controllers, controllerConfigItem{
		Event:       "request",
		Code:        "0004",
		Description: "test-0004",
		Middlewares: make([]string, 0),
		Handler:     "test-0004/16",
	})

	RegisterHandler("test-0014/16", func(ctx Ctx) {
		value |= 0x04
	})
	server.controllers = append(server.controllers, controllerConfigItem{
		Event:       "request",
		Code:        "0014",
		Description: "test-0014",
		Middlewares: make([]string, 0),
		Handler:     "test-0014/16",
	})

	RegisterMiddleware("test-0000/16", func(h Handler) Handler {
		return func(ctx Ctx) {
			value |= 0x10
			h(ctx)
		}
	})
	server.controllers = append(server.controllers, controllerConfigItem{
		Event:       "request",
		Code:        "0000",
		Bits:        "16",
		Description: "test-0000/16",
		Middlewares: []string{"test-0000/16"},
	})

	RegisterMiddleware("test-0000/14", func(h Handler) Handler {
		return func(ctx Ctx) {
			value |= 0x20
			h(ctx)
		}
	})
	server.controllers = append(server.controllers, controllerConfigItem{
		Event:       "request",
		Code:        "0000",
		Bits:        "14",
		Description: "test-0000/14",
		Middlewares: []string{"test-0000/14"},
	})

	RegisterMiddleware("test-0010/15", func(h Handler) Handler {
		return func(ctx Ctx) {
			value |= 0x40
			h(ctx)
		}
	})
	server.controllers = append(server.controllers, controllerConfigItem{
		Event:       "request",
		Code:        "0010",
		Bits:        "15",
		Description: "test-0010/15",
		Middlewares: []string{"test-0010/15"},
	})

	RegisterMiddleware("test-0010/12", func(h Handler) Handler {
		return func(ctx Ctx) {
			value |= 0x80
			h(ctx)
		}
	})
	server.controllers = append(server.controllers, controllerConfigItem{
		Event:       "request",
		Code:        "0010",
		Bits:        "12",
		Description: "test-0010/12",
		Middlewares: []string{"test-0010/12"},
	})

	err := server.registerHandlers()
	require.Nil(t, err)

	value = 0
	c := &ctx{}
	server.handlerTable[0x0001](c)
	assert.Equal(t, 0x20|0x01, value)

	value = 0
	c = &ctx{}
	server.handlerTable[0x0004](c)
	assert.Equal(t, 0x02, value)

	value = 0
	c = &ctx{}
	server.handlerTable[0x0014](c)
	assert.Equal(t, 0x40|0x04, value)
}
