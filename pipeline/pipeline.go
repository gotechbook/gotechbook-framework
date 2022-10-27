package pipeline

import (
	"context"
	"github.com/gotechbook/gotechbook-framework-logger"
)

type HandlerFunc func(ctx context.Context, in interface{}) (c context.Context, out interface{}, err error)

type AfterHandlerFunc func(ctx context.Context, out interface{}, err error) (interface{}, error)

type Channel struct {
	Handlers []HandlerFunc
}

type AfterChannel struct {
	Handlers []AfterHandlerFunc
}

type HandlerHooks struct {
	BeforeHandler *Channel
	AfterHandler  *AfterChannel
}

func NewHandlerHooks() *HandlerHooks {
	return &HandlerHooks{
		BeforeHandler: NewChannel(),
		AfterHandler:  NewAfterChannel(),
	}
}

func NewChannel() *Channel {
	return &Channel{Handlers: []HandlerFunc{}}
}

func NewAfterChannel() *AfterChannel {
	return &AfterChannel{Handlers: []AfterHandlerFunc{}}
}

func (p *Channel) ExecuteBeforePipeline(ctx context.Context, data interface{}) (context.Context, interface{}, error) {
	var err error
	res := data
	if len(p.Handlers) > 0 {
		for _, h := range p.Handlers {
			ctx, res, err = h(ctx, res)
			if err != nil {
				logger.Log.Debugf("go-tech-book-framework/handler: broken pipeline: %s", err.Error())
				return ctx, res, err
			}
		}
	}
	return ctx, res, nil
}
func (p *Channel) PushFront(h HandlerFunc) {
	hs := make([]HandlerFunc, len(p.Handlers)+1)
	hs[0] = h
	copy(hs[1:], p.Handlers)
	p.Handlers = hs
}
func (p *Channel) PushBack(h HandlerFunc) {
	p.Handlers = append(p.Handlers, h)
}
func (p *Channel) Clear() {
	p.Handlers = make([]HandlerFunc, 0)
}

func (p *AfterChannel) Clear() {
	p.Handlers = make([]AfterHandlerFunc, 0)
}
func (p *AfterChannel) PushFront(h AfterHandlerFunc) {
	hs := make([]AfterHandlerFunc, len(p.Handlers)+1)
	hs[0] = h
	copy(hs[1:], p.Handlers)
	p.Handlers = hs
}
func (p *AfterChannel) PushBack(h AfterHandlerFunc) {
	p.Handlers = append(p.Handlers, h)
}
func (p *AfterChannel) ExecuteAfterPipeline(ctx context.Context, res interface{}, err error) (interface{}, error) {
	ret := res
	if len(p.Handlers) > 0 {
		for _, h := range p.Handlers {
			ret, err = h(ctx, ret, err)
		}
	}
	return ret, err
}
