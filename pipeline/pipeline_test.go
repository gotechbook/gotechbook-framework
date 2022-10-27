package pipeline

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

var (
	handler1 = func(ctx context.Context, in interface{}) (context.Context, interface{}, error) {
		return ctx, in, errors.New("ohno")
	}
	handler2 = func(ctx context.Context, in interface{}) (context.Context, interface{}, error) {
		return ctx, nil, nil
	}
	p = &Channel{}
)

func TestPushFront(t *testing.T) {
	p.PushFront(handler1)
	p.PushFront(handler2)
	defer p.Clear()

	_, _, err := p.Handlers[0](nil, nil)
	assert.Nil(t, nil, err)
}

func TestPushBack(t *testing.T) {
	p.PushFront(handler1)
	p.PushBack(handler2)
	defer p.Clear()

	_, _, err := p.Handlers[0](nil, nil)
	assert.EqualError(t, errors.New("ohno"), err.Error())
}

func TestClear(t *testing.T) {
	p.PushFront(handler1)
	p.PushBack(handler2)
	assert.Len(t, p.Handlers, 2)
	p.Clear()
	assert.Len(t, p.Handlers, 0)
}
