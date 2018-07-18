package fctx

import (
	"context"
	"time"

	"github.com/vburenin/firempq/log"
	"go.uber.org/zap"
)

// FireMPQ context with the built-in logging

type Context struct {
	ctx     context.Context
	traceID string
	Logger  *zap.Logger
}

func WithCancel(parent context.Context, traceID string) (*Context, context.CancelFunc) {
	p, ok := parent.(*Context)
	pctx := parent
	if ok {
		pctx = p.ctx
	}
	ctx, cancel := context.WithCancel(pctx)
	return &Context{
		ctx:     ctx,
		traceID: traceID,
		Logger:  p.Logger.With(zap.String("id", traceID)),
	}, cancel
}

func Background(traceID string) *Context {
	return &Context{
		ctx:     context.Background(),
		traceID: traceID,
		Logger:  log.Logger,
	}
}

func Empty(traceID string) *Context {
	return &Context{
		ctx:     context.Background(),
		traceID: traceID,
		Logger:  log.Logger,
	}
}

func TODO(traceID string) *Context {
	return &Context{
		ctx:     context.TODO(),
		traceID: traceID,
		Logger:  log.Logger,
	}
}

func WithParent(parent context.Context, traceID string) *Context {
	if p, ok := parent.(*Context); ok {
		traceID = p.traceID + ":" + traceID
	}
	return &Context{
		ctx:     parent,
		traceID: traceID,
		Logger:  log.Logger.With(zap.String("id", traceID)),
	}
}

func (c *Context) Deadline() (deadline time.Time, ok bool) {
	return c.ctx.Deadline()
}

func (c *Context) Done() <-chan struct{} {
	return c.ctx.Done()
}

func (c *Context) Err() error {
	return c.ctx.Err()
}

func (c *Context) Value(key interface{}) interface{} {
	return c.ctx.Value(key)
}

func (c *Context) Trace(msg string, fields ...zap.Field) {
	c.Logger.Debug(msg, fields...)
}

func (c *Context) Debug(msg string, fields ...zap.Field) {
	c.Logger.Debug(msg, fields...)
}

func (c *Context) Info(msg string, fields ...zap.Field) {
	c.Logger.Info(msg, fields...)
}

func (c *Context) Warn(msg string, fields ...zap.Field) {
	c.Logger.Warn(msg, fields...)
}

func (c *Context) Error(msg string, fields ...zap.Field) {
	c.Logger.Error(msg, fields...)
}

func (c *Context) Panic(msg string, fields ...zap.Field) {
	c.Logger.Panic(msg, fields...)
}

func (c *Context) Fatal(msg string, fields ...zap.Field) {
	c.Logger.Fatal(msg, fields...)
}
