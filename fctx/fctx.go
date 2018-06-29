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
	Logger  *zap.SugaredLogger
}

func WithCancel(parent context.Context, traceID string) (*Context, context.CancelFunc) {
	p, ok := parent.(*Context)
	pctx := parent
	if !ok {
		pctx = p.ctx
	}
	ctx, cancel := context.WithCancel(pctx)
	return &Context{
		ctx:     ctx,
		traceID: traceID,
		Logger:  p.Logger,
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

func (c *Context) Trace(msg string, args ...interface{}) {
	c.Logger.Debugf(msg, args...)
}

func (c *Context) Debug(msg string) {
	c.Logger.Debug(msg)
}

func (c *Context) Debugf(msg string, args ...interface{}) {
	c.Logger.Debugf(msg, args...)
}

func (c *Context) Info(msg string) {
	c.Logger.Info(msg)
}

func (c *Context) Infof(msg string, args ...interface{}) {
	c.Logger.Infof(msg, args...)
}

func (c *Context) Warn(msg string) {
	c.Logger.Warn(msg)
}

func (c *Context) Warnf(msg string, args ...interface{}) {
	c.Logger.Warnf(msg, args...)
}

func (c *Context) Error(msg string) {
	c.Logger.Error(msg)
}

func (c *Context) Errorf(msg string, args ...interface{}) {
	c.Logger.Errorf(msg, args...)
}

func (c *Context) Panic(msg string) {
	c.Logger.Panic(msg)
}

func (c *Context) Criticalf(msg string, args ...interface{}) {
	c.Logger.Panicf(msg, args...)
}

func (c *Context) Fatal(msg string) {
	c.Logger.Fatal(msg)
}

func (c *Context) Fatalf(msg string, args ...interface{}) {
	c.Logger.Fatalf(msg, args...)
}
