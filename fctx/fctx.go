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
	if ok {
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

func WithParent(parent context.Context, traceID string) *Context {
	if p, ok := parent.(*Context); ok {
		traceID = p.traceID + ":" + traceID
	}
	return &Context{
		ctx:     parent,
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
	c.Logger.With(zap.String("id", c.traceID)).Debugf(msg, args...)
}

func (c *Context) Debug(msg string) {
	c.Logger.With(zap.String("id", c.traceID)).Debug(msg)
}

func (c *Context) Debugf(msg string, args ...interface{}) {
	c.Logger.With(zap.String("id", c.traceID)).Debugf(msg, args...)
}

func (c *Context) Info(msg string) {
	c.Logger.With(zap.String("id", c.traceID)).Info(msg)
}

func (c *Context) Infof(msg string, args ...interface{}) {
	c.Logger.With(zap.String("id", c.traceID)).Infof(msg, args...)
}

func (c *Context) Warn(msg string) {
	c.Logger.With(zap.String("id", c.traceID)).Warn(msg)
}

func (c *Context) Warnf(msg string, args ...interface{}) {
	c.Logger.With(zap.String("id", c.traceID)).Warnf(msg, args...)
}

func (c *Context) Error(msg string) {
	c.Logger.With(zap.String("id", c.traceID)).Error(msg)
}

func (c *Context) Errorf(msg string, args ...interface{}) {
	c.Logger.With(zap.String("id", c.traceID)).Errorf(msg, args...)
}

func (c *Context) Panic(msg string) {
	c.Logger.With(zap.String("id", c.traceID)).Panic(msg)
}

func (c *Context) Criticalf(msg string, args ...interface{}) {
	c.Logger.With(zap.String("id", c.traceID)).Panicf(msg, args...)
}

func (c *Context) Fatal(msg string) {
	c.Logger.With(zap.String("id", c.traceID)).Fatal(msg)
}

func (c *Context) Fatalf(msg string, args ...interface{}) {
	c.Logger.With(zap.String("id", c.traceID)).Fatalf(msg, args...)
}
