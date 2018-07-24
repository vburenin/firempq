package client

import "github.com/vburenin/firempq/export/proto"

// popOptions are used to set POP call parameters.
type popOptions struct {
	limit         int64
	waitTimeout   int64
	asyncCallback func(*Queue, error)
	asyncId       string
}

// NewPopOptions returns an empty instance of POP options.
func NewPopOptions() *popOptions {
	return &popOptions{}
}

// SetLimit sets user defined limit. Upper bound is defined by service config.
func (opts *popOptions) SetLimit(limit int64) *popOptions {
	opts.limit = limit
	return opts
}

// SetWaitTimeout sets wait timeout in milliseconds if no messages are available in the queue.
// Max limit defined by service config.
func (opts *popOptions) SetWaitTimeout(waitTimeout int64) *popOptions {
	opts.waitTimeout = waitTimeout
	return opts
}

func (opts *popOptions) SetAsyncCallback(cb func(*Queue, error)) *popOptions {
	opts.asyncCallback = cb
	return opts
}

func (opts *popOptions) sendOptions(t *TokenUtil) {
	if opts == nil {
		return
	}

	if opts.limit != 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.PrmLimit)
		t.SendInt(opts.limit)
	}
	if opts.waitTimeout > 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.PrmPopWait)
		t.SendInt(opts.waitTimeout)
	}

	if opts.asyncId != "" {
		t.SendSpace()
		t.SendToken(proto.PrmAsync)
		t.SendString(opts.asyncId)
	}
}

type popLockOptions struct {
	limit         int64
	waitTimeout   int64
	lockTimeout   int64
	asyncCallback func(*Queue, error)
	asyncId       string
}

func NewPopLockOptions() *popLockOptions {
	return &popLockOptions{lockTimeout: -1}
}

// SetLimit sets user defined limit. Upper bound is defined by service config.
func (opts *popLockOptions) SetLimit(limit int64) *popLockOptions {
	opts.limit = limit
	return opts
}

// SetWaitTimeout sets wait timeout in milliseconds if no messages are available in the queue.
// Max limit defined by service config.
func (opts *popLockOptions) SetWaitTimeout(waitTimeout int64) *popLockOptions {
	opts.waitTimeout = waitTimeout
	return opts
}

func (opts *popLockOptions) SetLockTimeout(lockTimeout int64) *popLockOptions {
	opts.lockTimeout = lockTimeout
	return opts
}

func (opts *popLockOptions) SetAsyncCallback(cb func(*Queue, error)) *popLockOptions {
	opts.asyncCallback = cb
	return opts
}

func (opts *popLockOptions) sendOptions(t *TokenUtil) {
	if opts == nil {
		return
	}
	if opts.limit != 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.PrmLimit)
		t.SendInt(opts.limit)
	}
	if opts.waitTimeout > 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.PrmPopWait)
		t.SendInt(opts.waitTimeout)
	}
	if opts.lockTimeout >= 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.PrmLockTimeout)
		t.SendInt(opts.lockTimeout)
	}
	if opts.asyncId != "" {
		t.SendSpace()
		t.SendToken(proto.PrmAsync)
		t.SendString(opts.asyncId)
	}
}

type QueueParams struct {
	msgTtl      int64
	maxSize     int64
	delay       int64
	popLimit    int64
	lockTimeout int64
}

func NewPQueueOptions() *QueueParams {
	return &QueueParams{
		msgTtl:      -1,
		maxSize:     -1,
		delay:       -1,
		popLimit:    -1,
		lockTimeout: -1,
	}
}

// SetMsgTtl sets default message ttl. Value must be positive.
func (opts *QueueParams) SetMsgTtl(v int64) *QueueParams {
	if v < 0 {
		panic("Value must be positive")
	}
	opts.msgTtl = v
	return opts
}

// SetMaxSize sets default max queue size. Value must be positive.
func (opts *QueueParams) SetMaxSize(v int64) *QueueParams {
	if v < 0 {
		panic("Value must be positive")
	}
	opts.maxSize = v
	return opts
}

// SetDelay sets default message delivery delay. Value must be positive.
func (opts *QueueParams) SetDelay(v int64) *QueueParams {
	if v < 0 {
		panic("Value must be positive")
	}
	opts.delay = v
	return opts
}

// SetPopLimit sets max number of pop attempts for each message . Value must be positive.
func (opts *QueueParams) SetPopLimit(v int64) *QueueParams {
	if v < 0 {
		panic("Value must be positive")
	}
	opts.popLimit = v
	return opts
}

// SetLockTimeout sets default pop lock timeout. Value must be positive.
func (opts *QueueParams) SetLockTimeout(v int64) *QueueParams {
	if v < 0 {
		panic("Value must be positive")
	}
	opts.lockTimeout = v
	return opts
}

func (opts *QueueParams) sendOptions(t *TokenUtil) {
	if opts == nil {
		return
	}
	if opts.msgTtl > 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.CPrmMsgTtl)
		t.SendInt(opts.msgTtl)
	}

	if opts.maxSize > 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.CPrmMaxMsgSize)
		t.SendInt(opts.maxSize)
	}

	if opts.delay >= 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.CPrmDeliveryDelay)
		t.SendInt(opts.delay)
	}

	if opts.popLimit >= 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.CPrmPopLimit)
		t.SendInt(opts.popLimit)
	}

	if opts.lockTimeout >= 0 {
		t.SendSpace()
		t.SendTokenWithSpace(proto.CPrmLockTimeout)
		t.SendInt(opts.lockTimeout)
	}
}
