// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package task

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type (
	// Manager is responsible for managing a group of tasks initiated during
	// tests. The interface is designed for the test framework to control tasks.
	// Typically, tests will only interact, and be provided with the smaller
	// Tasker interface to start tasks.
	Manager interface {
		Tasker
		Terminate(*logger.Logger)
		CompletedEvents() <-chan Event
	}

	// Event represents the result of a task execution.
	Event struct {
		Name            string
		Err             error
		TriggeredByTest bool
	}

	manager struct {
		group  ctxgroup.Group
		ctx    context.Context
		logger *logger.Logger
		events chan Event
		id     atomic.Uint32
		mu     struct {
			syncutil.Mutex
			cancelFns []context.CancelFunc
		}
	}
)

func NewManager(ctx context.Context, l *logger.Logger) Manager {
	g := ctxgroup.WithContext(ctx)
	return &manager{
		group:  g,
		ctx:    ctx,
		logger: l,
		events: make(chan Event),
	}
}

func (m *manager) defaultOptions() []Option {
	// The default panic handler simply returns the panic as an error.
	defaultPanicHandlerFn := func(_ context.Context, name string, l *logger.Logger, r interface{}) error {
		return fmt.Errorf("panic: %v", r)
	}
	// The default error handler simply returns the error as is.
	defaultErrorHandlerFn := func(_ context.Context, name string, l *logger.Logger, err error) error {
		return err
	}
	return []Option{
		Name(fmt.Sprintf("task-%d", m.id.Add(1))),
		Logger(m.logger),
		PanicHandler(defaultPanicHandlerFn),
		ErrorHandler(defaultErrorHandlerFn),
	}
}

func (m *manager) GoWithCancel(fn Func, opts ...Option) context.CancelFunc {
	opt := CombineOptions(OptionList(m.defaultOptions()...), OptionList(opts...))
	groupCtx, cancel := context.WithCancel(m.ctx)
	var expectedContextCancellation atomic.Bool

	// internalFunc is a wrapper around the user-provided function that
	// handles panics and errors.
	internalFunc := func(l *logger.Logger) (retErr error) {
		defer func() {
			if r := recover(); r != nil {
				retErr = opt.PanicHandler(groupCtx, opt.Name, l, r)
			}
			retErr = opt.ErrorHandler(groupCtx, opt.Name, l, retErr)
		}()
		retErr = fn(groupCtx, l)
		return retErr
	}

	m.group.Go(func() error {
		l, err := opt.L(opt.Name)
		if err != nil {
			return err
		}
		err = internalFunc(l)
		event := Event{
			Name: opt.Name,
			Err:  err,
			// TriggeredByTest is set to true if the task was canceled intentionally,
			// by the test, and we encounter an error. The assumption is that we
			// expect the error to have been caused by the cancelation, hence the
			// error above was not caused by a failure. This ensures we don't register
			// a test failure if the task was meant to be canceled. It's possible that
			// `expectedContextCancellation` could be set before the context is
			// canceled, thus we also ensure that the context is canceled.
			TriggeredByTest: err != nil && IsContextCanceled(groupCtx) && expectedContextCancellation.Load(),
		}

		// Do not send the event if the parent context is canceled. The test is
		// already aware of the cancelation and sending an event would be redundant.
		// For instance, a call to test.Fatal would already have captured the error
		// and canceled the context.
		if IsContextCanceled(m.ctx) {
			return nil
		}
		m.events <- event
		return err
	})

	taskCancelFn := func() {
		expectedContextCancellation.Store(true)
		cancel()
	}
	// Collect all taskCancelFn(s) so that we can explicitly stop all tasks when
	// the tasker is terminated.
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mu.cancelFns = append(m.mu.cancelFns, taskCancelFn)
	return taskCancelFn
}

func (m *manager) Go(fn Func, opts ...Option) {
	_ = m.GoWithCancel(fn, opts...)
}

// Terminate will call the stop functions for every task started during the
// test. Returns when all task functions have returned, or after a 5-minute
// timeout, whichever comes first. If the timeout is reached, the function logs
// a warning message and returns.
func (m *manager) Terminate(l *logger.Logger) {
	func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, cancel := range m.mu.cancelFns {
			cancel()
		}
	}()

	doneCh := make(chan error)
	go func() {
		defer close(doneCh)
		_ = m.group.Wait()
	}()

	WaitForChannel(doneCh, "tasks", l)
}

func (m *manager) CompletedEvents() <-chan Event {
	return m.events
}
