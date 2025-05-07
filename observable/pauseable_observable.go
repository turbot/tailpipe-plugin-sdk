package observable

import (
	"context"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"log/slog"
	"sync"
	"sync/atomic"
)

// PausableObservableImpl is an implementation of the PausableObservable interface -
// it extends ObservableImpl and adds Pause and Resume methods
// NotifyObservers will NOT proceed if pause has been called - it will block until Resume is called
// Thus, when the observer is paused, no events are sent
type PausableObservableImpl struct {
	ObservableImpl
	pausedAtomic atomic.Bool
	mu           sync.Mutex
	cond         *sync.Cond
}

// Pause pauses the observable, preventing any events from being sent to observers
func (p *PausableObservableImpl) Pause() error {
	slog.Info("PausableObservableImpl.Pause() called")

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.pausedAtomic.Load() {
		slog.Info("PausableObservableImpl.Pause - already paused")
		return nil // fast path, skip mutex
	}

	// create the condition variable if it doesn't exist
	p.ensureCond()
	p.pausedAtomic.Store(true)
	return nil
}

// Resume resumes the observable, allowing events to be sent to observers
func (p *PausableObservableImpl) Resume() error {
	slog.Info("PausableObservableImpl.Resume() called")

	p.mu.Lock()
	defer p.mu.Unlock()

	p.pausedAtomic.Store(false)

	if p.cond != nil {
		p.cond.Broadcast()
	}
	return nil
}

// create the condition variable if it doesn't exist
func (p *PausableObservableImpl) ensureCond() {
	if p.cond == nil {
		p.cond = sync.NewCond(&p.mu)
	}
}

// NotifyObservers overrides the base implementation
// It will block if Pause has been called until Resume is called
func (p *PausableObservableImpl) NotifyObservers(ctx context.Context, e events.Event) error {
	// if paused, clock until resumed
	p.BlockWhilePaused(ctx)

	// call base implementation
	return p.ObservableImpl.NotifyObservers(ctx, e)
}

func (p *PausableObservableImpl) BlockWhilePaused(ctx context.Context) {
	// fast path, skip mutex
	if !p.pausedAtomic.Load() {
		return
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	// Create notification channel for when pause is lifted
	waitDone := make(chan struct{})
	go func() {
		// Keep waiting while paused
		for p.pausedAtomic.Load() {
			// Wait releases mutex while waiting and reacquires it when woken
			p.cond.Wait()
			// Notify main goroutine that we're no longer paused
			waitDone <- struct{}{}
		}
	}()

	select {
	case <-waitDone:
		// Normal flow - pause was lifted via Resume()
	case <-ctx.Done():
		// signal to stop waiting - to avoid leaking the goroutine
		p.cond.Broadcast()
		// Context cancelled, let caller handle the error
		return
	}
}
