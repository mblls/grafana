package ngalert

import (
	"context"
	"errors"
	"fmt"

	"golang.org/x/sync/errgroup"

	"github.com/grafana/grafana/pkg/services/ngalert/schedule"
)

// EvaluationCoordinator determines whether this instance should evaluate alert rules.
// In HA single-node evaluation mode, only one node in the cluster evaluates rules.
type EvaluationCoordinator interface {
	// ShouldEvaluate returns true if this node should evaluate alert rules.
	ShouldEvaluate() bool
	// Updates returns a channel that emits the current evaluation decision immediately,
	// then emits only when the decision changes. The channel is closed when ctx is done.
	Updates(ctx context.Context) <-chan bool
}

// evaluationRunner encapsulates the lifecycle of alert rule evaluation.
// It provides clean Start/Stop/Done semantics and is designed to be used
// only from a single goroutine (runEvaluationLoop).
type evaluationRunner struct {
	ng                    *AlertNG
	needsPersisterRefresh bool // Set by Stop() to refresh persister on next Start()
	cancel                context.CancelFunc
	done                  chan error    // Receives error on crash, closed on clean exit
	stopped               chan struct{} // Closed when goroutine exits (for Stop to wait)
}

// Start begins evaluation.
// On first start, uses the persister created in init(). After a stop/start
// cycle, creates a fresh persister to avoid reusing a stopped one.
func (r *evaluationRunner) Start(ctx context.Context) {
	if r.cancel != nil {
		return
	}

	if r.needsPersisterRefresh {
		statePersister := initStatePersister(r.ng.Cfg.UnifiedAlerting, r.ng.stateManagerCfg, r.ng.FeatureToggles)
		r.ng.stateManager.SetPersister(statePersister)
		r.needsPersisterRefresh = false
	}

	r.ng.stateManager.Warm(ctx, r.ng.store, r.ng.store, r.ng.StartupInstanceReader)
	if r.ng.schedule == nil {
		r.ng.schedule = schedule.NewScheduler(r.ng.schedCfg, r.ng.stateManager)
	}

	r.ng.Log.Info("Starting alert rule evaluation")

	schedCtx, cancel := context.WithCancel(ctx)
	r.cancel = cancel
	r.done = make(chan error, 1)
	r.stopped = make(chan struct{})

	go r.run(schedCtx)
}

// run is the internal goroutine that runs the scheduler and state manager.
func (r *evaluationRunner) run(ctx context.Context) {
	defer close(r.stopped)
	defer close(r.done)

	g, gCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		err := r.ng.schedule.Run(gCtx)
		if err == nil && gCtx.Err() == nil {
			return fmt.Errorf("scheduler stopped unexpectedly")
		}
		return err
	})
	g.Go(func() error { return r.ng.stateManager.Run(gCtx) })

	if err := g.Wait(); err != nil && !errors.Is(err, context.Canceled) {
		r.done <- err
	}
}

// Stop halts evaluation and blocks until stopped.
func (r *evaluationRunner) Stop() {
	if r.cancel == nil {
		return
	}
	r.ng.Log.Info("Stopping alert rule evaluation")
	r.cancel()
	<-r.stopped // Wait on stopped, not done (preserves errors for caller)
	r.ng.stateManager.ClearCache()
	r.ng.schedule = nil
	r.cancel = nil
	r.done = nil
	r.stopped = nil
	r.needsPersisterRefresh = true // Next Start() needs a fresh persister
}

// Done returns error channel. Nil when not running (blocks in select).
func (r *evaluationRunner) Done() <-chan error {
	return r.done
}

func (ng *AlertNG) runEvaluationLoop(ctx context.Context) error {
	runner := &evaluationRunner{ng: ng}
	updates := ng.evaluationCoordinator.Updates(ctx)

	var shouldEvaluate bool
	for {
		select {
		case <-ctx.Done():
			if shouldEvaluate {
				runner.Stop()
			}
			return nil
		case err, ok := <-runner.Done():
			if ok {
				return err
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("evaluation stopped unexpectedly")
		case newShouldEvaluate, ok := <-updates:
			if !ok {
				if shouldEvaluate {
					runner.Stop()
				}
				return nil
			}
			if newShouldEvaluate && !shouldEvaluate {
				runner.Start(ctx)
			} else if !newShouldEvaluate && shouldEvaluate {
				runner.Stop()
			}
			shouldEvaluate = newShouldEvaluate
		}
	}
}
