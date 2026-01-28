package cluster

import (
	"context"
	"errors"
	"time"
)

const (
	// DefaultEvaluationCheckInterval is how often we check cluster position
	// to determine if this node should evaluate alert rules in single-node evaluation HA mode.
	DefaultEvaluationCheckInterval = 10 * time.Second
)

type ClusterPositionProvider interface {
	Position() int
}

// EvaluationCoordinator determines whether alert rule evaluation should occur
// based on cluster position. Only the node with position 0 evaluates rules.
type EvaluationCoordinator struct {
	cluster ClusterPositionProvider
}

func NewEvaluationCoordinator(cluster ClusterPositionProvider) (*EvaluationCoordinator, error) {
	if cluster == nil {
		return nil, errors.New("cluster position provider is required")
	}
	return &EvaluationCoordinator{cluster: cluster}, nil
}

// ShouldEvaluate returns true if this node should evaluate alert rules.
func (c *EvaluationCoordinator) ShouldEvaluate() bool {
	return c.cluster.Position() == 0
}

// Updates emits the current evaluation decision immediately and then on changes.
// It closes the channel when ctx is done.
func (c *EvaluationCoordinator) Updates(ctx context.Context) <-chan bool {
	updates := make(chan bool, 1)
	go func() {
		defer close(updates)

		current := c.ShouldEvaluate()
		sendLatest(updates, current)

		ticker := time.NewTicker(DefaultEvaluationCheckInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				next := c.ShouldEvaluate()
				if next == current {
					continue
				}
				current = next
				sendLatest(updates, current)
			}
		}
	}()
	return updates
}

func sendLatest(ch chan bool, value bool) {
	select {
	case ch <- value:
		return
	default:
	}
	select {
	case <-ch:
	default:
	}
	ch <- value
}

// NoopEvaluationCoordinator always returns true for ShouldEvaluate.
type NoopEvaluationCoordinator struct{}

func NewNoopEvaluationCoordinator() *NoopEvaluationCoordinator {
	return &NoopEvaluationCoordinator{}
}

func (c *NoopEvaluationCoordinator) ShouldEvaluate() bool {
	return true
}

// Updates emits true immediately. Since Noop always returns true (never changes),
// no further updates are emitted. The channel is closed when ctx is done.
func (c *NoopEvaluationCoordinator) Updates(ctx context.Context) <-chan bool {
	updates := make(chan bool, 1)
	updates <- true
	go func() {
		<-ctx.Done()
		close(updates)
	}()
	return updates
}
