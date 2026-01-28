package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type mockPositionProvider struct {
	position int
}

func (m *mockPositionProvider) Position() int {
	return m.position
}

func TestNewEvaluationCoordinator(t *testing.T) {
	t.Run("returns error when cluster is nil", func(t *testing.T) {
		coordinator, err := NewEvaluationCoordinator(nil)
		require.ErrorContains(t, err, "cluster position provider is required")
		require.Nil(t, coordinator)
	})

	t.Run("succeeds with valid cluster", func(t *testing.T) {
		coordinator, err := NewEvaluationCoordinator(&mockPositionProvider{position: 0})
		require.NoError(t, err)
		require.NotNil(t, coordinator)
	})
}

func TestEvaluationCoordinator_ShouldEvaluate(t *testing.T) {
	testCases := []struct {
		name     string
		position int
		expected bool
	}{
		{"position 0 should evaluate", 0, true},
		{"position 1 should not evaluate", 1, false},
		{"position 2 should not evaluate", 2, false},
		{"negative position should not evaluate", -1, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			coordinator, err := NewEvaluationCoordinator(&mockPositionProvider{position: tc.position})
			require.NoError(t, err)
			require.Equal(t, tc.expected, coordinator.ShouldEvaluate())
		})
	}
}

func TestNoopEvaluationCoordinator_ShouldEvaluate(t *testing.T) {
	coordinator := NewNoopEvaluationCoordinator()
	require.True(t, coordinator.ShouldEvaluate())
}
