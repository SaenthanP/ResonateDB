package cluster_test

import (
	"testing"
	"time"

	"github.com/saenthan/resonatedb/internal/cluster"
	"github.com/saenthan/resonatedb/sim"
	"github.com/stretchr/testify/require"
)

func TestSWIM_FailureDetection(t *testing.T) {
	s := sim.NewSimulator(42, []string{"A", "B", "C"})

	s.Run(10, time.Second)

	s.Partition("A", "C")
	s.Partition("B", "C")

	s.Run(20, time.Second)

	require.Equal(t, cluster.Fail, s.NodeState("A", "C"))
	require.Equal(t, cluster.Fail, s.NodeState("B", "C"))
	require.Equal(t, cluster.Alive, s.NodeState("A", "B"))
}
