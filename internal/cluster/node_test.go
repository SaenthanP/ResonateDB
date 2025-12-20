package cluster

import (
	"testing"

	pb "github.com/saenthan/resonatedb/proto-gen/cluster"
)

func protoUpdate(addr string, state ServerState, incarnation int) map[string]*pb.NodeUpdate {
	return map[string]*pb.NodeUpdate{
		addr: {
			Address:     addr,
			State:       state.ToProto(),
			Incarnation: int64(incarnation),
		},
	}
}

func TestMergeUpdates(t *testing.T) {
	tests := []struct {
		name          string
		initial       NodeUpdate
		incomingState ServerState
		incomingInc   int
		expectedState ServerState
		expectedInc   int
	}{
		{
			name: "Alive -> Suspect same incarnation",
			initial: NodeUpdate{
				Address:     "A",
				State:       Alive,
				Incarnation: 5,
			},
			incomingState: Suspect,
			incomingInc:   5,
			expectedState: Suspect,
			expectedInc:   5,
		},
		{
			name: "Suspect -> Suspect same incarnation",
			initial: NodeUpdate{
				Address:     "A",
				State:       Suspect,
				Incarnation: 5,
			},
			incomingState: Alive,
			incomingInc:   5,
			expectedState: Suspect,
			expectedInc:   5,
		},
		{
			name: "Fail -> Alive same incarnation (ignored)",
			initial: NodeUpdate{
				Address:     "A",
				State:       Fail,
				Incarnation: 5,
			},
			incomingState: Alive,
			incomingInc:   5,
			expectedState: Fail,
			expectedInc:   5,
		},
		{
			name: "Fail -> Alive higher incarnation (accept)",
			initial: NodeUpdate{
				Address:     "A",
				State:       Fail,
				Incarnation: 5,
			},
			incomingState: Alive,
			incomingInc:   6,
			expectedState: Alive,
			expectedInc:   6,
		},
		{
			name: "Alive -> Fail same incarnation",
			initial: NodeUpdate{
				Address:     "A",
				State:       Alive,
				Incarnation: 5,
			},
			incomingState: Fail,
			incomingInc:   5,
			expectedState: Fail,
			expectedInc:   5,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			node := &Node{
				Address: "self",
				updates: map[string]NodeUpdate{
					"A": tt.initial,
				},
			}

			incoming := protoUpdate("A", tt.incomingState, tt.incomingInc)
			node.mergeUpdates(incoming)

			got := node.updates["A"]
			if got.State != tt.expectedState {
				t.Errorf("state = %v, want %v", got.State, tt.expectedState)
			}
			if got.Incarnation != tt.expectedInc {
				t.Errorf("incarnation = %v, want %v", got.Incarnation, tt.expectedInc)
			}
		})
	}
}
