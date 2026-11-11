package sim

import (
	"github.com/anishathalye/porcupine"

	"github.com/DranzerD/RaftKV/kv"
)

// keyState is the sequential specification's state for one key.
type keyState struct {
	found bool
	value string
}

type histOutput struct {
	res     kv.Result
	unknown bool // the client never saw an answer
}

// kvModel says what a single, non-replicated key-value store would do. It
// is written separately from kv.Store on purpose, so the check does not
// lean on the code under test.
var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		var order []string
		for _, op := range history {
			k := op.Input.(kv.Command).Key
			if _, ok := byKey[k]; !ok {
				order = append(order, k)
			}
			byKey[k] = append(byKey[k], op)
		}
		out := make([][]porcupine.Operation, 0, len(order))
		for _, k := range order {
			out = append(out, byKey[k])
		}
		return out
	},
	Init: func() interface{} { return keyState{} },
	Step: func(state, input, output interface{}) (bool, interface{}) {
		st := state.(keyState)
		cmd := input.(kv.Command)
		out := output.(histOutput)

		var want kv.Result
		next := st
		switch cmd.Op {
		case kv.OpGet:
			want = kv.Result{Found: st.found, Value: st.value}
		case kv.OpPut:
			want = kv.Result{OK: true}
			next = keyState{found: true, value: cmd.Value}
		case kv.OpDelete:
			want = kv.Result{OK: st.found}
			next = keyState{}
		case kv.OpCAS:
			if st.found && st.value == cmd.Expect {
				want = kv.Result{OK: true}
				next = keyState{found: true, value: cmd.Value}
			} else {
				want = kv.Result{Found: st.found, Value: st.value}
			}
		}
		return out.unknown || out.res == want, next
	},
}

// CheckLinearizable reports whether the client-observed history could have
// come from a single correct key-value store that executed each operation
// at one instant between its call and its return. An operation that never
// returned may have taken effect at any point after its call.
func CheckLinearizable(history []Operation) bool {
	var end uint64
	for _, op := range history {
		if op.Return > end {
			end = op.Return
		}
		if op.Call > end {
			end = op.Call
		}
	}
	ops := make([]porcupine.Operation, 0, len(history))
	for _, op := range history {
		po := porcupine.Operation{
			ClientId: int(op.Client) - 1,
			Input:    op.Cmd,
			Call:     int64(op.Call),
			Output:   histOutput{res: op.Result},
			Return:   int64(op.Return),
		}
		if !op.Done {
			po.Output = histOutput{unknown: true}
			po.Return = int64(end) + 1
		}
		ops = append(ops, po)
	}
	return porcupine.CheckOperations(kvModel, ops)
}
