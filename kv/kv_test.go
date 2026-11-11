package kv

import (
	"reflect"
	"testing"
)

func TestOperations(t *testing.T) {
	s := NewStore()
	steps := []struct {
		cmd  Command
		want Result
	}{
		{Command{Op: OpGet, Key: "k"}, Result{}},
		{Command{Op: OpCAS, Key: "k", Expect: "", Value: "x"}, Result{}}, // no create via CAS
		{Command{Op: OpDelete, Key: "k"}, Result{}},
		{Command{Op: OpPut, Key: "k", Value: "1"}, Result{OK: true}},
		{Command{Op: OpGet, Key: "k"}, Result{Found: true, Value: "1"}},
		{Command{Op: OpCAS, Key: "k", Expect: "0", Value: "2"}, Result{Found: true, Value: "1"}},
		{Command{Op: OpCAS, Key: "k", Expect: "1", Value: "2"}, Result{OK: true}},
		{Command{Op: OpGet, Key: "k"}, Result{Found: true, Value: "2"}},
		{Command{Op: OpDelete, Key: "k"}, Result{OK: true}},
		{Command{Op: OpGet, Key: "k"}, Result{}},
	}
	for i, st := range steps {
		if got, status := s.Apply(st.cmd); got != st.want || status != Applied {
			t.Fatalf("step %d %v: got %+v (%v), want %+v", i, st.cmd.Op, got, status, st.want)
		}
	}
}

// A retried command returns its original result and does not run again,
// even when another client has changed the key in between.
func TestSessionsApplyOnce(t *testing.T) {
	s := NewStore()
	put := Command{Client: 1, Seq: 1, Op: OpPut, Key: "k", Value: "mine"}
	if _, st := s.Apply(put); st != Applied {
		t.Fatalf("first apply: %v", st)
	}
	s.Apply(Command{Client: 2, Seq: 1, Op: OpPut, Key: "k", Value: "theirs"})

	res, st := s.Apply(put)
	if st != Duplicate || !res.OK {
		t.Fatalf("retry: got %+v %v, want the saved result and Duplicate", res, st)
	}
	if got := s.Read("k"); got.Value != "theirs" {
		t.Fatalf("retry overwrote a later write: k = %q", got.Value)
	}

	// The saved result is the original one, not a recomputation.
	del := Command{Client: 1, Seq: 2, Op: OpDelete, Key: "k"}
	first, _ := s.Apply(del)
	again, st := s.Apply(del)
	if !first.OK || again != first || st != Duplicate {
		t.Fatalf("delete retry: first %+v, again %+v (%v)", first, again, st)
	}

	// Anything older than the latest command is dropped.
	if _, st := s.Apply(put); st != Stale {
		t.Fatalf("old command: %v, want Stale", st)
	}
	if s.Len() != 0 {
		t.Fatalf("stale command took effect: %v", s.Data())
	}
}

func TestEncodeDecode(t *testing.T) {
	cmds := []Command{
		{Client: 7, Seq: 9, Op: OpCAS, Key: "key", Value: "new", Expect: "old"},
		{Op: OpGet, Key: ""},
		{Client: 1<<64 - 1, Seq: 1<<64 - 1, Op: OpPut, Key: "k", Value: string(make([]byte, 5000))},
	}
	for _, c := range cmds {
		got, err := Decode(c.Encode())
		if err != nil || got != c {
			t.Fatalf("round trip of %+v: %+v, %v", c, got, err)
		}
	}
	good := cmds[0].Encode()
	for _, bad := range [][]byte{nil, []byte("cmd-1"), good[:len(good)-1], append(append([]byte{}, good...), 0)} {
		if _, err := Decode(bad); err == nil {
			t.Fatalf("decoded garbage %q", bad)
		}
	}
}

func TestSnapshotCarriesSessions(t *testing.T) {
	s := NewStore()
	s.Apply(Command{Client: 1, Seq: 4, Op: OpPut, Key: "a", Value: "1"})
	s.Apply(Command{Client: 2, Seq: 9, Op: OpDelete, Key: "a"})
	s.Apply(Command{Client: 3, Seq: 1, Op: OpPut, Key: "b", Value: ""})
	snap := s.Snapshot()

	r := NewStore()
	r.Apply(Command{Op: OpPut, Key: "junk", Value: "x"})
	if err := r.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(r.Data(), s.Data()) {
		t.Fatalf("data: %v, want %v", r.Data(), s.Data())
	}
	// A retry that reaches the restored node must still be recognised.
	if res, st := r.Apply(Command{Client: 2, Seq: 9, Op: OpDelete, Key: "a"}); st != Duplicate || !res.OK {
		t.Fatalf("retry after restore: %+v %v", res, st)
	}
	if string(r.Snapshot()) != string(snap) {
		t.Fatal("snapshot of a restored store differs: encoding is not deterministic")
	}
	for i := 0; i < len(snap); i++ {
		if err := NewStore().Restore(snap[:i]); err == nil {
			t.Fatalf("restored from a snapshot cut at byte %d", i)
		}
	}
}
