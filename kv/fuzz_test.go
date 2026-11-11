package kv

import (
	"bytes"
	"testing"
)

// Decode must never panic, and anything it accepts must survive a round
// trip through Encode unchanged.
func FuzzDecode(f *testing.F) {
	f.Add(Command{Client: 1, Seq: 2, Op: OpPut, Key: "k", Value: "v"}.Encode())
	f.Add(Command{Client: 7, Seq: 9, Op: OpCAS, Key: "k", Value: "new", Expect: "old"}.Encode())
	f.Add([]byte{magic, byte(OpGet)})
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, b []byte) {
		c, err := Decode(b)
		if err != nil {
			return
		}
		if again := c.Encode(); !bytes.Equal(again, b) {
			t.Fatalf("Decode accepted %x but Encode gives %x", b, again)
		}
	})
}

// Restore must never panic on arbitrary bytes, must leave the store
// untouched when it refuses them, and anything it accepts must snapshot
// back to the same bytes.
func FuzzRestore(f *testing.F) {
	s := NewStore()
	s.Apply(Command{Client: 1, Seq: 1, Op: OpPut, Key: "a", Value: "1"})
	s.Apply(Command{Client: 2, Seq: 5, Op: OpCAS, Key: "a", Expect: "x", Value: "2"})
	f.Add(s.Snapshot())
	f.Add(NewStore().Snapshot())
	f.Add([]byte{1, 0, 0, 0})
	f.Fuzz(func(t *testing.T, b []byte) {
		st := NewStore()
		st.Apply(Command{Op: OpPut, Key: "before", Value: "restore"})
		if err := st.Restore(b); err != nil {
			if v := st.Read("before"); !v.Found || st.Len() != 1 {
				t.Fatalf("a refused snapshot changed the store")
			}
			return
		}
		// Duplicate keys or clients in the input collapse, so compare a
		// second round trip rather than the raw input.
		once := st.Snapshot()
		again := NewStore()
		if err := again.Restore(once); err != nil {
			t.Fatalf("Restore rejects its own Snapshot: %v", err)
		}
		if !bytes.Equal(again.Snapshot(), once) {
			t.Fatal("snapshot is not stable across a restore")
		}
	})
}
