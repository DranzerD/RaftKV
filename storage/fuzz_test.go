package storage

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/DranzerD/RaftKV/raft"
)

// memFile is an in-memory File for fuzzing.
type memFile struct{ data []byte }

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(f.data)) {
		return 0, io.EOF
	}
	n := copy(p, f.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	if need := int(off) + len(p); need > len(f.data) {
		f.data = append(f.data, make([]byte, need-len(f.data))...)
	}
	return copy(f.data[off:], p), nil
}

func (f *memFile) Truncate(size int64) error { f.data = f.data[:size]; return nil }
func (f *memFile) Sync() error               { return nil }
func (f *memFile) Close() error              { return nil }

// Recovery must never panic on arbitrary bytes. When it succeeds, the file
// must have been cut to exactly the records it replayed, so that opening it
// a second time gives the same state, and a new record must append cleanly.
func FuzzOpen(f *testing.F) {
	seed := &memFile{}
	w, _, _, err := Open(seed)
	if err != nil {
		f.Fatal(err)
	}
	w.Save(raft.HardState{Term: 1, Vote: 1}, []raft.Entry{{Index: 1, Term: 1, Data: []byte("a")}})
	w.Save(raft.HardState{Term: 2}, []raft.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 2, Data: []byte("b")}})
	w.SetSnapshot(2, 2)
	w.Save(raft.HardState{Term: 2}, nil)
	f.Add(append([]byte(nil), seed.data...))
	f.Add(seed.data[:len(seed.data)-3])
	f.Add([]byte{})
	f.Add(make([]byte, 64))

	f.Fuzz(func(t *testing.T, data []byte) {
		file := &memFile{data: append([]byte(nil), data...)}
		w, hs, ents, err := Open(file)
		if err != nil {
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("unexpected error kind: %v", err)
			}
			return
		}
		if !bytes.HasPrefix(data, file.data) || int64(len(file.data)) != w.Size() {
			t.Fatalf("recovery kept %d bytes, records end at %d", len(file.data), w.Size())
		}
		w2, hs2, ents2, err := Open(&memFile{data: append([]byte(nil), file.data...)})
		if err != nil || hs2 != hs || !sameLog(ents, ents2) || w2.Size() != w.Size() {
			t.Fatalf("second recovery differs: err=%v", err)
		}

		// Append one more entry and check it is recovered.
		snapIndex, _ := w.Snapshot()
		next := snapIndex + uint64(len(ents)) + 1
		e := raft.Entry{Index: next, Term: hs.Term, Data: []byte("x")}
		if err := w.Save(hs, []raft.Entry{e}); err != nil {
			t.Fatal(err)
		}
		_, _, ents3, err := Open(&memFile{data: file.data})
		if err != nil || len(ents3) != len(ents)+1 || !sameLog(ents3[len(ents):], []raft.Entry{e}) {
			t.Fatalf("appended entry not recovered: err=%v, %d entries", err, len(ents3))
		}
	})
}
