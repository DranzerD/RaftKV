package storage

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DranzerD/RaftKV/raft"
)

func ent(index, term uint64, data string) raft.Entry {
	e := raft.Entry{Index: index, Term: term}
	if data != "" {
		e.Data = []byte(data)
	}
	return e
}

func sameLog(a, b []raft.Entry) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Index != b[i].Index || a[i].Term != b[i].Term || a[i].Type != b[i].Type ||
			!bytes.Equal(a[i].Data, b[i].Data) {
			return false
		}
	}
	return true
}

func mustOpen(t *testing.T, path string) (*WAL, raft.HardState, []raft.Entry) {
	t.Helper()
	w, hs, ents, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { w.Close() })
	return w, hs, ents
}

func mustSave(t *testing.T, w *WAL, hs raft.HardState, ents ...raft.Entry) {
	t.Helper()
	if err := w.Save(hs, ents); err != nil {
		t.Fatal(err)
	}
}

func TestEmptyFile(t *testing.T) {
	_, hs, ents := mustOpen(t, filepath.Join(t.TempDir(), "wal"))
	if hs != (raft.HardState{}) || len(ents) != 0 {
		t.Fatalf("fresh wal returned %+v and %d entries", hs, len(ents))
	}
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _, _ := mustOpen(t, path)

	big := string(bytes.Repeat([]byte("x"), 100_000))
	mustSave(t, w, raft.HardState{Term: 1, Vote: 2})
	mustSave(t, w, raft.HardState{Term: 1, Vote: 2}, ent(1, 1, "a"), ent(2, 1, ""), ent(3, 1, big))
	mustSave(t, w, raft.HardState{Term: 3, Vote: 0}, raft.Entry{Index: 4, Term: 3, Type: raft.EntryNoop})
	w.Close()

	w, hs, ents := mustOpen(t, path)
	want := []raft.Entry{ent(1, 1, "a"), ent(2, 1, ""), ent(3, 1, big), {Index: 4, Term: 3, Type: raft.EntryNoop}}
	if hs != (raft.HardState{Term: 3}) || !sameLog(ents, want) {
		t.Fatalf("recovered %+v and %d entries", hs, len(ents))
	}

	// Appending after recovery continues the same log.
	mustSave(t, w, raft.HardState{Term: 3, Vote: 1}, ent(5, 3, "e"))
	w.Close()
	_, hs, ents = mustOpen(t, path)
	if hs != (raft.HardState{Term: 3, Vote: 1}) || !sameLog(ents, append(want, ent(5, 3, "e"))) {
		t.Fatalf("after reopen and append: %+v and %d entries", hs, len(ents))
	}
}

// A later record that starts inside the existing log replaces its suffix,
// the way a follower's log is overwritten by a new leader.
func TestOverwriteSuffix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _, _ := mustOpen(t, path)
	mustSave(t, w, raft.HardState{Term: 2}, ent(1, 1, "a"), ent(2, 2, "b"), ent(3, 2, "c"))
	mustSave(t, w, raft.HardState{Term: 3}, ent(2, 3, "B"))
	w.Close()

	_, hs, ents := mustOpen(t, path)
	if hs.Term != 3 || !sameLog(ents, []raft.Entry{ent(1, 1, "a"), ent(2, 3, "B")}) {
		t.Fatalf("recovered %+v %+v", hs, ents)
	}
}

// buildThree writes three records and returns the file's bytes plus the
// size of the first two records.
func buildThree(t *testing.T) (data []byte, twoRecords int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal")
	w, _, _ := mustOpen(t, path)
	mustSave(t, w, raft.HardState{Term: 1, Vote: 1}, ent(1, 1, "a"))
	mustSave(t, w, raft.HardState{Term: 1, Vote: 1}, ent(2, 1, "b"))
	twoRecords = w.Size()
	mustSave(t, w, raft.HardState{Term: 2, Vote: 3}, ent(3, 2, "third"), ent(4, 2, "fourth"))
	w.Close()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return data, twoRecords
}

func checkTwoRecords(t *testing.T, what string, damaged []byte, twoRecords int64) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "wal")
	if err := os.WriteFile(path, damaged, 0o644); err != nil {
		t.Fatal(err)
	}
	w, hs, ents, err := OpenFile(path)
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if hs != (raft.HardState{Term: 1, Vote: 1}) || !sameLog(ents, []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b")}) {
		t.Fatalf("%s: recovered %+v and %+v, want the first two records", what, hs, ents)
	}
	// The damaged tail must be gone from the file, not just ignored: stale
	// bytes left behind a shorter record are garbage that every later
	// recovery would have to step over.
	if st, _ := os.Stat(path); st.Size() != twoRecords {
		t.Fatalf("%s: file is %d bytes after recovery, want the %d bytes of good records", what, st.Size(), twoRecords)
	}
	if w.Size() != twoRecords {
		t.Fatalf("%s: wal reports %d durable bytes, want %d", what, w.Size(), twoRecords)
	}
	if err := w.Save(raft.HardState{Term: 5}, []raft.Entry{ent(3, 5, "new")}); err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	size := w.Size()
	w.Close()
	if st, _ := os.Stat(path); st.Size() != size || size <= twoRecords {
		t.Fatalf("%s: file is %d bytes, wal reports %d, want more than %d and equal", what, st.Size(), size, twoRecords)
	}
	w, hs, ents, err = OpenFile(path)
	if err != nil {
		t.Fatalf("%s: reopen: %v", what, err)
	}
	w.Close()
	if hs.Term != 5 || len(ents) != 3 || string(ents[2].Data) != "new" {
		t.Fatalf("%s: after appending past the cut: %+v and %+v", what, hs, ents)
	}
}

// A crash can stop a write after any byte. Every such prefix must recover
// to the state before the write.
func TestTornTailAtEveryOffset(t *testing.T) {
	data, two := buildThree(t)
	for cut := two; cut < int64(len(data)); cut++ {
		checkTwoRecords(t, fmt.Sprintf("cut at %d", cut), data[:cut], two)
	}
}

// Any single flipped bit in the last record must be detected.
func TestBitFlipInLastRecord(t *testing.T) {
	data, two := buildThree(t)
	for i := two; i < int64(len(data)); i++ {
		for _, bit := range []byte{0x01, 0x80} {
			damaged := append([]byte(nil), data...)
			damaged[i] ^= bit
			checkTwoRecords(t, fmt.Sprintf("byte %d bit %#x", i, bit), damaged, two)
		}
	}
}

// Some filesystems hand back zero-filled blocks after a crash.
func TestZeroFilledTail(t *testing.T) {
	data, two := buildThree(t)
	checkTwoRecords(t, "zero tail", append(append([]byte(nil), data[:two]...), make([]byte, 4096)...), two)
}

// A record that passes its checksum but leaves a hole in the log is not a
// torn write. Recovery must refuse rather than return a log with a gap.
func TestGapIsCorruption(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	w, _, _ := mustOpen(t, path)
	mustSave(t, w, raft.HardState{Term: 1}, ent(1, 1, "a"))
	mustSave(t, w, raft.HardState{Term: 1}, ent(5, 1, "e"))
	w.Close()
	if _, _, _, err := OpenFile(path); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

type failingFile struct {
	File
	failSync bool
}

func (f *failingFile) Sync() error {
	if f.failSync {
		return errors.New("disk on fire")
	}
	return f.File.Sync()
}

// After a failed fsync the WAL must refuse further writes: the caller may
// not pretend the earlier record is durable.
func TestSaveErrorIsSticky(t *testing.T) {
	f, err := os.OpenFile(filepath.Join(t.TempDir(), "wal"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	ff := &failingFile{File: f}
	w, _, _, err := Open(ff)
	if err != nil {
		t.Fatal(err)
	}
	mustSave(t, w, raft.HardState{Term: 1}, ent(1, 1, "a"))
	ff.failSync = true
	if err := w.Save(raft.HardState{Term: 1}, []raft.Entry{ent(2, 1, "b")}); err == nil {
		t.Fatal("Save reported success although fsync failed")
	}
	ff.failSync = false
	if err := w.Save(raft.HardState{Term: 1}, []raft.Entry{ent(3, 1, "c")}); err == nil {
		t.Fatal("Save accepted a write after an earlier one failed")
	}
}

// BenchmarkSave measures the cost of one durable write, which is dominated
// by fsync on real disks.
func BenchmarkSave(b *testing.B) {
	w, _, _, err := OpenFile(filepath.Join(b.TempDir(), "wal"))
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()
	data := bytes.Repeat([]byte("v"), 128)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		e := raft.Entry{Index: uint64(i + 1), Term: 1, Data: data}
		if err := w.Save(raft.HardState{Term: 1}, []raft.Entry{e}); err != nil {
			b.Fatal(err)
		}
	}
}
