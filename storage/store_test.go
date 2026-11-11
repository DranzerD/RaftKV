package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DranzerD/RaftKV/raft"
)

func TestStoreSnapshotRoundTrip(t *testing.T) {
	dir := t.TempDir()
	open := func() (*Store, State) {
		fs, err := NewDirFS(dir)
		if err != nil {
			t.Fatal(err)
		}
		s, st, err := OpenStore(fs)
		if err != nil {
			t.Fatal(err)
		}
		return s, st
	}
	files := func() string {
		fs, _ := NewDirFS(dir)
		names, _ := fs.List()
		return fmt.Sprint(names)
	}

	s, st := open()
	if st.Snapshot != nil || len(st.Entries) != 0 {
		t.Fatalf("fresh store: %+v", st)
	}
	var ents []raft.Entry
	for i := uint64(1); i <= 50; i++ {
		ents = append(ents, ent(i, 1, "some payload to make the log worth compacting"))
	}
	if err := s.Save(raft.HardState{Term: 1, Vote: 1}, ents, nil); err != nil {
		t.Fatal(err)
	}
	before := s.WALSize()

	// Snapshot through 40, with one more entry in the same save.
	snap := &raft.Snapshot{Index: 40, Term: 1, Data: []byte("state at 40")}
	if err := s.Save(raft.HardState{Term: 1, Vote: 1}, []raft.Entry{ent(51, 1, "x")}, snap); err != nil {
		t.Fatal(err)
	}
	if s.WALSize() >= before/2 {
		t.Fatalf("wal is %d bytes after compaction, was %d", s.WALSize(), before)
	}
	if got := files(); got != "[snap-40 wal]" {
		t.Fatalf("directory holds %s", got)
	}
	// Appending after a compaction goes to the new file.
	if err := s.Save(raft.HardState{Term: 2, Vote: 2}, []raft.Entry{ent(52, 2, "y")}, nil); err != nil {
		t.Fatal(err)
	}
	s.Close()

	s, st = open()
	if st.HardState != (raft.HardState{Term: 2, Vote: 2}) || st.Snapshot == nil ||
		st.Snapshot.Index != 40 || string(st.Snapshot.Data) != "state at 40" {
		t.Fatalf("recovered %+v", st)
	}
	if len(st.Entries) != 12 || st.Entries[0].Index != 41 || st.Entries[11].Index != 52 {
		t.Fatalf("recovered %d entries starting at %d", len(st.Entries), st.Entries[0].Index)
	}

	// A second snapshot replaces the first; a log that disagrees with it
	// is dropped entirely.
	snap2 := &raft.Snapshot{Index: 60, Term: 3, Data: []byte("state at 60")}
	if err := s.Save(raft.HardState{Term: 3}, nil, snap2); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if got := files(); got != "[snap-60 wal]" {
		t.Fatalf("directory holds %s", got)
	}
	s, st = open()
	defer s.Close()
	if st.Snapshot.Index != 60 || len(st.Entries) != 0 {
		t.Fatalf("recovered snapshot %d and %d entries", st.Snapshot.Index, len(st.Entries))
	}
}

// A snapshot file the WAL does not name is an unfinished step, not state.
func TestStoreIgnoresUncommittedSnapshot(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewDirFS(dir)
	s, _, err := OpenStore(fs)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Save(raft.HardState{Term: 1}, []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b")}, nil); err != nil {
		t.Fatal(err)
	}
	s.Close()
	// Crash after step 1 of a snapshot: the file exists, the WAL record
	// does not. Also leave half of a WAL rewrite behind.
	if err := writeSnapshot(fs, &raft.Snapshot{Index: 2, Term: 1, Data: []byte("orphan")}); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "wal.tmp"), []byte("half a rewrite"), 0o644)

	s, st, err := OpenStore(fs)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if st.Snapshot != nil || len(st.Entries) != 2 {
		t.Fatalf("recovered %+v", st)
	}
	if names, _ := fs.List(); fmt.Sprint(names) != "[wal]" {
		t.Fatalf("leftovers not cleaned up: %v", names)
	}
}

// A snapshot file damaged after it was committed must stop recovery cold:
// the entries it replaced are gone, so there is nothing to fall back on.
func TestStoreRefusesDamagedSnapshot(t *testing.T) {
	dir := t.TempDir()
	fs, _ := NewDirFS(dir)
	s, _, err := OpenStore(fs)
	if err != nil {
		t.Fatal(err)
	}
	snap := &raft.Snapshot{Index: 2, Term: 1, Data: []byte("state")}
	if err := s.Save(raft.HardState{Term: 1}, []raft.Entry{ent(1, 1, "a"), ent(2, 1, "b")}, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Save(raft.HardState{Term: 1}, nil, snap); err != nil {
		t.Fatal(err)
	}
	s.Close()
	path := filepath.Join(dir, "snap-2")
	b, _ := os.ReadFile(path)
	b[len(b)-1] ^= 1
	os.WriteFile(path, b, 0o644)
	if _, _, err := OpenStore(fs); err == nil {
		t.Fatal("opened a store whose snapshot fails its checksum")
	}
}
