package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"

	"github.com/DranzerD/RaftKV/raft"
)

// FS is the handful of directory operations Store needs. The simulator
// supplies a fake that can fail at any step.
type FS interface {
	// Open opens a file for reading and writing, creating it if missing.
	Open(name string) (File, error)
	// Rename atomically replaces newName with oldName. The contents of
	// oldName must already be synced.
	Rename(oldName, newName string) error
	Remove(name string) error
	List() ([]string, error)
}

// State is everything a node needs to restart.
type State struct {
	HardState raft.HardState
	Snapshot  *raft.Snapshot // nil if no snapshot has been taken
	Entries   []raft.Entry   // the log after the snapshot
}

const (
	walName    = "wal"
	tmpSuffix  = ".tmp"
	snapPrefix = "snap-"
)

func snapName(index uint64) string { return snapPrefix + strconv.FormatUint(index, 10) }

// Store keeps a node's durable state in one directory: the WAL, plus the
// file of the snapshot the WAL starts after.
//
// Taking a snapshot goes in three steps, and a crash between any two leaves
// a directory that recovers to either the old state or the new one:
//
//  1. Write the snapshot to a temporary file, sync it, rename it into place.
//  2. Append a WAL record that names the snapshot. This is the commit point.
//  3. Replace the WAL with a short one holding only what follows the
//     snapshot, then delete older snapshot files.
type Store struct {
	fs  FS
	wal *WAL
	// The durable state, mirrored so the WAL can be rewritten.
	hs  raft.HardState
	log []raft.Entry
	err error
}

// OpenStore recovers the state kept in fs.
func OpenStore(fs FS) (*Store, State, error) {
	fail := func(err error) (*Store, State, error) { return nil, State{}, err }

	names, err := fs.List()
	if err != nil {
		return fail(err)
	}
	// Temporary files are leftovers of a step that never finished.
	for _, name := range names {
		if strings.HasSuffix(name, tmpSuffix) {
			if err := fs.Remove(name); err != nil {
				return fail(err)
			}
		}
	}

	f, err := fs.Open(walName)
	if err != nil {
		return fail(err)
	}
	wal, hs, entries, err := Open(f)
	if err != nil {
		f.Close()
		return fail(err)
	}
	st := State{HardState: hs, Entries: entries}
	if index, term := wal.Snapshot(); index > 0 {
		snap, err := readSnapshot(fs, snapName(index))
		if err != nil {
			wal.Close()
			return fail(fmt.Errorf("%w: snapshot %d named by the wal: %v", ErrCorrupt, index, err))
		}
		if snap.Index != index || snap.Term != term {
			wal.Close()
			return fail(fmt.Errorf("%w: snapshot file holds (%d, %d), wal expects (%d, %d)",
				ErrCorrupt, snap.Index, snap.Term, index, term))
		}
		st.Snapshot = snap
	}
	s := &Store{fs: fs, wal: wal, hs: hs, log: entries}
	// Snapshot files the WAL does not name were never committed, or have
	// been superseded.
	if err := s.removeOtherSnapshots(); err != nil {
		wal.Close()
		return fail(err)
	}
	return s, st, nil
}

// Save durably records one Ready: the snapshot if there is one, then the
// hard state and entries. After an error the Store is unusable; the process
// must stop and recover with OpenStore.
func (s *Store) Save(hs raft.HardState, entries []raft.Entry, snap *raft.Snapshot) error {
	if s.err != nil {
		return s.err
	}
	if err := s.save(hs, entries, snap); err != nil {
		s.err = err
		return err
	}
	return nil
}

func (s *Store) save(hs raft.HardState, entries []raft.Entry, snap *raft.Snapshot) error {
	base, _ := s.wal.Snapshot()
	newSnap := snap != nil && snap.Index > base
	if newSnap {
		if err := writeSnapshot(s.fs, snap); err != nil {
			return err
		}
		if p := snap.Index - base; p <= uint64(len(s.log)) && s.log[p-1].Term == snap.Term {
			s.log = append([]raft.Entry(nil), s.log[p:]...)
		} else {
			s.log = nil
		}
		base = snap.Index
		s.wal.SetSnapshot(snap.Index, snap.Term)
	}
	if err := s.wal.Save(hs, entries); err != nil {
		return err
	}
	s.hs = hs
	if len(entries) > 0 {
		s.log = append(s.log[:entries[0].Index-base-1], entries...)
	}
	if newSnap {
		return s.rewriteWAL()
	}
	return nil
}

// rewriteWAL replaces the WAL with one record holding the current state.
func (s *Store) rewriteWAL() error {
	tmpName := walName + tmpSuffix
	f, err := s.fs.Open(tmpName)
	if err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return err
	}
	fresh := &WAL{f: f}
	fresh.SetSnapshot(s.wal.Snapshot())
	if err := fresh.Save(s.hs, s.log); err != nil {
		f.Close()
		return err
	}
	// Both handles must be closed before the rename: Windows refuses to
	// replace an open file.
	if err := f.Close(); err != nil {
		return err
	}
	if err := s.wal.Close(); err != nil {
		return err
	}
	if err := s.fs.Rename(tmpName, walName); err != nil {
		return err
	}
	f, err = s.fs.Open(walName)
	if err != nil {
		return err
	}
	fresh.f = f
	s.wal = fresh
	return s.removeOtherSnapshots()
}

func (s *Store) removeOtherSnapshots() error {
	names, err := s.fs.List()
	if err != nil {
		return err
	}
	index, _ := s.wal.Snapshot()
	for _, name := range names {
		if strings.HasPrefix(name, snapPrefix) && (index == 0 || name != snapName(index)) {
			if err := s.fs.Remove(name); err != nil {
				return err
			}
		}
	}
	return nil
}

// WALSize returns the size of the WAL file in bytes.
func (s *Store) WALSize() int64 { return s.wal.Size() }

// Close closes the WAL.
func (s *Store) Close() error { return s.wal.Close() }

// Snapshot file: CRC-32C of the rest (uint32) | index (uint64) | term (uint64) | data.
const snapHeader = 20

func writeSnapshot(fs FS, snap *raft.Snapshot) error {
	b := make([]byte, snapHeader, snapHeader+len(snap.Data))
	binary.LittleEndian.PutUint64(b[4:12], snap.Index)
	binary.LittleEndian.PutUint64(b[12:20], snap.Term)
	b = append(b, snap.Data...)
	binary.LittleEndian.PutUint32(b[0:4], crc32.Checksum(b[4:], crcTable))

	tmpName := snapName(snap.Index) + tmpSuffix
	f, err := fs.Open(tmpName)
	if err != nil {
		return err
	}
	if err := f.Truncate(0); err != nil {
		f.Close()
		return err
	}
	if _, err := f.WriteAt(b, 0); err != nil {
		f.Close()
		return fmt.Errorf("storage: write snapshot: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("storage: sync snapshot: %w", err)
	}
	if err := f.Close(); err != nil {
		return err
	}
	return fs.Rename(tmpName, snapName(snap.Index))
}

func readSnapshot(fs FS, name string) (*raft.Snapshot, error) {
	names, err := fs.List()
	if err != nil {
		return nil, err
	}
	found := false
	for _, n := range names {
		found = found || n == name
	}
	if !found {
		return nil, errors.New("file is missing")
	}
	f, err := fs.Open(name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.NewSectionReader(f, 0, math.MaxInt64))
	if err != nil {
		return nil, err
	}
	if len(b) < snapHeader || crc32.Checksum(b[4:], crcTable) != binary.LittleEndian.Uint32(b[0:4]) {
		return nil, errors.New("checksum mismatch")
	}
	return &raft.Snapshot{
		Index: binary.LittleEndian.Uint64(b[4:12]),
		Term:  binary.LittleEndian.Uint64(b[12:20]),
		Data:  b[snapHeader:],
	}, nil
}

// DirFS is an FS backed by a real directory.
type DirFS struct {
	dir string
	// NoSync turns every fsync into a no-op. Acknowledged writes can then
	// be lost in a power cut. For benchmarks only.
	NoSync bool
}

// unsyncedFile is an *os.File whose Sync does nothing.
type unsyncedFile struct{ *os.File }

func (unsyncedFile) Sync() error { return nil }

// NewDirFS creates the directory if needed.
func NewDirFS(dir string) (*DirFS, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &DirFS{dir: dir}, nil
}

func (d *DirFS) Open(name string) (File, error) {
	_, statErr := os.Stat(filepath.Join(d.dir, name))
	f, err := os.OpenFile(filepath.Join(d.dir, name), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if d.NoSync {
		return unsyncedFile{f}, nil
	}
	if statErr != nil {
		// A new directory entry: make it durable before anyone relies on
		// the file existing.
		if err := d.syncDir(); err != nil {
			f.Close()
			return nil, err
		}
	}
	return f, nil
}

func (d *DirFS) Rename(oldName, newName string) error {
	if err := os.Rename(filepath.Join(d.dir, oldName), filepath.Join(d.dir, newName)); err != nil {
		return err
	}
	return d.syncDir()
}

func (d *DirFS) Remove(name string) error {
	return os.Remove(filepath.Join(d.dir, name))
}

func (d *DirFS) List() ([]string, error) {
	ents, err := os.ReadDir(d.dir)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(ents))
	for _, e := range ents {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// syncDir makes a create or rename durable. Windows has no way to sync a
// directory handle; NTFS journals the metadata change itself.
func (d *DirFS) syncDir() error {
	if d.NoSync || runtime.GOOS == "windows" {
		return nil
	}
	f, err := os.Open(d.dir)
	if err != nil {
		return fmt.Errorf("storage: open dir for sync: %w", err)
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("storage: sync dir: %w", err)
	}
	return nil
}
