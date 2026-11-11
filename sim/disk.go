package sim

import (
	"errors"
	"io"
	"math/rand"
	"sort"

	"github.com/DranzerD/RaftKV/storage"
)

var errInjected = errors.New("sim: injected disk fault")

// memFS is a fake directory that behaves like a disk across a power cut.
// It implements storage.FS.
//
// Fault injection: set failIn to N and the Nth mutating operation from now
// fails (a write lands only in part), after which every operation fails
// until the next crash. That stops a save at any of its steps.
type memFS struct {
	files map[string]*memFile

	failIn   int
	dead     bool
	tearSeed int64 // decides how much of a failed write lands
	lies     bool  // sync reports success without making anything durable
}

func newMemFS(lies bool) *memFS {
	return &memFS{files: make(map[string]*memFile), lies: lies}
}

// step is called before every mutating operation.
func (fs *memFS) step() error {
	if fs.dead {
		return errInjected
	}
	if fs.failIn > 0 {
		fs.failIn--
		if fs.failIn == 0 {
			fs.dead = true
			return errInjected
		}
	}
	return nil
}

func (fs *memFS) Open(name string) (storage.File, error) {
	if f, ok := fs.files[name]; ok {
		return f, nil
	}
	if err := fs.step(); err != nil {
		return nil, err
	}
	f := &memFile{fs: fs}
	fs.files[name] = f
	return f, nil
}

func (fs *memFS) Rename(oldName, newName string) error {
	f, ok := fs.files[oldName]
	if !ok {
		return errors.New("sim: rename of a missing file")
	}
	if err := fs.step(); err != nil {
		return err
	}
	delete(fs.files, oldName)
	fs.files[newName] = f
	return nil
}

func (fs *memFS) Remove(name string) error {
	if err := fs.step(); err != nil {
		return err
	}
	delete(fs.files, name)
	return nil
}

func (fs *memFS) List() ([]string, error) {
	names := make([]string, 0, len(fs.files))
	for name := range fs.files {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

// crash applies a power cut to every file and clears injected faults.
func (fs *memFS) crash(r *rand.Rand) {
	names, _ := fs.List()
	for _, name := range names {
		fs.files[name].crash(r)
	}
	fs.failIn, fs.dead = 0, false
}

// memFile is one file in a memFS: bytes written but not yet synced may be
// lost, kept in part, or damaged by a crash.
type memFile struct {
	fs     *memFS
	data   []byte
	synced int // data[:synced] survives a crash for certain
}

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
	if off < 0 || off > int64(len(f.data)) {
		return 0, errors.New("sim: write past end of file")
	}
	if int(off) < f.synced {
		f.synced = int(off)
	}
	if err := f.fs.step(); err != nil {
		keep := 0
		if len(p) > 0 {
			keep = int(f.fs.tearSeed % int64(len(p))) // always short of the full write
		}
		f.data = append(f.data[:off], p[:keep]...)
		return keep, err
	}
	f.data = append(f.data[:off], p...)
	return len(p), nil
}

func (f *memFile) Truncate(size int64) error {
	if size < 0 || size > int64(len(f.data)) {
		return errors.New("sim: bad truncate size")
	}
	if err := f.fs.step(); err != nil {
		return err
	}
	f.data = f.data[:size]
	if f.synced > int(size) {
		f.synced = int(size)
	}
	return nil
}

func (f *memFile) Sync() error {
	if err := f.fs.step(); err != nil {
		return err
	}
	if !f.fs.lies {
		f.synced = len(f.data)
	}
	return nil
}

func (f *memFile) Close() error { return nil }

// crash keeps a random prefix of the unsynced bytes and sometimes flips
// one bit of it.
func (f *memFile) crash(r *rand.Rand) {
	if unsynced := len(f.data) - f.synced; unsynced > 0 {
		keep := r.Intn(unsynced + 1)
		f.data = f.data[:f.synced+keep]
		if keep > 0 && r.Intn(4) == 0 {
			f.data[f.synced+r.Intn(keep)] ^= 1 << uint(r.Intn(8))
		}
	}
	f.synced = len(f.data)
}
