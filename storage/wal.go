// Package storage persists Raft's durable state: the current term, the vote
// and the log.
//
// The write-ahead log is one append-only file of records. Each record holds
// one Ready's worth of state, so a Ready is saved all-or-nothing:
//
//	header  body length (uint32) | CRC-32C of body (uint32)
//	body    term (uint64) | vote (uint64) | snapshot index (uint64) |
//	        snapshot term (uint64) | entry count (uint32) | entries
//	entry   index (uint64) | term (uint64) | type (uint8) | data length (uint32) | data
//
// All integers are little-endian. Entries in a record replace the log from
// their first index onward, which is how follower-side truncation is
// recorded without rewriting the file.
//
// Every record names the snapshot the log currently starts after. When a
// record names a newer one, replay drops the entries that snapshot covers,
// exactly as the Raft core did at that moment. The snapshot's contents live
// in a separate file; see Store.
//
// Recovery replays records until the first one that is incomplete or fails
// its checksum, and cuts the file there. That is the right answer for a
// write torn by a crash, since such a record was never acknowledged. It
// cannot tell a torn tail from corruption in the middle of the file; a bad
// sector under old records would silently drop everything after it.
//
// A WAL file only ever grows. Store replaces it with a fresh, short one
// each time a snapshot is taken.
package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"

	"github.com/DranzerD/RaftKV/raft"
)

// File is the slice of *os.File the WAL needs. The simulator supplies a
// fake that can tear writes and lose unsynced data.
type File interface {
	io.ReaderAt
	io.WriterAt
	Truncate(size int64) error
	Sync() error
	Close() error
}

// ErrCorrupt means the file passed its checksums but does not describe a
// valid log. Recovery refuses to guess.
var ErrCorrupt = errors.New("storage: wal is corrupt")

const (
	headerSize = 8
	minBody    = 36      // term + vote + snapshot index + snapshot term + count
	maxBody    = 1 << 30 // anything larger is treated as garbage
	entryFixed = 21      // index + term + type + data length
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// WAL is an open write-ahead log. It is not safe for concurrent use.
type WAL struct {
	f   File
	off int64 // end of the last complete record
	// The snapshot the log starts after; written into every record.
	snapIndex, snapTerm uint64
	buf                 []byte
	err                 error // sticky: after a failed Save the file's tail is unknown
}

// Open recovers the state stored in f and returns a WAL positioned to
// append after it. A torn tail is truncated away. An empty file yields a
// zero HardState and no entries.
func Open(f File) (*WAL, raft.HardState, []raft.Entry, error) {
	data, err := io.ReadAll(io.NewSectionReader(f, 0, math.MaxInt64))
	if err != nil {
		return nil, raft.HardState{}, nil, fmt.Errorf("storage: read wal: %w", err)
	}

	var hs raft.HardState
	var log []raft.Entry      // entries after base
	var base, baseTerm uint64 // the snapshot the log starts after
	off := 0
	for {
		body, size, ok := nextRecord(data[off:])
		if !ok {
			break
		}
		recHS, snapIndex, snapTerm, ents, err := decodeBody(body)
		if err != nil {
			return nil, raft.HardState{}, nil, fmt.Errorf("%w: record at offset %d: %v", ErrCorrupt, off, err)
		}
		switch {
		case snapIndex < base:
			return nil, raft.HardState{}, nil, fmt.Errorf(
				"%w: record at offset %d goes back from snapshot %d to %d", ErrCorrupt, off, base, snapIndex)
		case snapIndex > base:
			// Same rule as the core: keep what follows the snapshot only
			// if our log agrees with the snapshot's last entry.
			if p := snapIndex - base; p <= uint64(len(log)) && log[p-1].Term == snapTerm {
				log = append([]raft.Entry(nil), log[p:]...)
			} else {
				log = nil
			}
			base, baseTerm = snapIndex, snapTerm
		}
		if len(ents) > 0 {
			first := ents[0].Index
			if first <= base || first > base+uint64(len(log))+1 {
				return nil, raft.HardState{}, nil, fmt.Errorf(
					"%w: record at offset %d starts at index %d but the log covers %d..%d",
					ErrCorrupt, off, first, base+1, base+uint64(len(log)))
			}
			log = append(log[:first-base-1], ents...)
		}
		hs = recHS
		off += size
	}

	if off < len(data) {
		if err := f.Truncate(int64(off)); err != nil {
			return nil, raft.HardState{}, nil, fmt.Errorf("storage: truncate torn tail: %w", err)
		}
		if err := f.Sync(); err != nil {
			return nil, raft.HardState{}, nil, fmt.Errorf("storage: sync after truncate: %w", err)
		}
	}
	return &WAL{f: f, off: int64(off), snapIndex: base, snapTerm: baseTerm}, hs, log, nil
}

// Snapshot returns the index and term of the snapshot the log starts
// after, or zeros if the log starts at index 1.
func (w *WAL) Snapshot() (index, term uint64) { return w.snapIndex, w.snapTerm }

// SetSnapshot makes the next Save record that the log now starts after the
// given snapshot. The snapshot itself must already be durable.
func (w *WAL) SetSnapshot(index, term uint64) { w.snapIndex, w.snapTerm = index, term }

// OpenFile opens or creates the WAL at path.
func OpenFile(path string) (*WAL, raft.HardState, []raft.Entry, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, raft.HardState{}, nil, err
	}
	w, hs, ents, err := Open(f)
	if err != nil {
		f.Close()
		return nil, raft.HardState{}, nil, err
	}
	return w, hs, ents, nil
}

// Save durably records hs and entries. It returns only after fsync, so the
// caller may send messages that depend on this state as soon as it returns
// nil. After an error the WAL is unusable: the process must stop and
// recover with Open.
func (w *WAL) Save(hs raft.HardState, entries []raft.Entry) error {
	if w.err != nil {
		return w.err
	}
	b := append(w.buf[:0], make([]byte, headerSize)...)
	b = binary.LittleEndian.AppendUint64(b, hs.Term)
	b = binary.LittleEndian.AppendUint64(b, hs.Vote)
	b = binary.LittleEndian.AppendUint64(b, w.snapIndex)
	b = binary.LittleEndian.AppendUint64(b, w.snapTerm)
	b = binary.LittleEndian.AppendUint32(b, uint32(len(entries)))
	for _, e := range entries {
		b = binary.LittleEndian.AppendUint64(b, e.Index)
		b = binary.LittleEndian.AppendUint64(b, e.Term)
		b = append(b, byte(e.Type))
		b = binary.LittleEndian.AppendUint32(b, uint32(len(e.Data)))
		b = append(b, e.Data...)
	}
	w.buf = b
	body := b[headerSize:]
	if len(body) > maxBody {
		return fmt.Errorf("storage: record of %d bytes exceeds the %d byte limit", len(body), maxBody)
	}
	binary.LittleEndian.PutUint32(b[0:4], uint32(len(body)))
	binary.LittleEndian.PutUint32(b[4:8], crc32.Checksum(body, crcTable))

	if _, err := w.f.WriteAt(b, w.off); err != nil {
		w.err = fmt.Errorf("storage: write wal: %w", err)
		return w.err
	}
	if err := w.f.Sync(); err != nil {
		// The data may or may not be on disk. Retrying fsync is not
		// safe: the kernel can drop the dirty pages and report success.
		w.err = fmt.Errorf("storage: sync wal: %w", err)
		return w.err
	}
	w.off += int64(len(b))
	return nil
}

// Size returns the number of bytes of durable records.
func (w *WAL) Size() int64 { return w.off }

// Close closes the underlying file.
func (w *WAL) Close() error { return w.f.Close() }

// nextRecord returns the body of the record at the start of data and the
// record's total size. ok is false if the record is incomplete or damaged.
func nextRecord(data []byte) (body []byte, size int, ok bool) {
	if len(data) < headerSize {
		return nil, 0, false
	}
	n := binary.LittleEndian.Uint32(data[0:4])
	// An all-zero header would otherwise pass: the CRC of nothing is 0.
	if n < minBody || n > maxBody || uint64(len(data)-headerSize) < uint64(n) {
		return nil, 0, false
	}
	body = data[headerSize : headerSize+int(n)]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(data[4:8]) {
		return nil, 0, false
	}
	return body, headerSize + int(n), true
}

func decodeBody(b []byte) (hs raft.HardState, snapIndex, snapTerm uint64, ents []raft.Entry, err error) {
	hs = raft.HardState{
		Term: binary.LittleEndian.Uint64(b[0:8]),
		Vote: binary.LittleEndian.Uint64(b[8:16]),
	}
	snapIndex = binary.LittleEndian.Uint64(b[16:24])
	snapTerm = binary.LittleEndian.Uint64(b[24:32])
	ents, err = decodeEntries(binary.LittleEndian.Uint32(b[32:36]), b[minBody:])
	return hs, snapIndex, snapTerm, ents, err
}

func decodeEntries(count uint32, b []byte) ([]raft.Entry, error) {
	if uint64(count)*entryFixed > uint64(len(b)) {
		return nil, fmt.Errorf("claims %d entries in %d bytes", count, len(b))
	}
	ents := make([]raft.Entry, 0, count)
	for i := uint32(0); i < count; i++ {
		if len(b) < entryFixed {
			return nil, fmt.Errorf("entry %d is cut short", i)
		}
		e := raft.Entry{
			Index: binary.LittleEndian.Uint64(b[0:8]),
			Term:  binary.LittleEndian.Uint64(b[8:16]),
			Type:  raft.EntryType(b[16]),
		}
		n := binary.LittleEndian.Uint32(b[17:21])
		b = b[entryFixed:]
		if uint64(n) > uint64(len(b)) {
			return nil, fmt.Errorf("entry %d data is cut short", i)
		}
		if n > 0 {
			e.Data = append([]byte(nil), b[:n]...)
		}
		b = b[n:]
		if i > 0 && e.Index != ents[i-1].Index+1 {
			return nil, fmt.Errorf("entry indexes jump from %d to %d", ents[i-1].Index, e.Index)
		}
		ents = append(ents, e)
	}
	if len(b) != 0 {
		return nil, fmt.Errorf("%d trailing bytes", len(b))
	}
	return ents, nil
}
