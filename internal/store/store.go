// Package store implements a durable append-only log with CRC framing,
// torn-tail recovery, and atomic snapshot/compaction support.
package store

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
)

// ErrCorrupt is returned when a fully-written, committed record fails
// integrity checks. Real corruption is never silently ignored.
var ErrCorrupt = errors.New("store: corrupt committed record")

// Record is one committed mutation. Payload is a canonical JSON encoding
// of the mutation, supplied and interpreted by the caller.
type Record struct {
	Seq     uint64          `json:"seq"`
	Payload json.RawMessage `json:"payload"`
}

// line is the on-disk envelope: one JSON object per line.
type line struct {
	Seq     uint64          `json:"seq"`
	Payload json.RawMessage `json:"payload"`
	CRC     uint32          `json:"crc"`
}

func crcOf(seq uint64, payload []byte) uint32 {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], seq)
	c := crc32.NewIEEE()
	c.Write(b[:])
	c.Write(payload)
	return c.Sum32()
}

// Snapshot is a point-in-time image of the caller's state.
type Snapshot struct {
	Seq  uint64          `json:"seq"` // last applied record seq
	Data json.RawMessage `json:"data"`
}

type snapshotFile struct {
	Seq  uint64          `json:"seq"`
	Data json.RawMessage `json:"data"`
	CRC  uint32          `json:"crc"`
}

// Store owns the log and snapshot files inside a data directory.
type Store struct {
	mu       sync.Mutex
	dir      string
	logPath  string
	snapPath string
	log      *os.File
	seq      uint64 // last committed seq
	size     int64  // current log file size
}

const (
	logFileName  = "log.jsonl"
	snapFileName = "snapshot.json"
)

// Open opens (or creates) the store in dir and recovers it.
//
// Recovery rules:
//   - a torn/incomplete record at the log tail is truncated (it was never
//     acknowledged as committed);
//   - a CRC mismatch or malformed record that is fully newline-terminated
//     before end of file is real corruption and returns ErrCorrupt;
//   - a corrupt snapshot returns ErrCorrupt.
func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{
		dir:      dir,
		logPath:  filepath.Join(dir, logFileName),
		snapPath: filepath.Join(dir, snapFileName),
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.log = f
	return s, nil
}

// recover validates the snapshot and log, truncating a torn tail.
func (s *Store) recover() error {
	snap, err := s.LoadSnapshot()
	if err != nil {
		return err
	}
	if snap != nil {
		s.seq = snap.Seq
	}
	data, err := os.ReadFile(s.logPath)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	good := int64(0) // length of the valid prefix
	var lastSeq uint64
	for off := 0; off < len(data); {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			// Incomplete tail record: truncate, keep prior committed state.
			break
		}
		raw := data[off : off+nl]
		var l line
		if err := json.Unmarshal(raw, &l); err != nil {
			// Newline-terminated but unparseable mid-log is corruption;
			// as the final line it is a torn tail.
			if off+nl+1 == len(data) {
				break
			}
			return fmt.Errorf("%w: log offset %d: %v", ErrCorrupt, off, err)
		}
		if crcOf(l.Seq, l.Payload) != l.CRC {
			if off+nl+1 == len(data) {
				break // torn tail
			}
			return fmt.Errorf("%w: log offset %d: crc mismatch", ErrCorrupt, off)
		}
		if l.Seq <= lastSeq {
			return fmt.Errorf("%w: log offset %d: non-monotonic seq", ErrCorrupt, off)
		}
		lastSeq = l.Seq
		good = int64(off + nl + 1)
		off += nl + 1
	}
	if good < int64(len(data)) {
		if err := os.Truncate(s.logPath, good); err != nil {
			return err
		}
	}
	if lastSeq > s.seq {
		s.seq = lastSeq
	}
	s.size = good
	return nil
}

// Replay returns the snapshot (if any) and all committed records with
// Seq > snapshot.Seq, in order.
func (s *Store) Replay() (*Snapshot, []Record, error) {
	snap, err := s.LoadSnapshot()
	if err != nil {
		return nil, nil, err
	}
	var base uint64
	if snap != nil {
		base = snap.Seq
	}
	data, err := os.ReadFile(s.logPath)
	if os.IsNotExist(err) {
		return snap, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	var recs []Record
	for off := 0; off < len(data); {
		nl := bytes.IndexByte(data[off:], '\n')
		if nl < 0 {
			break
		}
		var l line
		if err := json.Unmarshal(data[off:off+nl], &l); err != nil {
			return nil, nil, fmt.Errorf("%w: replay: %v", ErrCorrupt, err)
		}
		if crcOf(l.Seq, l.Payload) != l.CRC {
			return nil, nil, fmt.Errorf("%w: replay: crc mismatch at seq %d", ErrCorrupt, l.Seq)
		}
		if l.Seq > base {
			recs = append(recs, Record{Seq: l.Seq, Payload: l.Payload})
		}
		off += nl + 1
	}
	return snap, recs, nil
}

// Append durably commits one record and returns its sequence number.
// The record is fsynced before Append returns.
func (s *Store) Append(payload json.RawMessage) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.seq + 1
	l := line{Seq: seq, Payload: payload, CRC: crcOf(seq, payload)}
	buf, err := json.Marshal(l)
	if err != nil {
		return 0, err
	}
	buf = append(buf, '\n')
	if _, err := s.log.Write(buf); err != nil {
		return 0, err
	}
	if err := s.log.Sync(); err != nil {
		return 0, err
	}
	s.seq = seq
	s.size += int64(len(buf))
	return seq, nil
}

// Seq returns the last committed sequence number.
func (s *Store) Seq() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seq
}

// Size returns the current log size in bytes.
func (s *Store) Size() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.size
}

// LoadSnapshot reads and validates the snapshot, or returns nil if none.
func (s *Store) LoadSnapshot() (*Snapshot, error) {
	data, err := os.ReadFile(s.snapPath)
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var sf snapshotFile
	if err := json.Unmarshal(data, &sf); err != nil {
		return nil, fmt.Errorf("%w: snapshot: %v", ErrCorrupt, err)
	}
	if crcOf(sf.Seq, sf.Data) != sf.CRC {
		return nil, fmt.Errorf("%w: snapshot: crc mismatch", ErrCorrupt)
	}
	return &Snapshot{Seq: sf.Seq, Data: sf.Data}, nil
}

// Compact atomically replaces the snapshot with data (representing all
// records up to and including seq) and resets the log to empty.
//
// Crash safety: the snapshot is written to a temp file, fsynced, renamed,
// then the log is replaced the same way. A crash at any point leaves either
// the old (snapshot, log) pair or the new one, both recoverable; a new
// snapshot with the old log is also safe because replay skips seq <= snap.Seq.
func (s *Store) Compact(seq uint64, data json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if seq > s.seq {
		return fmt.Errorf("compact seq %d beyond committed seq %d", seq, s.seq)
	}
	sf := snapshotFile{Seq: seq, Data: data, CRC: crcOf(seq, data)}
	buf, err := json.Marshal(sf)
	if err != nil {
		return err
	}
	tmp := s.snapPath + ".tmp"
	if err := writeFileAtomic(s.dir, tmp, s.snapPath, buf); err != nil {
		return err
	}
	s.log.Close()
	ltmp := s.logPath + ".tmp"
	if err := writeFileAtomic(s.dir, ltmp, s.logPath, nil); err != nil {
		return err
	}
	f, err := os.OpenFile(s.logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	s.log = f
	s.size = 0
	return nil
}

// writeFileAtomic writes buf to tmp, fsyncs, renames to final, fsyncs dir.
func writeFileAtomic(dir, tmp, final string, buf []byte) error {
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(buf); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// Close closes the store.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.log != nil {
		return s.log.Close()
	}
	return nil
}
