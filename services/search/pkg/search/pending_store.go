package search

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/gofrs/flock"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
)

const (
	// _pendingStoreVersion is the schema version of the persisted pending file.
	_pendingStoreVersion = 1
	// _pendingStoreFile is the JSON file holding spaces that still need a
	// successful (re)index run. It lives next to the bleve index directory.
	_pendingStoreFile = "pending_spaces.json"
	// _pendingStoreLock serializes read-modify-write access to the store
	// across all search service instances sharing the data path.
	_pendingStoreLock = "pending_spaces.lock"
	// _pendingLocksDir contains one cross-process lock file per space, used to
	// make sure only a single instance rebuilds a space at any given time.
	_pendingLocksDir = "space-locks"
)

// pendingSpace is the persisted state of one space whose index is not yet in
// sync with storage.
type pendingSpace struct {
	// Generation is bumped for every change that is scheduled for the space.
	// A rebuild may only clear the pending state when it finishes the latest
	// known generation, otherwise changes arriving during the run would be
	// lost.
	Generation uint64 `json:"generation"`
	// EnqueuedAt records when the latest change for the space was accepted.
	EnqueuedAt time.Time `json:"enqueued_at"`
}

type pendingStoreData struct {
	Version int                    `json:"version"`
	Spaces  map[string]pendingSpace `json:"spaces"`
}

// pendingStore persists spaces waiting for a successful index run. Every
// accepted change is persisted immediately, so a crash before the debounce
// timer fires or during a rebuild leaves a state the next startup can resume.
type pendingStore interface {
	// List returns all currently pending spaces.
	List() (map[string]pendingSpace, error)
	// Add marks a space as pending, bumping its generation, and returns the
	// new generation.
	Add(spaceID string) (uint64, error)
	// Done clears the pending state of a space when its stored generation
	// exactly matches generation. It returns false without changing anything
	// when a newer generation superseded the one that just finished or when
	// another consumer already cleared it. A generation of 0 is a no-op.
	Done(spaceID string, generation uint64) (bool, error)
	Close() error
}

// memoryPendingStore is an in-process pendingStore used when no persistent
// data path is configured (tests and in-memory indexes).
type memoryPendingStore struct {
	mu     sync.Mutex
	spaces map[string]pendingSpace
}

func newMemoryPendingStore() *memoryPendingStore {
	return &memoryPendingStore{spaces: map[string]pendingSpace{}}
}

func (s *memoryPendingStore) List() (map[string]pendingSpace, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]pendingSpace, len(s.spaces))
	for id, sp := range s.spaces {
		out[id] = sp
	}
	return out, nil
}

func (s *memoryPendingStore) Add(spaceID string) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	sp := s.spaces[spaceID]
	sp.Generation++
	sp.EnqueuedAt = time.Now()
	s.spaces[spaceID] = sp
	return sp.Generation, nil
}

func (s *memoryPendingStore) Done(spaceID string, generation uint64) (bool, error) {
	if generation == 0 {
		return false, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.spaces[spaceID].Generation != generation {
		return false, nil
	}
	delete(s.spaces, spaceID)
	return true, nil
}

func (s *memoryPendingStore) Close() error { return nil }

// filePendingStore persists pending spaces as JSON next to the search index.
// All access is cross-process safe: mutations hold an flock during the whole
// read-modify-write cycle, and the JSON file is replaced atomically.
type filePendingStore struct {
	path   string
	logger log.Logger

	mu   sync.Mutex
	flk  *flock.Flock
}

// newFilePendingStore creates (or opens) the persistent pending store inside
// the given search data directory.
func newFilePendingStore(datapath string, logger log.Logger) (*filePendingStore, error) {
	if datapath == "" {
		return nil, errors.New("a data path is required for the persistent pending store")
	}
	if err := os.MkdirAll(datapath, 0o700); err != nil {
		return nil, fmt.Errorf("create search data directory %s: %w", datapath, err)
	}
	s := &filePendingStore{
		path:   filepath.Join(datapath, _pendingStoreFile),
		logger: logger,
		flk:    flock.New(filepath.Join(datapath, _pendingStoreLock)),
	}
	return s, nil
}

// withFileLock runs fn while holding the cross-process store lock.
func (s *filePendingStore) withFileLock(fn func() error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.flk.Lock(); err != nil {
		return fmt.Errorf("acquire pending store lock: %w", err)
	}
	defer func() { _ = s.flk.Unlock() }()

	return fn()
}

func (s *filePendingStore) List() (map[string]pendingSpace, error) {
	out := map[string]pendingSpace{}
	err := s.withFileLock(func() error {
		data, err := s.loadLocked()
		if err != nil {
			return err
		}
		for id, sp := range data.Spaces {
			out[id] = sp
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *filePendingStore) Add(spaceID string) (uint64, error) {
	var generation uint64
	err := s.withFileLock(func() error {
		data, err := s.loadLocked()
		if err != nil {
			return err
		}
		sp := data.Spaces[spaceID]
		sp.Generation++
		sp.EnqueuedAt = time.Now()
		data.Spaces[spaceID] = sp
		generation = sp.Generation
		return s.saveLocked(data)
	})
	if err != nil {
		return 0, err
	}
	return generation, nil
}

func (s *filePendingStore) Done(spaceID string, generation uint64) (bool, error) {
	if generation == 0 {
		return false, nil
	}
	cleared := false
	err := s.withFileLock(func() error {
		data, err := s.loadLocked()
		if err != nil {
			return err
		}
		// Only clear when exactly the generation that just finished is still
		// the latest one. A newer generation (a change accepted during the
		// run) or an already-cleared entry must never be overwritten.
		if data.Spaces[spaceID].Generation != generation {
			return nil
		}
		delete(data.Spaces, spaceID)
		cleared = true
		return s.saveLocked(data)
	})
	if err != nil {
		return false, err
	}
	return cleared, nil
}

func (s *filePendingStore) Close() error { return nil }

// loadLocked reads and decodes the store. The caller must hold the file lock.
// A missing file means nothing is pending. A corrupt file is quarantined
// instead of wedging the service forever.
func (s *filePendingStore) loadLocked() (*pendingStoreData, error) {
	data := &pendingStoreData{
		Version: _pendingStoreVersion,
		Spaces:  map[string]pendingSpace{},
	}

	raw, err := os.ReadFile(s.path)
	if errors.Is(err, fs.ErrNotExist) {
		return data, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read pending store %s: %w", s.path, err)
	}
	if err := json.Unmarshal(raw, data); err != nil {
		quarantined := fmt.Sprintf("%s.corrupt-%s", s.path, time.Now().Format("20060102T150405.000000000"))
		if renameErr := os.Rename(s.path, quarantined); renameErr != nil {
			s.logger.Error().Err(renameErr).Str("path", s.path).Msg("could not quarantine corrupt pending store")
			return nil, fmt.Errorf("parse pending store %s: %w", s.path, err)
		}
		s.logger.Warn().Err(err).Str("path", s.path).Str("quarantined", quarantined).
			Msg("pending store was corrupt and has been quarantined, pending spaces are rebuilt on demand")
		return &pendingStoreData{Version: _pendingStoreVersion, Spaces: map[string]pendingSpace{}}, nil
	}
	if data.Spaces == nil {
		data.Spaces = map[string]pendingSpace{}
	}
	return data, nil
}

// saveLocked encodes and atomically replaces the store file. The caller must
// hold the file lock, which also makes the remove-and-rename fallback on
// Windows safe against concurrent readers/writers.
func (s *filePendingStore) saveLocked(data *pendingStoreData) error {
	data.Version = _pendingStoreVersion
	raw, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf("encode pending store: %w", err)
	}

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".pending-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary pending store: %w", err)
	}
	tmpName := tmp.Name()
	defer func() { _ = os.Remove(tmpName) }()

	if _, err = tmp.Write(raw); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write temporary pending store: %w", err)
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("flush temporary pending store: %w", err)
	}
	if err = tmp.Close(); err != nil {
		return fmt.Errorf("close temporary pending store: %w", err)
	}

	if err = os.Rename(tmpName, s.path); err != nil {
		// On some Windows versions os.Rename cannot replace an existing file.
		// The store lock guarantees no other process touches the file.
		if runtime.GOOS == "windows" {
			if removeErr := os.Remove(s.path); removeErr != nil && !errors.Is(removeErr, fs.ErrNotExist) {
				return fmt.Errorf("replace pending store: %w", err)
			}
			if err = os.Rename(tmpName, s.path); err != nil {
				return fmt.Errorf("replace pending store: %w", err)
			}
		} else {
			return fmt.Errorf("replace pending store: %w", err)
		}
	}
	return nil
}

// spaceLocker serializes index runs of one space across consumers and
// instances.
type spaceLocker interface {
	// tryLock attempts to acquire the rebuild lock without blocking.
	// ok is false when another consumer or instance currently rebuilds the
	// space; release must only be called when ok is true.
	tryLock(spaceID string) (release func(), ok bool, err error)
	// lock acquires the rebuild lock, waiting for other processes. It honors
	// ctx for manual index requests that carry a deadline.
	lock(ctx context.Context, spaceID string) (release func(), err error)
}

// nopSpaceLocker is used together with the memory store: the debouncer's
// in-process guards already serialize consumers within one instance.
type nopSpaceLocker struct{}

func (nopSpaceLocker) tryLock(string) (func(), bool, error) {
	return func() {}, true, nil
}

func (nopSpaceLocker) lock(context.Context, string) (func(), error) {
	return func() {}, nil
}

// fileSpaceLocker uses one flock file per space so that, in scaled
// deployments, two search instances cannot rebuild and publish the same space
// concurrently. Locks are released automatically by the OS if a process
// crashes, letting the next startup or another instance take over.
type fileSpaceLocker struct {
	dir string

	mu     sync.Mutex
	flocks map[string]*flock.Flock
}

func newFileSpaceLocker(datapath string) (*fileSpaceLocker, error) {
	dir := filepath.Join(datapath, _pendingLocksDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create space lock directory %s: %w", dir, err)
	}
	return &fileSpaceLocker{
		dir:    dir,
		flocks: map[string]*flock.Flock{},
	}, nil
}

func (l *fileSpaceLocker) pathFor(spaceID string) string {
	// Space ids are opaque and may contain characters that are awkward in
	// file names, so use a hex encoded, file system safe name.
	name := hex.EncodeToString([]byte(spaceID)) + ".lock"
	return filepath.Join(l.dir, name)
}

func (l *fileSpaceLocker) flockFor(spaceID string) *flock.Flock {
	l.mu.Lock()
	defer l.mu.Unlock()

	// flock files are never removed, so keying by the file name is enough.
	key := l.pathFor(spaceID)
	if f, ok := l.flocks[key]; ok {
		return f
	}
	f := flock.New(key)
	l.flocks[key] = f
	return f
}

func (l *fileSpaceLocker) tryLock(spaceID string) (func(), bool, error) {
	f := l.flockFor(spaceID)
	ok, err := f.TryLock()
	if err != nil {
		return nil, false, fmt.Errorf("acquire space lock: %w", err)
	}
	if !ok {
		return nil, false, nil
	}
	return func() { _ = f.Unlock() }, true, nil
}

func (l *fileSpaceLocker) lock(ctx context.Context, spaceID string) (func(), error) {
	f := l.flockFor(spaceID)
	for {
		ok, err := f.TryLock()
		if err != nil {
			return nil, fmt.Errorf("acquire space lock: %w", err)
		}
		if ok {
			return func() { _ = f.Unlock() }, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// sortedSpaceIDs returns the keys of m sorted. Sorting makes startup recovery
// deterministic.
func sortedSpaceIDs(m map[string]pendingSpace) []string {
	ids := make([]string, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}
