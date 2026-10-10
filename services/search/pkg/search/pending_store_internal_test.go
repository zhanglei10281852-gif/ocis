package search

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	sprovider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
)

func TestMemoryPendingStoreGenerations(t *testing.T) {
	s := newMemoryPendingStore()

	gen, err := s.Add("space")
	if err != nil || gen != 1 {
		t.Fatalf("first Add: gen=%d err=%v", gen, err)
	}
	gen, err = s.Add("space")
	if err != nil || gen != 2 {
		t.Fatalf("second Add: gen=%d err=%v", gen, err)
	}

	// An older generation cannot clear a newer one.
	cleared, err := s.Done("space", 1)
	if err != nil || cleared {
		t.Fatalf("stale Done: cleared=%v err=%v", cleared, err)
	}
	pending, _ := s.List()
	if len(pending) != 1 {
		t.Fatalf("space must stay pending after a stale completion, got %v", pending)
	}

	cleared, err = s.Done("space", 2)
	if err != nil || !cleared {
		t.Fatalf("latest Done: cleared=%v err=%v", cleared, err)
	}
	pending, _ = s.List()
	if len(pending) != 0 {
		t.Fatalf("space must be cleared after the latest completion, got %v", pending)
	}

	// A repeated completion of the same generation must not resurrect state.
	cleared, err = s.Done("space", 2)
	if err != nil || cleared {
		t.Fatalf("duplicate Done: cleared=%v err=%v", cleared, err)
	}
}

func TestFilePendingStorePersistsAcrossInstances(t *testing.T) {
	dir := t.TempDir()

	s1, err := newFilePendingStore(dir, log.NopLogger())
	if err != nil {
		t.Fatal(err)
	}
	if gen, err := s1.Add("space-a"); err != nil || gen != 1 {
		t.Fatalf("Add space-a: gen=%d err=%v", gen, err)
	}
	if gen, err := s1.Add("space-b"); err != nil || gen != 1 {
		t.Fatalf("Add space-b: gen=%d err=%v", gen, err)
	}
	if gen, err := s1.Add("space-a"); err != nil || gen != 2 {
		t.Fatalf("second Add space-a: gen=%d err=%v", gen, err)
	}

	// The change is durable before any timer fires.
	raw, err := os.ReadFile(filepath.Join(dir, _pendingStoreFile))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "space-a") || !strings.Contains(string(raw), "space-b") {
		t.Fatalf("pending spaces not persisted: %s", raw)
	}

	// Simulate a restart: a new instance sees exactly the persisted state.
	s2, err := newFilePendingStore(dir, log.NopLogger())
	if err != nil {
		t.Fatal(err)
	}
	pending, err := s2.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 2 || pending["space-a"].Generation != 2 || pending["space-b"].Generation != 1 {
		t.Fatalf("recovered state mismatch: %+v", pending)
	}

	// Completion from the new instance is visible to the old one.
	cleared, err := s2.Done("space-a", 2)
	if err != nil || !cleared {
		t.Fatalf("Done from s2: cleared=%v err=%v", cleared, err)
	}
	pending, _ = s1.List()
	if _, ok := pending["space-a"]; ok {
		t.Fatalf("space-a should be gone for s1 as well: %+v", pending)
	}
	if _, ok := pending["space-b"]; !ok {
		t.Fatal("space-b should still be pending")
	}

	// A stale generation from a finished run cannot remove newer pending state.
	cleared, err = s1.Done("space-b", 42)
	if err != nil || cleared {
		t.Fatalf("stale Done across instances: cleared=%v err=%v", cleared, err)
	}
}

func TestFilePendingStoreQuarantinesCorruptFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, _pendingStoreFile)
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := newFilePendingStore(dir, log.NopLogger())
	if err != nil {
		t.Fatal(err)
	}
	pending, err := s.List()
	if err != nil {
		t.Fatalf("corrupt store should be quarantined, not fatal: %v", err)
	}
	if len(pending) != 0 {
		t.Fatalf("expected empty recovered store, got %v", pending)
	}
	if gen, err := s.Add("space"); err != nil || gen != 1 {
		t.Fatalf("Add after quarantine: gen=%d err=%v", gen, err)
	}
}

func TestFileSpaceLockerSerializesAcrossInstances(t *testing.T) {
	dir := t.TempDir()
	l1, err := newFileSpaceLocker(dir)
	if err != nil {
		t.Fatal(err)
	}
	l2, err := newFileSpaceLocker(dir)
	if err != nil {
		t.Fatal(err)
	}

	release, ok, err := l1.tryLock("space-a")
	if err != nil || !ok {
		t.Fatalf("first tryLock: ok=%v err=%v", ok, err)
	}

	if _, ok, err := l2.tryLock("space-a"); err != nil || ok {
		t.Fatalf("second tryLock must fail while held: ok=%v err=%v", ok, err)
	}

	// A different space is unaffected.
	relOther, ok, err := l2.tryLock("space-other")
	if err != nil || !ok {
		t.Fatalf("other space lock: ok=%v err=%v", ok, err)
	}
	relOther()

	// Blocking lock waits for the holder and honors the context.
	ctx, cancel := context.WithCancel(context.Background())
	blocked := make(chan error, 1)
	go func() {
		rel, err := l2.lock(ctx, "space-a")
		if err == nil {
			rel()
		}
		blocked <- err
	}()
	select {
	case err := <-blocked:
		t.Fatalf("lock should block, returned early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	select {
	case err := <-blocked:
		if err == nil {
			t.Fatal("expected context cancellation error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked lock did not react to context cancellation")
	}

	release()

	rel2, err := l2.lock(context.Background(), "space-a")
	if err != nil {
		t.Fatalf("lock after release: %v", err)
	}
	rel2()
}

// TestPersistentDebouncerWiresFileStore is a compile-time guard that the
// persistent constructor wires the file based store and locker.
func TestPersistentDebouncerWiresFileStore(t *testing.T) {
	dir := t.TempDir()
	d, err := NewPersistentSpaceDebouncer(time.Hour, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
		return nil
	}, dir, log.NopLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = d.Close(context.Background()) }()

	if _, ok := d.store.(*filePendingStore); !ok {
		t.Fatalf("expected filePendingStore, got %T", d.store)
	}
	if _, ok := d.locks.(*fileSpaceLocker); !ok {
		t.Fatalf("expected fileSpaceLocker, got %T", d.locks)
	}
}
