package search_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	sprovider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	"github.com/owncloud/ocis/v2/services/search/pkg/search"
)

const _pendingStoreFileName = "pending_spaces.json"

func readPendingSpaces(dir string) (map[string]json.RawMessage, error) {
	raw, err := os.ReadFile(filepath.Join(dir, _pendingStoreFileName))
	if err != nil {
		return nil, err
	}
	var data struct {
		Spaces map[string]json.RawMessage `json:"spaces"`
	}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	return data.Spaces, nil
}

var _ = Describe("Persistent SpaceDebouncer", func() {
	var (
		dir string
		ctx = context.Background()
		sid = &sprovider.StorageSpaceId{OpaqueId: "storageid$spaceid!spaceid"}
	)

	BeforeEach(func() {
		var err error
		dir, err = os.MkdirTemp("", "search-pending-")
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		_ = os.RemoveAll(dir)
	})

	It("persists an accepted change before the timer fires and recovers it after a restart", func() {
		var runs atomic.Int32

		// The first instance accepts the change but is stopped before the
		// debounce window elapses.
		d1, err := search.NewPersistentSpaceDebouncer(time.Hour, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			runs.Add(1)
			return nil
		}, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())

		d1.Debounce(ctx, sid)
		Eventually(func() []string {
			spaces, err := readPendingSpaces(dir)
			Expect(err).ToNot(HaveOccurred())
			ids := make([]string, 0, len(spaces))
			for id := range spaces {
				ids = append(ids, id)
			}
			return ids
		}, "1s", "10ms").Should(ContainElement(sid.OpaqueId))

		Expect(d1.Close(ctx)).To(Succeed())
		Expect(int(runs.Load())).To(Equal(0))

		// A fresh instance finds the pending space on startup and converges,
		// without any new event arriving.
		d2, err := search.NewPersistentSpaceDebouncer(40*time.Millisecond, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			runs.Add(1)
			return nil
		}, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())
		d2.Start()
		defer func() { _ = d2.Close(ctx) }()

		Eventually(func() int { return int(runs.Load()) }, "2s", "10ms").Should(Equal(1))
		Eventually(func() int {
			spaces, err := readPendingSpaces(dir)
			Expect(err).ToNot(HaveOccurred())
			return len(spaces)
		}, "1s", "10ms").Should(Equal(0))

		Consistently(func() int { return int(runs.Load()) }, "300ms", "20ms").Should(Equal(1))
	})

	It("does not clear changes that arrive while an earlier rebuild is running", func() {
		var runs atomic.Int32
		entered := make(chan struct{}, 2)
		proceed := make(chan struct{})

		d, err := search.NewPersistentSpaceDebouncer(40*time.Millisecond, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			runs.Add(1)
			entered <- struct{}{}
			<-proceed
			return nil
		}, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())
		defer func() { _ = d.Close(ctx) }()

		d.Debounce(ctx, sid)
		Eventually(entered, "2s").Should(Receive()) // first rebuild starts

		// Another change is accepted while the first rebuild is still running.
		d.Debounce(ctx, sid)
		close(proceed)

		Eventually(func() int { return int(runs.Load()) }, "2s", "10ms").Should(Equal(2))
		Eventually(func() int {
			spaces, err := readPendingSpaces(dir)
			Expect(err).ToNot(HaveOccurred())
			return len(spaces)
		}, "1s", "10ms").Should(Equal(0))
		Consistently(func() int { return int(runs.Load()) }, "300ms", "20ms").Should(Equal(2))
	})

	It("keeps the space pending when traversal fails and a later instance succeeds", func() {
		var failedAttempts atomic.Int32
		d1, err := search.NewPersistentSpaceDebouncer(40*time.Millisecond, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			failedAttempts.Add(1)
			return errors.New("space traversal failed")
		}, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())

		d1.Debounce(ctx, sid)
		Eventually(func() int { return int(failedAttempts.Load()) }, "2s", "10ms").Should(BeNumerically(">=", 1))
		Expect(d1.Close(ctx)).To(Succeed())

		// The failed generation is still present after the instance stopped.
		spaces, err := readPendingSpaces(dir)
		Expect(err).ToNot(HaveOccurred())
		Expect(spaces).To(HaveKey(sid.OpaqueId))

		var recoveredRuns atomic.Int32
		d2, err := search.NewPersistentSpaceDebouncer(40*time.Millisecond, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			recoveredRuns.Add(1)
			return nil
		}, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())
		d2.Start()
		defer func() { _ = d2.Close(ctx) }()

		Eventually(func() int { return int(recoveredRuns.Load()) }, "2s", "10ms").Should(Equal(1))
		Eventually(func() int {
			spaces, err := readPendingSpaces(dir)
			Expect(err).ToNot(HaveOccurred())
			return len(spaces)
		}, "1s", "10ms").Should(Equal(0))
	})

	It("clears a pending generation when a manual RunIndex covers it", func() {
		var runs atomic.Int32
		d, err := search.NewPersistentSpaceDebouncer(time.Hour, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			runs.Add(1)
			return nil
		}, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())
		defer func() { _ = d.Close(ctx) }()

		d.Debounce(ctx, sid) // pending, automatic timer far in the future

		Expect(d.RunIndex(ctx, sid)).To(Succeed())
		Expect(int(runs.Load())).To(Equal(1))
		spaces, err := readPendingSpaces(dir)
		Expect(err).ToNot(HaveOccurred())
		Expect(spaces).To(BeEmpty())

		// A manual run without pending changes still walks and leaves no state.
		Expect(d.RunIndex(ctx, sid)).To(Succeed())
		Expect(int(runs.Load())).To(Equal(2))
		spaces, err = readPendingSpaces(dir)
		Expect(err).ToNot(HaveOccurred())
		Expect(spaces).To(BeEmpty())

		Consistently(func() int { return int(runs.Load()) }, "200ms", "20ms").Should(Equal(2))
	})

	It("schedules the automatic follow-up when a change arrives during the manual walk", func() {
		var (
			runs     atomic.Int32
			entered  = make(chan struct{}, 2)
			proceed  = make(chan struct{})
			manualMu sync.Mutex
			manualErr error
		)

		d, err := search.NewPersistentSpaceDebouncer(40*time.Millisecond, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			runs.Add(1)
			entered <- struct{}{}
			<-proceed
			return nil
		}, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())
		defer func() { _ = d.Close(ctx) }()

		d.Debounce(ctx, sid) // gen 1

		go func() {
			err := d.RunIndex(ctx, sid)
			manualMu.Lock()
			manualErr = err
			manualMu.Unlock()
		}()
		Eventually(entered, "2s").Should(Receive())

		d.Debounce(ctx, sid) // gen 2, accepted during the manual walk
		close(proceed)

		Eventually(func() error {
			manualMu.Lock()
			defer manualMu.Unlock()
			return manualErr
		}, "2s", "10ms").Should(BeNil())
		Eventually(func() int { return int(runs.Load()) }, "2s", "10ms").Should(Equal(2))
		Eventually(func() int {
			spaces, err := readPendingSpaces(dir)
			Expect(err).ToNot(HaveOccurred())
			return len(spaces)
		}, "1s", "10ms").Should(Equal(0))
	})

	It("never runs two instances of the same space concurrently and converges without overwriting completions", func() {
		var (
			runs        atomic.Int32
			overlaps    atomic.Int32
			running     atomic.Int32
		)
		indexFunc := func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			runs.Add(1)
			if !running.CompareAndSwap(0, 1) {
				overlaps.Add(1)
			}
			time.Sleep(200 * time.Millisecond)
			running.Store(0)
			return nil
		}

		d1, err := search.NewPersistentSpaceDebouncer(30*time.Millisecond, indexFunc, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())
		d2, err := search.NewPersistentSpaceDebouncer(30*time.Millisecond, indexFunc, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())
		d1.Start()
		d2.Start()
		defer func() { _ = d1.Close(ctx) }()
		defer func() { _ = d2.Close(ctx) }()

		// Both instances accept the change for the same space.
		d1.Debounce(ctx, sid)
		d2.Debounce(ctx, sid)

		Eventually(func() int { return int(runs.Load()) }, "2s", "20ms").Should(BeNumerically(">=", 1))
		Eventually(func() int {
			spaces, err := readPendingSpaces(dir)
			Expect(err).ToNot(HaveOccurred())
			return len(spaces)
		}, "3s", "20ms").Should(Equal(0))

		// After convergence no further runs are scheduled, and runs never
		// overlapped.
		Eventually(func() int { return int(runs.Load()) }, "1s").Should(BeNumerically("<=", 2))
		Consistently(func() int { return int(runs.Load()) }, "400ms", "30ms").Should(Equal(2))
		Expect(int(overlaps.Load())).To(Equal(0))
	})

	It("lets a graceful Close finish a started successful rebuild", func() {
		var runs atomic.Int32
		d, err := search.NewPersistentSpaceDebouncer(40*time.Millisecond, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			runs.Add(1)
			time.Sleep(150 * time.Millisecond)
			return nil
		}, dir, log.NopLogger())
		Expect(err).ToNot(HaveOccurred())

		d.Debounce(ctx, sid)
		time.Sleep(100 * time.Millisecond) // let the rebuild start

		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		Expect(d.Close(shutdownCtx)).To(Succeed())

		Expect(int(runs.Load())).To(Equal(1))
		spaces, err := readPendingSpaces(dir)
		Expect(err).ToNot(HaveOccurred())
		Expect(spaces).To(BeEmpty())
	})
})
