package search

import (
	"context"
	"sync"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
)

// IndexFunc rebuilds the search index of one space. It must return the error
// of the space traversal or index commit so that the pending state is kept on
// failure and only a later successful run can clear it.
type IndexFunc func(ctx context.Context, id *provider.StorageSpaceId) error

// _maxRetryBackoff caps the delay between failed rebuild attempts.
const _maxRetryBackoff = 1 * time.Minute

// SpaceDebouncer debounces index runs on spaces for a configurable amount of
// time.
//
// In addition to plain debouncing it guarantees that every accepted change is
// eventually reflected in the index:
//
//   - Every Debounce call is persisted with a per-space generation before the
//     timer is armed, so a crash inside the debounce window is recovered on the
//     next startup.
//   - Changes arriving while a rebuild is running bump the generation again;
//     the finishing run cannot clear them and another run is scheduled.
//   - A per-space lock (in-process and, with a persistent store, cross-process)
//     makes sure consumers or instances never publish overlapping completion
//     results for the same space.
//   - Failed traversals/index commits keep the space pending and are retried
//     with a capped backoff; only success clears the latest generation.
//   - Close stops timers and waits for started runs; anything still pending
//     remains persisted for the next startup.
type SpaceDebouncer struct {
	after  time.Duration
	f      IndexFunc
	store  pendingStore
	locks  spaceLocker
	logger log.Logger

	mutex    sync.Mutex
	timers   map[string]*time.Timer
	gens     map[string]uint64
	lastCtx  map[string]context.Context
	failures map[string]int
	guards   map[string]chan struct{}
	wg       sync.WaitGroup

	started bool
	closed  bool
}

// NewSpaceDebouncer returns a new in-memory SpaceDebouncer instance.
//
// Pending state is not persisted and runs are only serialized within the
// process; it is kept for setups without a persistent data path.
func NewSpaceDebouncer(d time.Duration, f func(ctx context.Context, id *provider.StorageSpaceId)) *SpaceDebouncer {
	return newSpaceDebouncer(d, func(ctx context.Context, id *provider.StorageSpaceId) error {
		f(ctx, id)
		return nil
	}, newMemoryPendingStore(), nopSpaceLocker{}, log.NopLogger())
}

// NewPersistentSpaceDebouncer returns a SpaceDebouncer that survives restarts.
// Pending spaces are persisted next to the search index in datapath and
// rebuilds of one space are serialized across all service instances sharing
// that path.
func NewPersistentSpaceDebouncer(d time.Duration, f IndexFunc, datapath string, logger log.Logger) (*SpaceDebouncer, error) {
	store, err := newFilePendingStore(datapath, logger)
	if err != nil {
		return nil, err
	}
	locks, err := newFileSpaceLocker(datapath)
	if err != nil {
		return nil, err
	}
	return newSpaceDebouncer(d, f, store, locks, logger), nil
}

func newSpaceDebouncer(d time.Duration, f IndexFunc, store pendingStore, locks spaceLocker, logger log.Logger) *SpaceDebouncer {
	return &SpaceDebouncer{
		after:    d,
		f:        f,
		store:    store,
		locks:    locks,
		logger:   logger,
		timers:   map[string]*time.Timer{},
		gens:     map[string]uint64{},
		lastCtx:  map[string]context.Context{},
		failures: map[string]int{},
		guards:   map[string]chan struct{}{},
	}
}

// Debounce restarts the debounce timer for the given space and persists the
// pending generation so the change survives a crash before the timer fires.
// Consecutive events on the same space are still merged into one timer.
func (d *SpaceDebouncer) Debounce(ctx context.Context, id *provider.StorageSpaceId) {
	spaceID := id.OpaqueId

	d.mutex.Lock()
	defer d.mutex.Unlock()

	gen, err := d.store.Add(spaceID)
	if err != nil {
		// Persistence failed: still schedule the run in this process instead
		// of dropping the change, using a locally increasing generation.
		d.logger.Error().Err(err).Str("space", spaceID).Msg("could not persist pending space, the change is only tracked until restart")
		gen = d.gens[spaceID] + 1
	}
	d.gens[spaceID] = gen
	d.failures[spaceID] = 0
	if ctx != nil {
		d.lastCtx[spaceID] = ctx
	}
	if !d.closed {
		d.armLocked(spaceID, d.after)
	}
}

// RunIndex executes a rebuild on demand (manual IndexSpace) under the same
// serialization as automatic runs. When the space has a pending generation,
// its pending state is cleared on success if no newer change arrived during
// the walk, so a manual rebuild converges to the same final index as the
// automatic recovery path.
func (d *SpaceDebouncer) RunIndex(ctx context.Context, id *provider.StorageSpaceId) error {
	spaceID := id.OpaqueId

	// Wait for an automatic run in this process to finish before acquiring
	// the cross-process lock; this also prevents same-process double runs.
	ch := d.guardCh(spaceID)
	select {
	case ch <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	releaseGuard := func() { <-ch }

	release, err := d.locks.lock(ctx, spaceID)
	if err != nil {
		releaseGuard()
		return err
	}
	defer func() {
		release()
		releaseGuard()
	}()

	// Capture the generation that this walk is about to cover, now that no
	// other run for the space can be in progress.
	d.mutex.Lock()
	gen := d.gens[spaceID]
	d.mutex.Unlock()

	if err := d.f(ctx, id); err != nil {
		return err
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.resolveLocked(spaceID, gen, nil)
}

// Start re-arms the debounce timers for spaces that were still pending when a
// previous instance stopped. It does not scan all spaces: only spaces found in
// the persistent store are scheduled, an idle startup does nothing.
func (d *SpaceDebouncer) Start() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.started || d.closed {
		return
	}
	d.started = true

	pending, err := d.store.List()
	if err != nil {
		d.logger.Error().Err(err).Msg("could not load pending spaces, previously accepted changes may be delayed")
		return
	}
	if len(pending) == 0 {
		return
	}

	d.logger.Info().Int("spaces", len(pending)).Msg("recovering pending space index runs")
	for _, spaceID := range sortedSpaceIDs(pending) {
		d.gens[spaceID] = pending[spaceID].Generation
		if d.lastCtx[spaceID] == nil {
			d.lastCtx[spaceID] = context.Background()
		}
		d.armLocked(spaceID, d.after)
	}
}

// Close stops accepting new timer runs and waits for runs that already
// started, bounded by ctx. Pending spaces are never removed here: runs which
// do not finish in time, or whose process is killed, stay persisted and are
// taken over by the next startup.
func (d *SpaceDebouncer) Close(ctx context.Context) error {
	d.mutex.Lock()
	d.closed = true
	for spaceID, t := range d.timers {
		t.Stop()
		delete(d.timers, spaceID)
	}
	d.mutex.Unlock()

	finished := make(chan struct{})
	go func() {
		d.wg.Wait()
		close(finished)
	}()

	select {
	case <-finished:
	case <-ctx.Done():
		d.logger.Warn().Msg("shutdown deadline reached while space index runs are still in progress, they remain pending for the next startup")
	}

	return d.store.Close()
}

// guardCh returns the in-process serialization channel for a space.
func (d *SpaceDebouncer) guardCh(spaceID string) chan struct{} {
	d.mutex.Lock()
	defer d.mutex.Unlock()
	return d.guardChLocked(spaceID)
}

func (d *SpaceDebouncer) guardChLocked(spaceID string) chan struct{} {
	ch, ok := d.guards[spaceID]
	if !ok {
		ch = make(chan struct{}, 1)
		d.guards[spaceID] = ch
	}
	return ch
}

// armLocked (re)starts the timer for a space. Callers must hold d.mutex.
func (d *SpaceDebouncer) armLocked(spaceID string, delay time.Duration) {
	if t := d.timers[spaceID]; t != nil {
		t.Stop()
	}
	d.timers[spaceID] = time.AfterFunc(delay, func() {
		d.fire(spaceID)
	})
}

// retryDelayLocked computes a capped exponential backoff after failed runs.
// Callers must hold d.mutex.
func (d *SpaceDebouncer) retryDelayLocked(spaceID string) time.Duration {
	fails := d.failures[spaceID]
	delay := d.after
	if delay <= 0 {
		// Avoid a hot retry loop when debouncing was configured without delay.
		delay = time.Millisecond
	}
	for i := 0; i < fails && delay < _maxRetryBackoff; i++ {
		delay *= 2
		if delay > _maxRetryBackoff {
			delay = _maxRetryBackoff
		}
	}
	return delay
}

// fire is the timer callback: it reserves the in-process guard, acquires the
// cross-process space lock and runs the rebuild for the latest generation.
func (d *SpaceDebouncer) fire(spaceID string) {
	d.mutex.Lock()
	if d.closed {
		d.mutex.Unlock()
		return
	}
	delete(d.timers, spaceID)

	gen := d.gens[spaceID]
	if gen == 0 {
		// Nothing pending anymore, e.g. a manual rebuild already covered it.
		d.mutex.Unlock()
		return
	}

	ch := d.guardChLocked(spaceID)
	select {
	case ch <- struct{}{}:
	default:
		// A run on this space is already active in this process. The run's
		// post-check reschedules itself when the generation advanced.
		d.armLocked(spaceID, d.after)
		d.mutex.Unlock()
		return
	}
	// Register while holding d.mutex so Close can never race Add with Wait.
	d.wg.Add(1)
	ctx := d.lastCtx[spaceID]
	d.mutex.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}

	release, ok, err := d.locks.tryLock(spaceID)
	if err != nil {
		d.logger.Error().Err(err).Str("space", spaceID).Msg("could not acquire space index lock")
		<-ch
		d.wg.Done()
		d.mutex.Lock()
		if !d.closed {
			d.armLocked(spaceID, d.retryDelayLocked(spaceID))
		}
		d.mutex.Unlock()
		return
	}
	if !ok {
		// Another instance is rebuilding this space. It keeps the pending
		// state and will clear it on success; just check back afterwards.
		<-ch
		d.wg.Done()
		d.mutex.Lock()
		if !d.closed && d.gens[spaceID] != 0 {
			d.armLocked(spaceID, d.after)
		}
		d.mutex.Unlock()
		return
	}

	// Done only after finalizing the pending state, so Close never returns
	// before a successful completion has been persisted.
	defer d.wg.Done()

	runErr := d.f(ctx, &provider.StorageSpaceId{OpaqueId: spaceID})
	release()
	<-ch

	d.mutex.Lock()
	defer d.mutex.Unlock()
	d.resolveLocked(spaceID, gen, runErr)
}

// resolveLocked finalizes a run: failures keep the space pending and are
// retried; success clears the captured generation only if no newer change
// arrived, otherwise another run is scheduled. Callers must hold d.mutex.
func (d *SpaceDebouncer) resolveLocked(spaceID string, gen uint64, runErr error) error {
	if runErr != nil {
		d.failures[spaceID]++
		if d.closed {
			return runErr
		}
		d.logger.Error().
			Err(runErr).
			Str("space", spaceID).
			Int("attempt", d.failures[spaceID]).
			Msg("space indexing failed, the space stays pending and will be retried")
		d.armLocked(spaceID, d.retryDelayLocked(spaceID))
		return runErr
	}

	d.failures[spaceID] = 0

	// CAS in the store: the entry is only cleared when no newer generation
	// superseded the run that just finished.
	cleared, err := d.store.Done(spaceID, gen)
	if err != nil {
		d.logger.Error().Err(err).Str("space", spaceID).Msg("could not mark space as indexed, it stays pending")
		if !d.closed {
			d.armLocked(spaceID, d.after)
		}
		return err
	}

	if cleared {
		delete(d.gens, spaceID)
		delete(d.lastCtx, spaceID)
		if t := d.timers[spaceID]; t != nil {
			// A timer may only exist if a change arrived during the run, in
			// which case Done would not have cleared the entry. Stop it
			// defensively to avoid a redundant scan.
			t.Stop()
			delete(d.timers, spaceID)
		}
		return nil
	}

	if d.gens[spaceID] == gen {
		// No newer change was seen locally, yet the generation is gone from
		// the shared store: another consumer or instance finished the same
		// generation. Resync the local mirror instead of re-running forever.
		delete(d.gens, spaceID)
		delete(d.lastCtx, spaceID)
		if t := d.timers[spaceID]; t != nil {
			t.Stop()
			delete(d.timers, spaceID)
		}
		return nil
	}

	if d.gens[spaceID] == 0 {
		// Already resynced while the run was finishing.
		if t := d.timers[spaceID]; t != nil {
			t.Stop()
			delete(d.timers, spaceID)
		}
		return nil
	}

	// A newer generation exists because a change was accepted while this run
	// was active; schedule the run that covers it.
	if !d.closed && d.timers[spaceID] == nil {
		d.armLocked(spaceID, d.after)
	}
	return nil
}
