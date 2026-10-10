package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/r3labs/sse/v2"

	revactx "github.com/owncloud/reva/v2/pkg/ctx"
	"github.com/owncloud/reva/v2/pkg/events"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	ehmsg "github.com/owncloud/ocis/v2/protogen/gen/ocis/messages/eventhistory/v0"
	ehsvc "github.com/owncloud/ocis/v2/protogen/gen/ocis/services/eventhistory/v0"
	"github.com/owncloud/ocis/v2/services/sse/pkg/config"
)

const (
	// EventTypeResync is the SSE event name used to tell a client that a lossless
	// resumption was not possible. After a resync event the client only receives
	// events published at or after the freshly established live boundary.
	EventTypeResync = "resync"

	// Resync reasons, sent in the resync event payload.
	ResyncReasonCursorMalformed = "cursor_malformed"
	ResyncReasonCursorUnknown   = "cursor_unknown"

	historyCallTimeout = 30 * time.Second
	liveSubTimeout     = 10 * time.Second

	// nonceQueryKey tags an internal subscriber so the OnSubscribe callback can
	// route it back to the owning handler invocation.
	nonceQueryKey = "sse_resume_nonce"
)

var sendSSETypeName = reflect.TypeOf(events.SendSSE{}).String()

type resyncInfo struct {
	Reason string `json:"reason"`
}

// SSE defines implements the business logic for Service.
type SSE struct {
	c             *config.Config
	l             log.Logger
	m             *chi.Mux
	sse           *sse.Server
	evChannel     <-chan events.Event
	historyClient ehsvc.EventHistoryService

	subWaiters *subWaiterRegistry
}

// subWaiterRegistry tracks handler invocations waiting for their internal live
// subscriber to register. Held by pointer so copies of SSE share one registry.
type subWaiterRegistry struct {
	mu      sync.Mutex
	waiters map[string]chan struct{}
}

func newSubWaiterRegistry() *subWaiterRegistry {
	return &subWaiterRegistry{waiters: make(map[string]chan struct{})}
}

func (r *subWaiterRegistry) add(nonce string) chan struct{} {
	ch := make(chan struct{}, 1)
	r.mu.Lock()
	r.waiters[nonce] = ch
	r.mu.Unlock()
	return ch
}

func (r *subWaiterRegistry) remove(nonce string) {
	r.mu.Lock()
	delete(r.waiters, nonce)
	r.mu.Unlock()
}

func (r *subWaiterRegistry) signal(nonce string) {
	r.mu.Lock()
	ch := r.waiters[nonce]
	r.mu.Unlock()
	if ch != nil {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// NewSSE returns a service implementation for Service.
func NewSSE(c *config.Config, l log.Logger, ch <-chan events.Event, mux *chi.Mux, historyClient ehsvc.EventHistoryService) (SSE, error) {
	sseServer := sse.New()
	// streams must never replay the in-memory event log: the resume cursor is
	// handled via eventhistory. Setting it on the server before any stream is
	// created avoids racing with the stream goroutine on a later field mutation.
	sseServer.AutoReplay = false

	s := SSE{
		c:             c,
		l:             l,
		m:             mux,
		sse:           sseServer,
		evChannel:     ch,
		historyClient: historyClient,
		subWaiters:    newSubWaiterRegistry(),
	}

	// needed to hand internal (pre-fetch) live subscribers back to their handler
	s.sse.OnSubscribe = s.onSubscriberRegistered

	mux.Route("/ocs/v2.php/apps/notifications/api/v1/notifications", func(r chi.Router) {
		r.Get("/sse", s.HandleSSE)
	})

	go s.ListenForEvents()

	return s, nil
}

// ServeHTTP fulfills Handler interface
func (s SSE) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.m.ServeHTTP(w, r)
}

// ListenForEvents listens for events
func (s SSE) ListenForEvents() {
	for e := range s.evChannel {
		switch ev := e.Event.(type) {
		default:
			s.l.Error().Interface("event", ev).Msg("unhandled event")
		case events.SendSSE:
			for _, uid := range ev.UserIDs {
				// the bus event id is stable across sse service restarts and is
				// the same id the eventhistory service persisted the event under
				s.sse.Publish(uid, &sse.Event{
					ID:    []byte(e.ID),
					Event: []byte(ev.Type),
					Data:  ev.Message,
				})
			}
		}
	}
}

// HandleSSE is the GET handler for events
func (s SSE) HandleSSE(w http.ResponseWriter, r *http.Request) {
	u, ok := revactx.ContextGetUser(r.Context())
	if !ok {
		s.l.Error().Msg("sse: no user in context")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	uid := u.GetId().GetOpaqueId()
	if uid == "" {
		s.l.Error().Msg("sse: user in context is broken")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// creates the per-user stream (no subscribers yet; live boundary for
	// resume connections is established below before the history lookup)
	s.sse.CreateStream(uid)

	if s.c.KeepAliveInterval != 0 {
		ticker := time.NewTicker(s.c.KeepAliveInterval)
		defer ticker.Stop()
		go func() {
			for range ticker.C {
				s.sse.Publish(uid, &sse.Event{
					Comment: []byte("keepalive"),
				})
			}
		}()
	}

	cursor := r.Header.Get("Last-Event-ID")
	if cursor == "" {
		// no cursor: keep the existing live-only subscription semantics
		q := r.URL.Query()
		q.Set("stream", uid)
		r.URL.RawQuery = q.Encode()

		s.sse.ServeHTTP(w, r)
		return
	}

	// --- resumable subscription path ---

	// Establish the live boundary BEFORE accessing the eventhistory, so no
	// event published in the meantime can slip through. The internal request
	// shares the client request context, it is torn down on disconnect/return.
	live, err := s.attachLiveSubscriber(r, uid)
	if err != nil {
		s.l.Error().Err(err).Str("user_id", uid).Msg("sse: could not establish live boundary")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if _, err := uuid.Parse(cursor); err != nil {
		// malformed cursor: do not pretend a lossless resume is possible
		if !writeStreamHeader(w) || !s.writeResync(w, ResyncReasonCursorMalformed) {
			return
		}
		s.streamLive(r.Context(), w, live, nil)
		return
	}

	history, err := s.fetchHistory(r.Context(), uid)
	if err != nil {
		// No frame has been written yet: the client's cursor is untouched, no
		// partial replay leaked. A retry after recovery replays the same order.
		s.l.Error().Err(err).Str("user_id", uid).Msg("sse: cannot retrieve event history")
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	if !writeStreamHeader(w) {
		return
	}

	replay, cursorFound := buildReplay(history, uid, cursor)
	if !cursorFound {
		// well-formed cursor, but unknown or already expired: resync explicitly
		if !s.writeResync(w, ResyncReasonCursorUnknown) {
			return
		}
		s.streamLive(r.Context(), w, live, nil)
		return
	}

	// continuous replay up to the live boundary, then live events with the
	// replayed ids suppressed to avoid duplicates
	replayed := make(map[string]struct{}, len(replay))
	for _, ev := range replay {
		if !s.writeEvent(w, ev) {
			return
		}
		replayed[ev.GetId()] = struct{}{}
	}

	s.streamLive(r.Context(), w, live, replayed)
}

// liveSubscriber is a live subscription established before the history
// lookup. Cleanup happens through the shared client request context; when that
// cancels, r3labs deregisters and the parsed event channel is closed.
type liveSubscriber struct {
	events <-chan *sse.Event
}

// attachLiveSubscriber registers a fresh live subscriber on the user's stream
// and returns once the subscriber is registered, i.e. before the history call.
func (s SSE) attachLiveSubscriber(r *http.Request, uid string) (*liveSubscriber, error) {
	nonce := uuid.New().String()
	registered := s.subWaiters.add(nonce)
	events := make(chan *sse.Event, 64)
	defer s.subWaiters.remove(nonce)

	req := r.Clone(r.Context())
	q := url.Values{}
	q.Set("stream", uid)
	q.Set(nonceQueryKey, nonce)
	req.URL.RawQuery = q.Encode()
	// r3labs parses Last-Event-ID itself and only accepts numeric ids;
	// the cursor is handled by this handler, not by the internal call.
	req.Header.Del("Last-Event-ID")

	// r3labs writes live frames to the capture writer, which parses them back
	// into events. Direct access to the subscriber's unexported channel is not
	// possible, so the wire is used as the handoff boundary.
	capture := &captureResponseWriter{events: events}
	serveDone := make(chan struct{})
	go func() {
		s.sse.ServeHTTP(capture, req)
		close(serveDone)
	}()
	go func() {
		<-serveDone
		close(events)
	}()

	select {
	case <-registered:
		return &liveSubscriber{events: events}, nil
	case <-time.After(liveSubTimeout):
		return nil, fmt.Errorf("timed out waiting for live subscriber registration")
	case <-r.Context().Done():
		return nil, r.Context().Err()
	}
}

// onSubscriberRegistered is the r3labs OnSubscribe callback. It signals the
// handler invocation identified by a nonce that its live boundary is active.
func (s SSE) onSubscriberRegistered(_ string, sub *sse.Subscriber) {
	nonce := sub.URL.Query().Get(nonceQueryKey)
	if nonce == "" {
		return
	}

	s.subWaiters.signal(nonce)
}

// fetchHistory retrieves the persisted events matching the authenticated user.
func (s SSE) fetchHistory(ctx context.Context, uid string) ([]*ehmsg.Event, error) {
	if s.historyClient == nil {
		return nil, fmt.Errorf("no eventhistory client configured")
	}

	callCtx, cancel := context.WithTimeout(ctx, historyCallTimeout)
	defer cancel()

	resp, err := s.historyClient.GetEventsForUser(callCtx, &ehsvc.GetEventsForUserRequest{
		UserID: uid,
	})
	if err != nil {
		return nil, err
	}

	return resp.GetEvents(), nil
}

// replayEvent is a history event annotated with the ordering key derived from
// the business timestamp inside the notification payload.
type replayEvent struct {
	ev *ehmsg.Event
	ts string
}

// buildReplay turns the history snapshot into the deterministic, authorized
// replay list following the cursor. It returns false if the cursor is not part
// of the snapshot (unknown or expired).
func buildReplay(history []*ehmsg.Event, uid, cursor string) ([]*ehmsg.Event, bool) {
	ours := make([]replayEvent, 0, len(history))
	seen := make(map[string]struct{}, len(history))

	for _, ev := range history {
		if ev == nil || ev.GetId() == "" {
			continue
		}
		if _, dup := seen[ev.GetId()]; dup {
			continue
		}
		// The history lookup matches a regex over the raw payload and can
		// over-match. Only re-publish SendSSE events that explicitly list the
		// authenticated user as a recipient.
		if ev.GetType() != sendSSETypeName {
			continue
		}

		var msg events.SendSSE
		if err := json.Unmarshal(ev.GetEvent(), &msg); err != nil {
			continue
		}
		if !containsUser(msg.UserIDs, uid) {
			continue
		}

		seen[ev.GetId()] = struct{}{}
		ours = append(ours, replayEvent{ev: ev, ts: eventTimestamp(msg.Message)})
	}

	// Deterministic order independent of the store's listing order: business
	// timestamp first (so events the client already saw never sort after the
	// cursor), event id as a tie breaker. Identical snapshots produce identical
	// sequences on every (re)try.
	sort.Slice(ours, func(i, j int) bool {
		if ours[i].ts != ours[j].ts {
			return ours[i].ts < ours[j].ts
		}
		return ours[i].ev.GetId() < ours[j].ev.GetId()
	})

	cursorIdx := -1
	for i, item := range ours {
		if item.ev.GetId() == cursor {
			cursorIdx = i
			break
		}
	}
	if cursorIdx == -1 {
		return nil, false
	}

	out := make([]*ehmsg.Event, 0, len(ours)-cursorIdx-1)
	for _, item := range ours[cursorIdx+1:] {
		out = append(out, item.ev)
	}

	return out, true
}

// eventTimestamp extracts the business timestamp from a notification message
// in a best-effort manner. The notification payload carries an RFC3339 value
// under "datetime". Messages without it sort at the beginning by id, keeping
// the overall order deterministic.
func eventTimestamp(message []byte) string {
	var probe struct {
		Datetime string `json:"datetime"`
	}
	if err := json.Unmarshal(message, &probe); err != nil {
		return ""
	}
	return probe.Datetime
}

func containsUser(users []string, user string) bool {
	for _, u := range users {
		if u == user {
			return true
		}
	}
	return false
}

// writeStreamHeader starts the SSE response.
func writeStreamHeader(w http.ResponseWriter) bool {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	return true
}

// writeEvent replays one history event using the exact wire encoding r3labs
// uses for the equivalent live event.
func (s SSE) writeEvent(w http.ResponseWriter, ev *ehmsg.Event) bool {
	var msg events.SendSSE
	if err := json.Unmarshal(ev.GetEvent(), &msg); err != nil {
		return false
	}

	return writeFrame(w, &sse.Event{
		ID:    []byte(ev.GetId()),
		Event: []byte(msg.Type),
		Data:  msg.Message,
	})
}

// writeResync sends the observable resync signal. The event intentionally has
// no id, so it does not advance the cursor: the next business event establishes
// the new live boundary.
func (s SSE) writeResync(w http.ResponseWriter, reason string) bool {
	data, err := json.Marshal(resyncInfo{Reason: reason})
	if err != nil {
		return false
	}

	return writeFrame(w, &sse.Event{
		Event: []byte(EventTypeResync),
		Data:  data,
	})
}

// streamLive forwards live events to the client until the connection ends.
// Events whose id is in replayed already went out via replay and are
// suppressed here, making the replay/live handoff duplicate-free. Events not
// present in the replay snapshot are forwarded even if they arrived while the
// snapshot was being taken, keeping the handoff gap-free.
func (s SSE) streamLive(ctx context.Context, w http.ResponseWriter, live *liveSubscriber, replayed map[string]struct{}) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev, ok := <-live.events:
			if !ok {
				return
			}
			if len(ev.Data) == 0 && len(ev.Comment) == 0 {
				continue
			}
			if len(ev.Data) > 0 {
				if _, skip := replayed[string(ev.ID)]; skip {
					continue
				}
			}
			if !writeFrame(w, ev) {
				return
			}
		}
	}
}

// writeFrame serializes one event in the SSE wire format. It matches the
// encoding of r3labs' http handler so replayed and live events are
// indistinguishable on the wire.
func writeFrame(w http.ResponseWriter, ev *sse.Event) bool {
	var buf bytes.Buffer

	if len(ev.Data) > 0 {
		fmt.Fprintf(&buf, "id: %s\n", ev.ID)

		if bytes.HasPrefix(ev.Data, []byte(":")) {
			fmt.Fprintf(&buf, "%s\n", ev.Data)
		} else {
			fmt.Fprintf(&buf, "data: %s\n", ev.Data)
		}

		if len(ev.Event) > 0 {
			fmt.Fprintf(&buf, "event: %s\n", ev.Event)
		}
	}

	if len(ev.Comment) > 0 {
		fmt.Fprintf(&buf, ": %s\n", ev.Comment)
	}

	buf.WriteByte('\n')

	if _, err := w.Write(buf.Bytes()); err != nil {
		return false
	}

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	return true
}

// captureResponseWriter receives the frames r3labs writes for the internal live
// subscriber and turns them back into parsed events.
type captureResponseWriter struct {
	events chan<- *sse.Event
	buf    []byte
}

func (c *captureResponseWriter) Header() http.Header { return http.Header{} }

func (c *captureResponseWriter) WriteHeader(int) {}

func (c *captureResponseWriter) Flush() {}

func (c *captureResponseWriter) Write(b []byte) (int, error) {
	c.buf = append(c.buf, b...)
	c.dispatch()
	return len(b), nil
}

// dispatch forwards all complete SSE dispatches found in the buffer.
func (c *captureResponseWriter) dispatch() {
	for {
		idx, n := nextDispatchBoundary(c.buf)
		if idx < 0 {
			return
		}

		block := c.buf[:idx]
		c.buf = c.buf[idx+n:]

		if ev := parseEventBlock(block); ev != nil {
			c.events <- ev
		}
	}
}

// nextDispatchBoundary locates the blank line ending an SSE dispatch.
func nextDispatchBoundary(b []byte) (int, int) {
	if i := bytes.Index(b, []byte("\n\n")); i >= 0 {
		return i, 2
	}
	if i := bytes.Index(b, []byte("\r\n\r\n")); i >= 0 {
		return i, 4
	}
	return -1, 0
}

// parseEventBlock parses one SSE dispatch (without the trailing blank line).
func parseEventBlock(block []byte) *sse.Event {
	ev := &sse.Event{}
	hasComment := false

	for _, line := range bytes.Split(block, []byte("\n")) {
		line = bytes.TrimSuffix(line, []byte("\r"))

		switch {
		case bytes.HasPrefix(line, []byte(":")):
			ev.Comment = dropOneSpace(line[1:])
			hasComment = true
		case bytes.HasPrefix(line, []byte("data:")):
			if len(ev.Data) > 0 {
				ev.Data = append(ev.Data, '\n')
			}
			ev.Data = append(ev.Data, dropOneSpace(line[len("data:"):])...)
		case bytes.HasPrefix(line, []byte("id:")):
			ev.ID = dropOneSpace(line[len("id:"):])
		case bytes.HasPrefix(line, []byte("event:")):
			ev.Event = dropOneSpace(line[len("event:"):])
		}
	}

	if len(ev.Data) == 0 && len(ev.ID) == 0 && len(ev.Event) == 0 && !hasComment {
		return nil
	}

	return ev
}

// dropOneSpace removes the single optional leading space the SSE format allows
// after a field colon.
func dropOneSpace(b []byte) []byte {
	if len(b) > 0 && b[0] == ' ' {
		return b[1:]
	}
	return b
}
