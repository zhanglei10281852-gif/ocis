package service_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	userv1beta1 "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	ehmsg "github.com/owncloud/ocis/v2/protogen/gen/ocis/messages/eventhistory/v0"
	ehsvc "github.com/owncloud/ocis/v2/protogen/gen/ocis/services/eventhistory/v0"
	"github.com/owncloud/ocis/v2/services/sse/pkg/config"
	"github.com/owncloud/ocis/v2/services/sse/pkg/service"
	revactx "github.com/owncloud/reva/v2/pkg/ctx"
	"github.com/owncloud/reva/v2/pkg/events"
	microclient "go-micro.dev/v4/client"
)

const ssePath = "/ocs/v2.php/apps/notifications/api/v1/notifications/sse"

// ---------------------------------------------------------------------------
// test environment
// ---------------------------------------------------------------------------

type testEnv struct {
	t       *testing.T
	server  *httptest.Server
	bus     chan events.Event
	history *fakeHistory
}

func newTestEnv(t *testing.T, keepAlive time.Duration) *testEnv {
	t.Helper()

	mux := chi.NewMux()
	// emulate the proxy's authentication middleware: identify the user from a
	// request header and put it into the server-side request context
	mux.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if u := r.Header.Get("X-Test-User"); u != "" {
				r = r.WithContext(revactx.ContextSetUser(r.Context(), &userv1beta1.User{
					Id: &userv1beta1.UserId{OpaqueId: u},
				}))
			}
			next.ServeHTTP(w, r)
		})
	})

	bus := make(chan events.Event, 16)
	history := newFakeHistory()

	_, err := service.NewSSE(&config.Config{KeepAliveInterval: keepAlive}, log.Logger{}, bus, mux, history)
	if err != nil {
		t.Fatalf("could not create sse service: %v", err)
	}

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(bus) })

	return &testEnv{t: t, server: srv, bus: bus, history: history}
}

// emit publishes a SendSSE on the bus, returns the stable event id
func (e *testEnv) emit(send events.SendSSE) string {
	id := uuid.New().String()
	e.bus <- events.Event{ID: id, Type: "events.SendSSE", Event: send}
	return id
}

// record stores an event in the fake history, exactly as eventhistory would
func (e *testEnv) record(id string, send events.SendSSE) {
	raw, err := json.Marshal(send)
	if err != nil {
		e.t.Fatalf("marshal: %v", err)
	}
	e.history.add(&ehmsg.Event{Type: "events.SendSSE", Id: id, Event: raw})
}

// emitAndRecord stores first (as if the event had already been persisted)
// and then publishes it on the bus
func (e *testEnv) emitAndRecord(send events.SendSSE) string {
	id := e.emit(send)
	e.record(id, send)
	return id
}

type clientConn struct {
	cancel context.CancelFunc
	frames chan frame
	resp   *http.Response
}

func (e *testEnv) connect(user string, cursor string) *clientConn {
	e.t.Helper()

	ctx, cancel := context.WithCancel(context.Background())

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.server.URL+ssePath, nil)
	if err != nil {
		cancel()
		e.t.Fatalf("request: %v", err)
	}
	req.Header.Set("X-Test-User", user)
	if cursor != "" {
		req.Header.Set("Last-Event-ID", cursor)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		cancel()
		e.t.Fatalf("connect as %s: %v", user, err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		cancel()
		e.t.Fatalf("expected 200, got %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	c := &clientConn{cancel: cancel, frames: make(chan frame, 32), resp: resp}
	go func() {
		defer close(c.frames)
		readFrames(e.t, resp.Body, c.frames)
	}()

	return c
}

func (e *testEnv) connectExpectStatus(user, cursor string) int {
	e.t.Helper()

	req, err := http.NewRequest(http.MethodGet, e.server.URL+ssePath, nil)
	if err != nil {
		e.t.Fatalf("request: %v", err)
	}
	req.Header.Set("X-Test-User", user)
	if cursor != "" {
		req.Header.Set("Last-Event-ID", cursor)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatalf("connect: %v", err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func (c *clientConn) close() {
	c.cancel()
	_ = c.resp.Body.Close()
	for range c.frames {
	}
}

func (c *clientConn) next(timeout time.Duration) (frame, bool) {
	select {
	case f, ok := <-c.frames:
		return f, ok
	case <-time.After(timeout):
		return frame{}, false
	}
}

// frame is a decoded SSE dispatch as seen by the client
type frame struct {
	id      string
	event   string
	data    string
	comment string
}

func readFrames(t *testing.T, r io.Reader, out chan<- frame) {
	t.Helper()

	br := bufio.NewReader(r)
	for {
		var block bytes.Buffer
		for {
			line, err := br.ReadBytes('\n')
			if err != nil {
				return
			}
			if strings.TrimRight(string(line), "\r\n") == "" {
				break
			}
			block.Write(line)
		}
		out <- decodeFrame(block.Bytes())
	}
}

func decodeFrame(block []byte) frame {
	f := frame{}
	for _, line := range strings.Split(strings.TrimSpace(string(block)), "\n") {
		line = strings.TrimSuffix(line, "\r")
		switch {
		case strings.HasPrefix(line, "id:"):
			f.id = strings.TrimPrefix(strings.TrimPrefix(line, "id:"), " ")
		case strings.HasPrefix(line, "event:"):
			f.event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			if f.data != "" {
				f.data += "\n"
			}
			f.data += strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
		case strings.HasPrefix(line, ":"):
			f.comment = strings.TrimPrefix(strings.TrimPrefix(line, ":"), " ")
		}
	}
	return f
}

// ---------------------------------------------------------------------------
// fake eventhistory service
// ---------------------------------------------------------------------------

type fakeHistory struct {
	mu     sync.Mutex
	events []*ehmsg.Event
	err    error
}

func newFakeHistory() *fakeHistory {
	return &fakeHistory{}
}

func (f *fakeHistory) add(ev *ehmsg.Event) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, ev)
}

func (f *fakeHistory) setError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

func (f *fakeHistory) GetEvents(_ context.Context, _ *ehsvc.GetEventsRequest, _ ...microclient.CallOption) (*ehsvc.GetEventsResponse, error) {
	return &ehsvc.GetEventsResponse{}, nil
}

func (f *fakeHistory) GetEventsForUser(_ context.Context, req *ehsvc.GetEventsForUserRequest, _ ...microclient.CallOption) (*ehsvc.GetEventsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.err != nil {
		return nil, f.err
	}

	// emulate the eventhistory regex match over raw payloads
	rx := regexp.MustCompile(fmt.Sprintf(`\W%s\W`, regexp.QuoteMeta(req.UserID)))

	var matched []*ehmsg.Event
	for _, ev := range f.events {
		if rx.Match(ev.Event) {
			matched = append(matched, ev)
		}
	}

	// the real store listing has no defined order
	rand.Shuffle(len(matched), func(i, j int) {
		matched[i], matched[j] = matched[j], matched[i]
	})

	return &ehsvc.GetEventsResponse{Events: matched}, nil
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func notification(users []string, data string) events.SendSSE {
	return events.SendSSE{
		UserIDs: users,
		Type:    "userlog-notification",
		Message: []byte(data),
	}
}

// timedNotification builds a notification message carrying an ascending
// business timestamp, as the real userlog-converted payloads do
func timedNotification(users []string, n int) events.SendSSE {
	return notification(users, fmt.Sprintf(`{"n":%d,"datetime":"2026-01-01T00:00:%02dZ"}`, n, n))
}

// no cursor: live-only semantics
func TestLiveOnlyWithoutCursor(t *testing.T) {
	env := newTestEnv(t, 0)

	c := env.connect("alice", "")
	defer c.close()

	id := env.emitAndRecord(notification([]string{"alice"}, `{"msg":"hello"}`))

	f, ok := c.next(2 * time.Second)
	if !ok {
		t.Fatal("did not receive live event")
	}
	if f.id != id {
		t.Errorf("event id: want %s got %q", id, f.id)
	}
	if f.event != "userlog-notification" {
		t.Errorf("event type: %q", f.event)
	}
	if f.data != `{"msg":"hello"}` {
		t.Errorf("data: %q", f.data)
	}
	if f.comment != "" {
		t.Errorf("unexpected comment: %q", f.comment)
	}
}

// reconnect with cursor: missed events replayed once, then live continues
func TestResumeFromCursor(t *testing.T) {
	env := newTestEnv(t, 0)

	// initial connection, receive two events
	c1 := env.connect("alice", "")
	id1 := env.emitAndRecord(timedNotification([]string{"alice"}, 1))
	id2 := env.emitAndRecord(timedNotification([]string{"alice"}, 2))
	for _, want := range []string{id1, id2} {
		f, ok := c1.next(2 * time.Second)
		if !ok || f.id != want {
			t.Fatalf("initial stream: want %s got %v (%v)", want, f, ok)
		}
	}
	c1.close()

	// disconnected: event 3 is persisted but no live delivery
	id3 := uuid.New().String()
	env.record(id3, timedNotification([]string{"alice"}, 3))
	env.bus <- events.Event{ID: id3, Type: "events.SendSSE", Event: timedNotification([]string{"alice"}, 3)}

	// resume from id2
	c2 := env.connect("alice", id2)
	defer c2.close()

	f, ok := c2.next(2 * time.Second)
	if !ok {
		t.Fatal("missing replay of event 3")
	}
	if f.id != id3 {
		t.Fatalf("replay id: want %s got %q", id3, f.id)
	}

	// event 4: persisted and published after resume -> exactly once (dedupe handoff)
	id4 := uuid.New().String()
	env.record(id4, timedNotification([]string{"alice"}, 4))
	env.bus <- events.Event{ID: id4, Type: "events.SendSSE", Event: timedNotification([]string{"alice"}, 4)}

	// event 5: published but not yet persisted -> still arrives, no gap
	id5 := uuid.New().String()
	env.bus <- events.Event{ID: id5, Type: "events.SendSSE", Event: timedNotification([]string{"alice"}, 5)}

	got := map[string]int{}
	for i := 0; i < 2; i++ {
		f, ok := c2.next(2 * time.Second)
		if !ok {
			t.Fatalf("missing live handoff events, got %v", got)
		}
		got[f.id]++
	}
	if got[id4] != 1 || got[id5] != 1 {
		t.Fatalf("handoff must deliver each event exactly once: %v", got)
	}

	// nothing more should arrive: no duplicates of already delivered events
	if f, ok := c2.next(500 * time.Millisecond); ok {
		t.Fatalf("unexpected duplicate frame: %+v", f)
	}
}

// replay order is deterministic: for payloads without a business timestamp,
// the event id defines the order, regardless of store listing order
func TestReplayDeterministicOrder(t *testing.T) {
	env := newTestEnv(t, 0)

	ids := make([]string, 10)
	for i := range ids {
		ids[i] = uuid.New().String()
		env.record(ids[i], notification([]string{"alice"}, fmt.Sprintf(`{"i":%d}`, i)))
	}
	sort.Strings(ids)

	cursor := ids[2] // already saw up to index 2
	c := env.connect("alice", cursor)
	defer c.close()

	for i := 3; i < len(ids); i++ {
		f, ok := c.next(2 * time.Second)
		if !ok {
			t.Fatalf("missing replay at position %d", i)
		}
		if f.id != ids[i] {
			t.Fatalf("replay order at %d: want %s got %q", i, ids[i], f.id)
		}
	}
}

// replay must follow business time, not the lexicographic order of the random
// event ids; otherwise already seen events with larger uuids would be
// re-delivered and unseen ones skipped
func TestReplayFollowsTimeNotUUIDOrder(t *testing.T) {
	env := newTestEnv(t, 0)

	rawIDs := make([]string, 4)
	for i := range rawIDs {
		rawIDs[i] = uuid.New().String()
	}
	sort.Strings(rawIDs)

	// map chronological positions onto uuids in REVERSE order:
	// event 1 -> largest uuid, event 4 -> smallest uuid
	chronological := []string{rawIDs[3], rawIDs[2], rawIDs[1], rawIDs[0]}
	for i, id := range chronological {
		payload := fmt.Sprintf(`{"i":%d,"datetime":"2026-03-01T00:00:0%dZ"}`, i+1, i+1)
		env.record(id, notification([]string{"alice"}, payload))
	}

	cursor := chronological[1] // client last saw event 2 (second-largest uuid)
	c := env.connect("alice", cursor)
	defer c.close()

	// expect event 3 then event 4 in time order, i.e. uuids rawIDs[1], rawIDs[0]
	for i, want := range chronological[2:] {
		f, ok := c.next(2 * time.Second)
		if !ok {
			t.Fatalf("missing time-ordered replay at %d", i)
		}
		if f.id != want {
			t.Fatalf("replay at %d must be chronological: want %s got %q", i, want, f.id)
		}
	}

	// event 1 (already seen, larger uuid) must not reappear
	if f, ok := c.next(500 * time.Millisecond); ok {
		t.Fatalf("already-seen event re-delivered: %+v", f)
	}
}

// malformed cursor: resync signal without id, then fresh live boundary
func TestMalformedCursorResync(t *testing.T) {
	env := newTestEnv(t, 0)

	// some history that must NOT be replayed after a malformed cursor
	old := uuid.New().String()
	env.record(old, notification([]string{"alice"}, `{"old":true}`))

	c := env.connect("alice", "definitely-not-a-uuid")
	defer c.close()

	f, ok := c.next(2 * time.Second)
	if !ok {
		t.Fatal("expected resync event")
	}
	if f.event != "resync" {
		t.Fatalf("want resync event, got %q", f.event)
	}
	if f.id != "" {
		t.Errorf("resync must not advance the cursor, but got id %q", f.id)
	}
	if !strings.Contains(f.data, "cursor_malformed") {
		t.Errorf("resync reason: %q", f.data)
	}

	// live events after the boundary arrive
	id := env.emitAndRecord(notification([]string{"alice"}, `{"fresh":true}`))
	if f, ok := c.next(2 * time.Second); !ok || f.id != id {
		t.Fatalf("live event after resync: %v (%v)", f, ok)
	}
}

// unknown (or expired) cursor: resync, not a faked lossless resume
func TestUnknownCursorResync(t *testing.T) {
	env := newTestEnv(t, 0)

	old := uuid.New().String()
	env.record(old, notification([]string{"alice"}, `{"old":true}`))

	unknown := uuid.New().String()
	c := env.connect("alice", unknown)
	defer c.close()

	f, ok := c.next(2 * time.Second)
	if !ok {
		t.Fatal("expected resync event")
	}
	if f.event != "resync" || !strings.Contains(f.data, "cursor_unknown") {
		t.Fatalf("want cursor_unknown resync, got %+v", f)
	}
	if f.id != "" {
		t.Errorf("resync must not advance the cursor, got %q", f.id)
	}
	if f, ok := c.next(500 * time.Millisecond); ok {
		t.Fatalf("old history must not be replayed, got %+v", f)
	}
}

// users are isolated: per-user events, cursors, authorization
func TestUserIsolation(t *testing.T) {
	env := newTestEnv(t, 0)

	aliceOnly := uuid.New().String()
	env.record(aliceOnly, notification([]string{"alice"}, `{"who":"alice","datetime":"2026-02-01T00:00:01Z"}`))

	// the message references bob, so the history regex would match it for bob...
	aliceMsgBobRef := uuid.New().String()
	env.record(aliceMsgBobRef, notification([]string{"alice"}, `{"x":"-bob-","datetime":"2026-02-01T00:00:02Z"}`))

	both := uuid.New().String()
	env.record(both, notification([]string{"alice", "bob"}, `{"who":"both","datetime":"2026-02-01T00:00:03Z"}`))

	bobOnly := uuid.New().String()
	env.record(bobOnly, notification([]string{"bob"}, `{"who":"bob","datetime":"2026-02-01T00:00:04Z"}`))

	// bob reconnects using alice's cursor: cursor is unknown to bob -> resync
	cBob := env.connect("bob", aliceOnly)
	defer cBob.close()
	f, _ := cBob.next(2 * time.Second)
	if f.event != "resync" {
		t.Fatalf("bob with alice's cursor must resync, got %+v", f)
	}

	// bob's fresh boundary must not leak alice-only events, even when the raw
	// payload matched the regex
	if f, ok := cBob.next(500 * time.Millisecond); ok {
		t.Fatalf("no event expected right after resync, got %+v", f)
	}

	// bob live event for bob works
	id := env.emitAndRecord(notification([]string{"bob"}, `{"live":true}`))
	if f, ok := cBob.next(2 * time.Second); !ok || f.id != id {
		t.Fatalf("bob live: %+v (%v)", f, ok)
	}

	// alice resumes from aliceOnly: she gets her own events (including the one
	// whose payload merely references bob), in chronological order, but never
	// bob-only; the regex over-match must not cross the authorization boundary
	cAlice := env.connect("alice", aliceOnly)
	defer cAlice.close()

	replayIDs := map[string]int{}
	for i := 0; i < 2; i++ {
		fa, ok := cAlice.next(2 * time.Second)
		if !ok {
			t.Fatalf("missing alice replay at %d: %v", i, replayIDs)
		}
		replayIDs[fa.id]++
	}
	if replayIDs[aliceMsgBobRef] != 1 || replayIDs[both] != 1 {
		t.Fatalf("alice replay must be [bob-ref event, shared event]: %v", replayIDs)
	}
	if f, ok := cAlice.next(500 * time.Millisecond); ok {
		t.Fatalf("bob-only event leaked into alice stream: %+v", f)
	}

	// same event delivered to both users live: both receive, same id
	liveBoth := env.emitAndRecord(notification([]string{"alice", "bob"}, `{"live":"both"}`))

	cb := make(chan string, 2)
	go func() {
		f, ok := cBob.next(2 * time.Second)
		if ok {
			cb <- f.id
		}
	}()
	go func() {
		f, ok := cAlice.next(2 * time.Second)
		if ok {
			cb <- f.id
		}
	}()
	recipients := map[string]struct{}{}
	for i := 0; i < 2; i++ {
		select {
		case id := <-cb:
			recipients[id] = struct{}{}
		case <-time.After(2 * time.Second):
			t.Fatal("missing live delivery to one of the users")
		}
	}
	if len(recipients) != 1 {
		t.Fatalf("same event id expected on both streams, got %v", recipients)
	}
	if _, ok := recipients[liveBoth]; !ok {
		t.Fatalf("delivered id mismatch: %v", recipients)
	}
}

// eventhistory unavailable: 503, no frames, cursor untouched; retry after
// recovery replays the same order
func TestHistoryUnavailable(t *testing.T) {
	env := newTestEnv(t, 0)

	var ids []string
	for i := 0; i < 5; i++ {
		id := uuid.New().String()
		env.record(id, notification([]string{"alice"}, fmt.Sprintf(`{"i":%d}`, i)))
		ids = append(ids, id)
	}
	sort.Strings(ids)
	cursor := ids[0]

	boom := fmt.Errorf("eventhistory down")
	env.history.setError(boom)

	if status := env.connectExpectStatus("alice", cursor); status != http.StatusServiceUnavailable {
		t.Fatalf("want 503 while history unavailable, got %d", status)
	}

	// recover and retry with the same cursor
	env.history.setError(nil)

	c := env.connect("alice", cursor)
	defer c.close()

	for i := 1; i < len(ids); i++ {
		f, ok := c.next(2 * time.Second)
		if !ok || f.id != ids[i] {
			t.Fatalf("retry replay at %d: want %s got %+v (%v)", i, ids[i], f, ok)
		}
	}
}

// concurrent publishes to the same user: nothing lost or duplicated
func TestConcurrentPublishesSameUser(t *testing.T) {
	env := newTestEnv(t, 0)

	// pre-disconnect burst
	const n = 20
	ids := make([]string, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := uuid.New().String()
			ids[i] = id
			env.record(id, notification([]string{"alice"}, fmt.Sprintf(`{"i":%d}`, i)))
			env.bus <- events.Event{ID: id, Type: "events.SendSSE", Event: notification([]string{"alice"}, fmt.Sprintf(`{"i":%d}`, i))}
		}(i)
	}
	wg.Wait()

	sorted := append([]string(nil), ids...)
	sort.Strings(sorted)

	c := env.connect("alice", sorted[2])
	defer c.close()

	got := map[string]int{}
	for i := 3; i < n; i++ {
		f, ok := c.next(3 * time.Second)
		if !ok {
			t.Fatalf("missing concurrent replay at %d, got %d events", i, len(got))
		}
		got[f.id]++
	}
	for i := 3; i < n; i++ {
		if got[sorted[i]] != 1 {
			t.Fatalf("event %s delivered %d times", sorted[i], got[sorted[i]])
		}
	}
}

// keepalive comments don't carry an id and don't disturb the notification flow
func TestKeepaliveComments(t *testing.T) {
	env := newTestEnv(t, 60*time.Millisecond)

	c := env.connect("alice", "")
	defer c.close()

	sawKeepalive := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		f, ok := c.next(2 * time.Second)
		if !ok {
			break
		}
		if f.comment == "keepalive" {
			sawKeepalive = true
			if f.id != "" {
				t.Fatalf("keepalive comment must not carry an id, got %q", f.id)
			}
			continue
		}
	}
	if !sawKeepalive {
		t.Fatal("did not receive any keepalive comments")
	}

	id := env.emitAndRecord(notification([]string{"alice"}, `{"msg":"after-keepalive"}`))
	f, ok := c.next(2 * time.Second)
	if !ok || f.id != id {
		t.Fatalf("business event after keepalive: %+v (%v)", f, ok)
	}
}

// no user in context: authentication failure remains an error
func TestAuthFailure(t *testing.T) {
	mux := chi.NewMux()
	bus := make(chan events.Event, 1)
	_, err := service.NewSSE(&config.Config{}, log.Logger{}, bus, mux, newFakeHistory())
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + ssePath)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("want 500 without user context, got %d", resp.StatusCode)
	}
}
