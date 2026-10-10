package service

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/owncloud/reva/v2/pkg/events"
	"github.com/owncloud/reva/v2/pkg/storagespace"
	"github.com/owncloud/reva/v2/pkg/store"
	"github.com/stretchr/testify/require"
	"go-micro.dev/v4/client"
	microstore "go-micro.dev/v4/store"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	ehmsg "github.com/owncloud/ocis/v2/protogen/gen/ocis/messages/eventhistory/v0"
	ehsvc "github.com/owncloud/ocis/v2/protogen/gen/ocis/services/eventhistory/v0"
)

var errHistoryUnavailable = errors.New("eventhistory unavailable")

// fakeHistoryClient is a test double for the eventhistory gRPC client.
type fakeHistoryClient struct {
	mu        sync.Mutex
	requests  [][]string
	responder func(ids []string) (*ehsvc.GetEventsResponse, error)
}

func (f *fakeHistoryClient) GetEvents(_ context.Context, in *ehsvc.GetEventsRequest, _ ...client.CallOption) (*ehsvc.GetEventsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	ids := append([]string(nil), in.GetIds()...)
	f.requests = append(f.requests, ids)
	if f.responder == nil {
		return &ehsvc.GetEventsResponse{}, nil
	}
	return f.responder(ids)
}

func (f *fakeHistoryClient) GetEventsForUser(_ context.Context, _ *ehsvc.GetEventsForUserRequest, _ ...client.CallOption) (*ehsvc.GetEventsResponse, error) {
	return &ehsvc.GetEventsResponse{}, nil
}

func (f *fakeHistoryClient) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

func newConvergenceService(t *testing.T, ttl time.Duration, hc ehsvc.EventHistoryService, registerEvents bool) *ActivitylogService {
	t.Helper()
	registered := map[string]events.Unmarshaller{}
	if registerEvents {
		for _, e := range []events.Unmarshaller{events.UploadReady{}} {
			registered[reflect.TypeOf(e).String()] = e
		}
	}
	return &ActivitylogService{
		log:              log.Logger{},
		store:            store.Create(),
		evHistory:        hc,
		eventHistoryTTL:  ttl,
		registeredEvents: registered,
	}
}

func seedReferences(t *testing.T, a *ActivitylogService, rid *provider.ResourceId, refs []RawActivity) {
	t.Helper()
	b, err := json.Marshal(refs)
	require.NoError(t, err)
	require.NoError(t, a.store.Write(&microstore.Record{
		Key:   storagespace.FormatResourceID(rid),
		Value: b,
	}))
}

func readReferences(t *testing.T, a *ActivitylogService, rid *provider.ResourceId) []RawActivity {
	t.Helper()
	refs, err := a.Activities(rid)
	require.NoError(t, err)
	return refs
}

func notFoundStatus(ids ...string) *ehsvc.GetEventsResponse {
	resp := &ehsvc.GetEventsResponse{}
	for _, id := range ids {
		resp.Statuses = append(resp.Statuses, &ehsvc.EventLookupStatus{
			Id:    id,
			State: ehsvc.EventLookupState_EVENT_LOOKUP_STATE_NOT_FOUND,
		})
	}
	return resp
}

func statusResponse(state ehsvc.EventLookupState, ids ...string) *ehsvc.GetEventsResponse {
	resp := &ehsvc.GetEventsResponse{}
	for _, id := range ids {
		resp.Statuses = append(resp.Statuses, &ehsvc.EventLookupStatus{Id: id, State: state})
	}
	return resp
}

func decodableEventMsg(id string) *ehmsg.Event {
	b, err := json.Marshal(events.UploadReady{})
	if err != nil {
		panic(err)
	}
	return &ehmsg.Event{
		Id:    id,
		Type:  reflect.TypeOf(events.UploadReady{}).String(),
		Event: b,
	}
}

func TestDecideConvergence(t *testing.T) {
	ttl := 2 * time.Hour
	now := time.Now()
	young := now.Add(-30 * time.Minute)
	old := now.Add(-3 * time.Hour)

	a := newConvergenceService(t, ttl, &fakeHistoryClient{}, true)

	for _, tc := range []struct {
		name   string
		ts     time.Time
		lookup eventBodyLookup
		want   convergenceDecision
	}{
		{"present body always keeps", young, lookupPresent, convergeKeep},
		{"present old body keeps", old, lookupPresent, convergeKeep},
		{"unknown status keeps", old, lookupUnknown, convergeKeep},
		{"transient read failure keeps even old", old, lookupReadFailed, convergeKeep},
		{"missing within retention is in flight", young, lookupNotFound, convergeKeep},
		{"missing beyond retention is expired", old, lookupNotFound, convergeDrop},
		{"corrupt young body drops", young, lookupCorrupt, convergeDrop},
		{"corrupt old body drops", old, lookupCorrupt, convergeDrop},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, a.decideConvergence(RawActivity{Timestamp: tc.ts}, tc.lookup, now))
		})
	}

	// at exactly TTL + grace the reference is considered expired
	boundary := RawActivity{Timestamp: now.Add(-(ttl + eventArrivalGrace))}
	require.Equal(t, convergeDrop, a.decideConvergence(boundary, lookupNotFound, now))
}

func TestEventBodyLookupFromStatus(t *testing.T) {
	require.Equal(t, lookupNotFound, eventBodyLookupFromStatus(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_NOT_FOUND))
	require.Equal(t, lookupCorrupt, eventBodyLookupFromStatus(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_CORRUPT))
	require.Equal(t, lookupReadFailed, eventBodyLookupFromStatus(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_READ_FAILED))
	require.Equal(t, lookupUnknown, eventBodyLookupFromStatus(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_UNSPECIFIED))
}

func TestConvergeReferencesExpiredVsInFlight(t *testing.T) {
	rid := resourceID("base")
	now := time.Now()
	expired1 := RawActivity{EventID: "expired-1", Timestamp: now.Round(0).Add(-3 * time.Hour)}
	expired2 := RawActivity{EventID: "expired-2", Timestamp: now.Round(0).Add(-4 * time.Hour)}
	inflight := RawActivity{EventID: "in-flight", Timestamp: now.Round(0).Add(-1 * time.Minute)}
	unrelated := RawActivity{EventID: "unrelated", Timestamp: now.Round(0).Add(-5 * time.Hour)}

	hc := &fakeHistoryClient{
		responder: func(ids []string) (*ehsvc.GetEventsResponse, error) {
			// converge re-arbitrates every candidate, even ones the read
			// already classified as dead
			return notFoundStatus(ids...), nil
		},
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	seedReferences(t, a, rid, []RawActivity{expired1, expired2, inflight, unrelated})

	// the read would only submit expired references as dead; submitting the
	// in-flight one as well proves converge does not trust stale decisions
	a.convergeReferences(rid, map[string]time.Time{
		expired1.EventID: expired1.Timestamp,
		expired2.EventID: expired2.Timestamp,
		inflight.EventID: inflight.Timestamp,
	})

	got := readReferences(t, a, rid)
	var ids []string
	for _, ra := range got {
		ids = append(ids, ra.EventID)
	}
	require.ElementsMatch(t, []string{"in-flight", "unrelated"}, ids)
}

func TestConvergeReferencesTransientReadFailureKeeps(t *testing.T) {
	rid := resourceID("base")
	old := RawActivity{EventID: "old", Timestamp: time.Now().Round(0).Add(-5 * time.Hour)}

	hc := &fakeHistoryClient{
		responder: func(ids []string) (*ehsvc.GetEventsResponse, error) {
			return statusResponse(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_READ_FAILED, ids...), nil
		},
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	seedReferences(t, a, rid, []RawActivity{old})

	a.convergeReferences(rid, map[string]time.Time{old.EventID: old.Timestamp})

	require.ElementsMatch(t, []RawActivity{old}, readReferences(t, a, rid))
}

func TestConvergeReferencesCorruptDrops(t *testing.T) {
	rid := resourceID("base")
	young := RawActivity{EventID: "young-corrupt", Timestamp: time.Now().Round(0).Add(-1 * time.Minute)}

	hc := &fakeHistoryClient{
		responder: func(ids []string) (*ehsvc.GetEventsResponse, error) {
			return statusResponse(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_CORRUPT, ids...), nil
		},
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	seedReferences(t, a, rid, []RawActivity{young})

	a.convergeReferences(rid, map[string]time.Time{young.EventID: young.Timestamp})

	require.Empty(t, readReferences(t, a, rid))
}

func TestConvergeReferencesEventHistoryUnavailableKeeps(t *testing.T) {
	rid := resourceID("base")
	old := RawActivity{EventID: "old", Timestamp: time.Now().Round(0).Add(-5 * time.Hour)}

	hc := &fakeHistoryClient{
		responder: func(_ []string) (*ehsvc.GetEventsResponse, error) {
			return nil, errHistoryUnavailable
		},
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	seedReferences(t, a, rid, []RawActivity{old})

	a.convergeReferences(rid, map[string]time.Time{old.EventID: old.Timestamp})

	require.ElementsMatch(t, []RawActivity{old}, readReferences(t, a, rid))
}

func TestConvergeReferencesGenerationMismatchKeeps(t *testing.T) {
	rid := resourceID("base")
	current := RawActivity{EventID: "e", Timestamp: time.Now().Round(0).Add(-5 * time.Hour)}

	hc := &fakeHistoryClient{
		responder: func(ids []string) (*ehsvc.GetEventsResponse, error) {
			return notFoundStatus(ids...), nil
		},
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	seedReferences(t, a, rid, []RawActivity{current})

	// a stale read recorded a different generation of the same reference
	a.convergeReferences(rid, map[string]time.Time{"e": current.Timestamp.Add(-time.Hour)})

	require.ElementsMatch(t, []RawActivity{current}, readReferences(t, a, rid))
	require.Equal(t, 0, hc.callCount(), "no re-check must happen for generation mismatches")
}

func TestConvergeReferencesBodyArrivedKeeps(t *testing.T) {
	rid := resourceID("base")
	old := RawActivity{EventID: "late-body", Timestamp: time.Now().Round(0).Add(-5 * time.Hour)}

	// the body arrives right after the read classified the reference as dead
	hc := &fakeHistoryClient{
		responder: func(ids []string) (*ehsvc.GetEventsResponse, error) {
			return &ehsvc.GetEventsResponse{Events: []*ehmsg.Event{decodableEventMsg(ids[0])}}, nil
		},
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	seedReferences(t, a, rid, []RawActivity{old})

	a.convergeReferences(rid, map[string]time.Time{old.EventID: old.Timestamp})

	require.ElementsMatch(t, []RawActivity{old}, readReferences(t, a, rid))
}

func TestConvergeReferencesStillUndecodableDrops(t *testing.T) {
	rid := resourceID("base")
	old := RawActivity{EventID: "undecodable", Timestamp: time.Now().Round(0).Add(-5 * time.Hour)}

	hc := &fakeHistoryClient{
		responder: func(ids []string) (*ehsvc.GetEventsResponse, error) {
			// eventhistory has a body, but the activitylog can not decode it
			b, _ := json.Marshal(struct{ UnknownField int }{UnknownField: 1})
			return &ehsvc.GetEventsResponse{Events: []*ehmsg.Event{{
				Id:    ids[0],
				Type:  "some.unregistered.Type",
				Event: b,
			}}}, nil
		},
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	seedReferences(t, a, rid, []RawActivity{old})

	a.convergeReferences(rid, map[string]time.Time{old.EventID: old.Timestamp})

	require.Empty(t, readReferences(t, a, rid))
}

func TestConvergeReferencesEmptyAndMissingDoNotCall(t *testing.T) {
	rid := resourceID("base")
	hc := &fakeHistoryClient{}
	a := newConvergenceService(t, time.Hour, hc, true)

	a.convergeReferences(rid, nil)
	a.convergeReferences(rid, map[string]time.Time{"gone": time.Now()})

	require.Equal(t, 0, hc.callCount())
}

func TestConvergeReferencesIsConcurrencySafe(t *testing.T) {
	rid := resourceID("base")
	ts := time.Now().Round(0).Add(-5 * time.Hour)
	refs := []RawActivity{
		{EventID: "e1", Timestamp: ts},
		{EventID: "e2", Timestamp: ts},
		{EventID: "keep", Timestamp: time.Now().Round(0)},
	}

	hc := &fakeHistoryClient{
		responder: func(ids []string) (*ehsvc.GetEventsResponse, error) {
			return notFoundStatus(ids...), nil
		},
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	seedReferences(t, a, rid, refs)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			a.convergeReferences(rid, map[string]time.Time{"e1": ts, "e2": ts})
		}()
	}
	wg.Wait()

	// the remaining list must be intact and readable (no torn writes)
	require.ElementsMatch(t, []RawActivity{{EventID: "keep", Timestamp: refs[2].Timestamp}}, readReferences(t, a, rid))
}

// TestConvergeReferencesPersistentState simulates both services restarting:
// conclusions derive solely from the persisted reference timestamp and the
// configured TTL, so freshly constructed services sharing the store reach the
// same decision.
func TestConvergeReferencesPersistentState(t *testing.T) {
	rid := resourceID("base")
	young := RawActivity{EventID: "young", Timestamp: time.Now().Round(0).Add(-1 * time.Minute)}
	old := RawActivity{EventID: "old", Timestamp: time.Now().Round(0).Add(-5 * time.Hour)}
	sto := store.Create()
	seedReferences(t, &ActivitylogService{store: sto}, rid, []RawActivity{young, old})

	notFoundHC := func() *fakeHistoryClient {
		return &fakeHistoryClient{responder: func(ids []string) (*ehsvc.GetEventsResponse, error) {
			return notFoundStatus(ids...), nil
		}}
	}

	// "restarted" service instance, young reference is kept
	a1 := &ActivitylogService{log: log.Logger{}, store: sto, evHistory: notFoundHC(), eventHistoryTTL: time.Hour, registeredEvents: map[string]events.Unmarshaller{}}
	a1.convergeReferences(rid, map[string]time.Time{"young": young.Timestamp})

	// another "restart", same store and TTL, old reference is removed
	a2 := &ActivitylogService{log: log.Logger{}, store: sto, evHistory: notFoundHC(), eventHistoryTTL: time.Hour, registeredEvents: map[string]events.Unmarshaller{}}
	a2.convergeReferences(rid, map[string]time.Time{"old": old.Timestamp})

	got := readReferences(t, &ActivitylogService{store: sto}, rid)
	var ids []string
	for _, ra := range got {
		ids = append(ids, ra.EventID)
	}
	require.ElementsMatch(t, []string{"young"}, ids)
}

// TestMissingBodyArrivesLaterFillsIn covers the core scenario: a reference
// exists before its eventhistory body; reads within the retention window keep
// it, and once the body arrives the activity can be composed on a later read.
func TestMissingBodyArrivesLaterFillsIn(t *testing.T) {
	rid := resourceID("base")
	ra := RawActivity{EventID: "late", Timestamp: time.Now().Round(0)}
	sto := store.Create()
	seedReferences(t, &ActivitylogService{store: sto}, rid, []RawActivity{ra})

	hc := &fakeHistoryClient{}
	var calls int
	hc.responder = func(ids []string) (*ehsvc.GetEventsResponse, error) {
		calls++
		// first "read": body still in flight
		if calls == 1 {
			return notFoundStatus(ids...), nil
		}
		// later "read": body has been written
		return &ehsvc.GetEventsResponse{Events: []*ehmsg.Event{decodableEventMsg(ids[0])}}, nil
	}
	a := newConvergenceService(t, time.Hour, hc, true)
	a.store = sto

	// first read: not found but within retention, so nothing is dead
	res, err := hc.GetEvents(context.Background(), &ehsvc.GetEventsRequest{Ids: []string{ra.EventID}})
	require.NoError(t, err)
	states := map[string]ehsvc.EventLookupState{}
	for _, st := range res.GetStatuses() {
		states[st.GetId()] = st.GetState()
	}
	require.Len(t, res.GetEvents(), 0)
	require.Equal(t, convergeKeep, a.decideConvergence(ra, eventBodyLookupFromStatus(states[ra.EventID]), time.Now()))

	// reference is still there after the first read
	require.ElementsMatch(t, []RawActivity{ra}, readReferences(t, a, rid))

	// second read: the body arrives and is returned
	res2, err := hc.GetEvents(context.Background(), &ehsvc.GetEventsRequest{Ids: []string{ra.EventID}})
	require.NoError(t, err)
	require.Len(t, res2.GetEvents(), 1)
	require.Equal(t, ra.EventID, res2.GetEvents()[0].GetId())
	// and the reference was never deleted
	require.ElementsMatch(t, []RawActivity{ra}, readReferences(t, a, rid))
}
