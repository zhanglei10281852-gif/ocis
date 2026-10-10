package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sync"
	"time"

	gateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/go-chi/chi/v5"
	"github.com/owncloud/reva/v2/pkg/autoprop"
	"github.com/owncloud/reva/v2/pkg/events"
	"github.com/owncloud/reva/v2/pkg/rgrpc/todo/pool"
	"github.com/owncloud/reva/v2/pkg/storagespace"
	"github.com/owncloud/reva/v2/pkg/utils"
	microstore "go-micro.dev/v4/store"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	"github.com/owncloud/ocis/v2/ocis-pkg/tracing"
	ehmsg "github.com/owncloud/ocis/v2/protogen/gen/ocis/messages/eventhistory/v0"
	ehsvc "github.com/owncloud/ocis/v2/protogen/gen/ocis/services/eventhistory/v0"
	settingssvc "github.com/owncloud/ocis/v2/protogen/gen/ocis/services/settings/v0"
	"github.com/owncloud/ocis/v2/services/activitylog/pkg/config"
)

// Nats runs into max payload exceeded errors at around 7k activities. Let's keep a buffer.
var _maxActivities = 6000

// defaultEventHistoryTTL matches the default TTL of the eventhistory store.
// It is only used when no explicit TTL is configured.
const defaultEventHistoryTTL = 336 * time.Hour

// eventArrivalGrace is added to the eventhistory TTL when deciding whether a
// missing event body might still be in flight. It covers clock skew between
// the activitylog and eventhistory services as well as event delivery and
// processing delays.
const eventArrivalGrace = time.Minute

// convergenceTimeout bounds the eventhistory re-check performed while
// converging references.
const convergenceTimeout = 10 * time.Second

// RawActivity represents an activity as it is stored in the activitylog store
type RawActivity struct {
	EventID   string    `json:"event_id"`
	Depth     int       `json:"depth"`
	Timestamp time.Time `json:"timestamp"`
}

// eventBodyLookup is the activitylog view of the eventhistory lookup outcome
// for a single referenced event.
type eventBodyLookup int

const (
	// lookupUnknown means eventhistory did not report on the id, e.g. an
	// older eventhistory version. The reference is treated as potentially
	// valid and is never removed.
	lookupUnknown eventBodyLookup = iota
	// lookupPresent means the event body was returned.
	lookupPresent
	// lookupNotFound means no body is stored at the moment; it may still be
	// in flight or may have expired.
	lookupNotFound
	// lookupCorrupt means a body is stored but can not be decoded and is
	// therefore permanently lost.
	lookupCorrupt
	// lookupReadFailed means a transient storage backend error prevented the
	// body from being read. The event must be considered potentially valid.
	lookupReadFailed
)

// convergenceDecision states what should happen with an activity reference.
type convergenceDecision int

const (
	// convergeKeep retains the reference for future reads.
	convergeKeep convergenceDecision = iota
	// convergeDrop removes the reference: it is confirmed dead.
	convergeDrop
)

// ActivitylogService logs events per resource
type ActivitylogService struct {
	cfg        *config.Config
	log        log.Logger
	events     <-chan events.Event
	store      microstore.Store
	gws        pool.Selectable[gateway.GatewayAPIClient]
	mux        *chi.Mux
	evHistory  ehsvc.EventHistoryService
	valService settingssvc.ValueService
	lock       sync.RWMutex

	// eventHistoryTTL is the retention duration of the eventhistory store.
	// A reference whose event body is missing and whose timestamp is older
	// than TTL + grace is considered expired.
	eventHistoryTTL time.Duration

	registeredEvents map[string]events.Unmarshaller
}

// New creates a new ActivitylogService
func New(opts ...Option) (*ActivitylogService, error) {
	o := &Options{}
	for _, opt := range opts {
		opt(o)
	}

	if o.Stream == nil {
		return nil, errors.New("stream is required")
	}

	if o.Store == nil {
		return nil, errors.New("store is required")
	}

	ch, err := events.Consume(o.Stream, o.Config.Service.Name, o.RegisteredEvents...)
	if err != nil {
		return nil, err
	}

	ttl := defaultEventHistoryTTL
	if o.Config != nil && o.Config.EventHistoryTTL > 0 {
		ttl = o.Config.EventHistoryTTL
	}

	s := &ActivitylogService{
		log:              o.Logger,
		cfg:              o.Config,
		events:           ch,
		store:            o.Store,
		gws:              o.GatewaySelector,
		mux:              o.Mux,
		evHistory:        o.HistoryClient,
		valService:       o.ValueClient,
		lock:             sync.RWMutex{},
		eventHistoryTTL:  ttl,
		registeredEvents: make(map[string]events.Unmarshaller),
	}

	s.mux.Get("/graph/v1beta1/extensions/org.libregraph/activities", s.HandleGetItemActivities)

	for _, e := range o.RegisteredEvents {
		typ := reflect.TypeOf(e)
		s.registeredEvents[typ.String()] = e
	}

	go s.Run()

	return s, nil
}

// Run runs the service
func (a *ActivitylogService) Run() {
	for e := range a.events {
		var err error

		// trace provider is available here, otherwise the activitylog service should have crashed
		tp, _ := tracing.GetServiceTraceProvider(a.cfg.Tracing, a.cfg.Service.Name)
		evCtx := context.Background()
		ctx, span := events.TraceEventConsumer(evCtx, tp, e)
		ctx = autoprop.SetMetaToContext(ctx, e.ExtraInfo)
		// span is closed at the end of the loop

		switch ev := e.Event.(type) {
		case events.UploadReady:
			err = a.AddActivity(ctx, ev.FileRef, e.ID, utils.TSToTime(ev.Timestamp))
		case events.FileTouched:
			err = a.AddActivity(ctx, ev.Ref, e.ID, utils.TSToTime(ev.Timestamp))
		// Disabled https://github.com/owncloud/ocis/issues/10293
		//case events.FileDownloaded:
		// we are only interested in public link downloads - so no need to store others.
		//if ev.ImpersonatingUser.GetDisplayName() == "Public" {
		//	err = a.AddActivity(ctx, ev.Ref, e.ID, utils.TSToTime(ev.Timestamp))
		//}
		case events.ContainerCreated:
			err = a.AddActivity(ctx, ev.Ref, e.ID, utils.TSToTime(ev.Timestamp))
		case events.ItemTrashed:
			err = a.AddActivityTrashed(ctx, ev.ID, ev.Ref, e.ID, utils.TSToTime(ev.Timestamp))
		case events.ItemPurged:
			err = a.RemoveResource(ev.ID) // no ctx needed at the moment
		case events.ItemMoved:
			err = a.AddActivity(ctx, ev.Ref, e.ID, utils.TSToTime(ev.Timestamp))
		case events.ShareCreated:
			err = a.AddActivity(ctx, toRef(ev.ItemID), e.ID, utils.TSToTime(ev.CTime))
		case events.ShareUpdated:
			if ev.Sharer != nil && ev.ItemID != nil && ev.Sharer.GetOpaqueId() != ev.ItemID.GetSpaceId() {
				err = a.AddActivity(ctx, toRef(ev.ItemID), e.ID, utils.TSToTime(ev.MTime))
			}
		case events.ShareRemoved:
			err = a.AddActivity(ctx, toRef(ev.ItemID), e.ID, ev.Timestamp)
		case events.LinkCreated:
			err = a.AddActivity(ctx, toRef(ev.ItemID), e.ID, utils.TSToTime(ev.CTime))
		case events.LinkUpdated:
			if ev.Sharer != nil && ev.ItemID != nil && ev.Sharer.GetOpaqueId() != ev.ItemID.GetSpaceId() {
				err = a.AddActivity(ctx, toRef(ev.ItemID), e.ID, utils.TSToTime(ev.MTime))
			}
		case events.LinkRemoved:
			err = a.AddActivity(ctx, toRef(ev.ItemID), e.ID, utils.TSToTime(ev.Timestamp))
		case events.SpaceShared:
			err = a.AddSpaceActivity(ev.ID, e.ID, ev.Timestamp) // no ctx needed at the moment
		case events.SpaceUnshared:
			err = a.AddSpaceActivity(ev.ID, e.ID, ev.Timestamp) // no ctx needed at the moment
		}

		if err != nil {
			a.log.Error().Err(err).Interface("event", e).Msg("could not process event")
		}

		span.End()
	}
}

// AddActivity adds the activity to the given resource and all its parents
func (a *ActivitylogService) AddActivity(ctx context.Context, initRef *provider.Reference, eventID string, timestamp time.Time) error {
	gwc, err := a.gws.Next()
	if err != nil {
		return fmt.Errorf("cant get gateway client: %w", err)
	}

	ctx2, err := utils.GetServiceUserContextWithContext(ctx, gwc, a.cfg.ServiceAccount.ServiceAccountID, a.cfg.ServiceAccount.ServiceAccountSecret)
	if err != nil {
		return fmt.Errorf("cant get service user context: %w", err)
	}

	return a.addActivity(initRef, eventID, timestamp, func(ref *provider.Reference) (*provider.ResourceInfo, error) {
		return utils.GetResource(ctx2, ref, gwc)
	})
}

// AddActivityTrashed adds the activity to given trashed resource and all its former parents
func (a *ActivitylogService) AddActivityTrashed(ctx context.Context, resourceID *provider.ResourceId, reference *provider.Reference, eventID string, timestamp time.Time) error {
	gwc, err := a.gws.Next()
	if err != nil {
		return fmt.Errorf("cant get gateway client: %w", err)
	}

	ctx2, err := utils.GetServiceUserContextWithContext(ctx, gwc, a.cfg.ServiceAccount.ServiceAccountID, a.cfg.ServiceAccount.ServiceAccountSecret)
	if err != nil {
		return fmt.Errorf("cant get service user context: %w", err)
	}

	// store activity on trashed item
	if err := a.storeActivity(storagespace.FormatResourceID(resourceID), eventID, 0, timestamp); err != nil {
		return fmt.Errorf("could not store activity: %w", err)
	}

	// get previous parent
	ref := &provider.Reference{
		ResourceId: reference.GetResourceId(),
		Path:       filepath.Dir(reference.GetPath()),
	}

	return a.addActivity(ref, eventID, timestamp, func(ref *provider.Reference) (*provider.ResourceInfo, error) {
		return utils.GetResource(ctx2, ref, gwc)
	})
}

// AddSpaceActivity adds the activity to the given spaceroot
func (a *ActivitylogService) AddSpaceActivity(spaceID *provider.StorageSpaceId, eventID string, timestamp time.Time) error {
	// spaceID is in format <providerid>$<spaceid>
	// activitylog service uses format <providerid>$<spaceid>!<resourceid>
	// lets do some converting, shall we?
	rid, err := storagespace.ParseID(spaceID.GetOpaqueId())
	if err != nil {
		return fmt.Errorf("could not parse space id: %w", err)
	}
	rid.OpaqueId = rid.GetSpaceId()
	return a.storeActivity(storagespace.FormatResourceID(&rid), eventID, 0, timestamp)

}

// Activities returns the activities for the given resource
func (a *ActivitylogService) Activities(rid *provider.ResourceId) ([]RawActivity, error) {
	a.lock.RLock()
	defer a.lock.RUnlock()

	return a.activities(rid)
}

// convergeReferences removes references that a read classified as dead, but
// only if both conditions still hold at deletion time:
//
//   - the same generation of the reference is still stored. The reference
//     timestamp acts as its generation: if it changed or disappeared in the
//     meantime, another state owns the reference and this stale read must not
//     touch it.
//   - eventhistory still confirms the event body as dead. A body arriving
//     right after the read's cleanup decision makes the reference valid
//     again, so it must be kept.
//
// No destructive write happens when the activitylog store cannot be read,
// when eventhistory is unavailable or when individual bodies can only be
// read with transient errors.
func (a *ActivitylogService) convergeReferences(rid *provider.ResourceId, dead map[string]time.Time) {
	if len(dead) == 0 {
		return
	}

	a.lock.Lock()
	defer a.lock.Unlock()

	cur, err := a.activities(rid)
	if err != nil {
		a.log.Error().Err(err).Msg("could not read activities while converging references")
		return
	}

	// restrict the re-check to references that still exist with the exact
	// same generation as during the read
	candidates := make(map[string]RawActivity, len(dead))
	ids := make([]string, 0, len(dead))
	for _, ra := range cur {
		gen, ok := dead[ra.EventID]
		if !ok || !ra.Timestamp.Equal(gen) {
			continue
		}
		if _, queued := candidates[ra.EventID]; !queued {
			candidates[ra.EventID] = ra
			ids = append(ids, ra.EventID)
		}
	}
	if len(ids) == 0 {
		return
	}

	// final arbitration against eventhistory: the body may have arrived
	// after the read classified the reference as dead
	ctx, cancel := context.WithTimeout(context.Background(), convergenceTimeout)
	defer cancel()
	res, err := a.evHistory.GetEvents(ctx, &ehsvc.GetEventsRequest{Ids: ids})
	if err != nil {
		a.log.Error().Err(err).Msg("could not re-check events while converging references")
		return
	}

	found := make(map[string]*ehmsg.Event, len(res.GetEvents()))
	for _, e := range res.GetEvents() {
		found[e.GetId()] = e
	}
	states := make(map[string]ehsvc.EventLookupState, len(res.GetStatuses()))
	for _, st := range res.GetStatuses() {
		states[st.GetId()] = st.GetState()
	}

	now := time.Now()
	toDelete := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if e, ok := found[id]; ok {
			// the body exists. It is only safe to drop if it is still
			// undecodable for this service; a body that can now be turned
			// into an activity (e.g. after an upgrade) keeps its reference.
			if a.unwrapEvent(e) == nil {
				toDelete[id] = struct{}{}
			}
			continue
		}
		if a.decideConvergence(candidates[id], eventBodyLookupFromStatus(states[id]), now) == convergeDrop {
			toDelete[id] = struct{}{}
		}
	}
	if len(toDelete) == 0 {
		return
	}

	acts := make([]RawActivity, 0, len(cur))
	for _, ra := range cur {
		if _, drop := toDelete[ra.EventID]; drop {
			continue
		}
		acts = append(acts, ra)
	}

	b, err := json.Marshal(acts)
	if err != nil {
		a.log.Error().Err(err).Msg("could not marshal activities while converging references")
		return
	}

	if err := a.store.Write(&microstore.Record{
		Key:   storagespace.FormatResourceID(rid),
		Value: b,
	}); err != nil {
		a.log.Error().Err(err).Msg("could not store activities while converging references")
	}
}

// decideConvergence determines whether an activity reference is safe to drop.
// Only bodies confirmed permanently lost (corrupt) or missing beyond the
// eventhistory retention period lead to convergeDrop. Everything else, in
// particular transient read failures, keeps the reference so a later read can
// fill in the activity once the body becomes visible.
func (a *ActivitylogService) decideConvergence(ra RawActivity, lookup eventBodyLookup, now time.Time) convergenceDecision {
	switch lookup {
	case lookupCorrupt:
		return convergeDrop
	case lookupNotFound:
		if a.isBeyondEventHistoryRetention(ra.Timestamp, now) {
			return convergeDrop
		}
		return convergeKeep
	default:
		// present bodies, transient read failures and unknown states always
		// keep the reference
		return convergeKeep
	}
}

// isBeyondEventHistoryRetention reports whether an event from the given
// timestamp must have expired in the eventhistory store, including the grace
// period for clock skew and event delivery delay.
func (a *ActivitylogService) isBeyondEventHistoryRetention(eventTime, now time.Time) bool {
	ttl := a.eventHistoryTTL
	if ttl <= 0 {
		ttl = defaultEventHistoryTTL
	}
	return !eventTime.Add(ttl).Add(eventArrivalGrace).After(now)
}

// eventBodyLookupFromStatus translates an eventhistory lookup status into the
// activitylog view. Missing status information is treated as unknown so that
// references are never deleted based on assumptions.
func eventBodyLookupFromStatus(state ehsvc.EventLookupState) eventBodyLookup {
	switch state {
	case ehsvc.EventLookupState_EVENT_LOOKUP_STATE_NOT_FOUND:
		return lookupNotFound
	case ehsvc.EventLookupState_EVENT_LOOKUP_STATE_CORRUPT:
		return lookupCorrupt
	case ehsvc.EventLookupState_EVENT_LOOKUP_STATE_READ_FAILED:
		return lookupReadFailed
	default:
		return lookupUnknown
	}
}

// RemoveResource removes the resource from the store
func (a *ActivitylogService) RemoveResource(rid *provider.ResourceId) error {
	if rid == nil {
		return fmt.Errorf("resource id is required")
	}

	a.lock.Lock()
	defer a.lock.Unlock()

	return a.store.Delete(storagespace.FormatResourceID(rid))
}

func (a *ActivitylogService) activities(rid *provider.ResourceId) ([]RawActivity, error) {
	resourceID := storagespace.FormatResourceID(rid)

	records, err := a.store.Read(resourceID)
	if err != nil && err != microstore.ErrNotFound {
		return nil, fmt.Errorf("could not read activities: %w", err)
	}

	if len(records) == 0 {
		return []RawActivity{}, nil
	}

	var activities []RawActivity
	if err := json.Unmarshal(records[0].Value, &activities); err != nil {
		return nil, fmt.Errorf("could not unmarshal activities: %w", err)
	}

	return activities, nil
}

// note: getResource is abstracted to allow unit testing, in general this will just be utils.GetResource
func (a *ActivitylogService) addActivity(initRef *provider.Reference, eventID string, timestamp time.Time, getResource func(*provider.Reference) (*provider.ResourceInfo, error)) error {
	var (
		info  *provider.ResourceInfo
		err   error
		depth int
		ref   = initRef
	)
	for {
		info, err = getResource(ref)
		if err != nil {
			return fmt.Errorf("could not get resource info: %w", err)
		}

		if err := a.storeActivity(storagespace.FormatResourceID(info.GetId()), eventID, depth, timestamp); err != nil {
			return fmt.Errorf("could not store activity: %w", err)
		}

		if info != nil && utils.IsSpaceRoot(info) {
			return nil
		}

		depth++
		ref = &provider.Reference{ResourceId: info.GetParentId()}
	}
}

func (a *ActivitylogService) storeActivity(resourceID string, eventID string, depth int, timestamp time.Time) error {
	a.lock.Lock()
	defer a.lock.Unlock()

	records, err := a.store.Read(resourceID)
	if err != nil && err != microstore.ErrNotFound {
		return err
	}

	var activities []RawActivity
	if len(records) > 0 {
		if err := json.Unmarshal(records[0].Value, &activities); err != nil {
			return err
		}
	}

	if l := len(activities); l >= _maxActivities {
		activities = activities[l-_maxActivities+1:]
	}

	activities = append(activities, RawActivity{
		EventID:   eventID,
		Depth:     depth,
		Timestamp: timestamp,
	})

	b, err := json.Marshal(activities)
	if err != nil {
		return err
	}

	return a.store.Write(&microstore.Record{
		Key:   resourceID,
		Value: b,
	})
}

func toRef(r *provider.ResourceId) *provider.Reference {
	return &provider.Reference{
		ResourceId: r,
	}
}

func toSpace(r *provider.Reference) *provider.StorageSpaceId {
	return &provider.StorageSpaceId{
		OpaqueId: storagespace.FormatStorageID(r.GetResourceId().GetStorageId(), r.GetResourceId().GetSpaceId()),
	}
}
