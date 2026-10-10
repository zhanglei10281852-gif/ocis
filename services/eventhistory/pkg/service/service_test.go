package service_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sort"
	"time"

	userv1beta1 "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	ehsvc "github.com/owncloud/ocis/v2/protogen/gen/ocis/services/eventhistory/v0"
	"github.com/owncloud/ocis/v2/services/eventhistory/pkg/config"
	"github.com/owncloud/ocis/v2/services/eventhistory/pkg/service"
	"github.com/owncloud/reva/v2/pkg/events"
	"github.com/owncloud/reva/v2/pkg/store"
	"github.com/owncloud/reva/v2/pkg/utils"
	microevents "go-micro.dev/v4/events"
	microstore "go-micro.dev/v4/store"
)

var _ = Describe("EventHistoryService", func() {
	var (
		cfg = &config.Config{}

		eh  *service.EventHistoryService
		bus testBus
		sto microstore.Store
	)

	BeforeEach(func() {
		var err error
		sto = store.Create()
		bus = testBus(make(chan events.Event))
		eh, err = service.NewEventHistoryService(cfg, bus, sto, log.Logger{})
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		close(bus)
	})

	It("Records events, stores them and allows them to be retrieved", func() {
		id := bus.Publish(events.UploadReady{})

		// service will store eventually
		time.Sleep(500 * time.Millisecond)

		resp := &ehsvc.GetEventsResponse{}
		err := eh.GetEvents(context.Background(), &ehsvc.GetEventsRequest{Ids: []string{id}}, resp)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp).ToNot(BeNil())

		Expect(len(resp.Events)).To(Equal(1))
		Expect(resp.Events[0].Id).To(Equal(id))
	})

	It("Reports NOT_FOUND status for ids that are not stored", func() {
		resp := &ehsvc.GetEventsResponse{}
		err := eh.GetEvents(context.Background(), &ehsvc.GetEventsRequest{Ids: []string{"missing", "missing", "also-missing"}}, resp)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Events).To(BeEmpty())

		// duplicate ids are only reported once
		Expect(len(resp.Statuses)).To(Equal(2))
		Expect(resp.Statuses[0].Id).To(Equal("missing"))
		Expect(resp.Statuses[0].State).To(Equal(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_NOT_FOUND))
		Expect(resp.Statuses[1].Id).To(Equal("also-missing"))
		Expect(resp.Statuses[1].State).To(Equal(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_NOT_FOUND))
	})

	It("Reports READ_FAILED status when the store can not be read", func() {
		failing := &failingReadStore{Store: sto, fail: map[string]bool{"unreadable": true}}
		ehFail, err := service.NewEventHistoryService(cfg, bus, failing, log.Logger{})
		Expect(err).ToNot(HaveOccurred())

		resp := &ehsvc.GetEventsResponse{}
		err = ehFail.GetEvents(context.Background(), &ehsvc.GetEventsRequest{Ids: []string{"unreadable"}}, resp)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Events).To(BeEmpty())
		Expect(len(resp.Statuses)).To(Equal(1))
		Expect(resp.Statuses[0].Id).To(Equal("unreadable"))
		Expect(resp.Statuses[0].State).To(Equal(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_READ_FAILED))
	})

	It("Reports CORRUPT status and removes undecodable records", func() {
		Expect(sto.Write(&microstore.Record{
			Key:   "corrupt",
			Value: []byte("this is not json"),
		})).To(Succeed())

		resp := &ehsvc.GetEventsResponse{}
		err := eh.GetEvents(context.Background(), &ehsvc.GetEventsRequest{Ids: []string{"corrupt"}}, resp)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp.Events).To(BeEmpty())
		Expect(len(resp.Statuses)).To(Equal(1))
		Expect(resp.Statuses[0].Id).To(Equal("corrupt"))
		Expect(resp.Statuses[0].State).To(Equal(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_CORRUPT))

		// the corrupt record has been removed, later reads report NOT_FOUND
		// instead of failing on the same record again
		resp2 := &ehsvc.GetEventsResponse{}
		err = eh.GetEvents(context.Background(), &ehsvc.GetEventsRequest{Ids: []string{"corrupt"}}, resp2)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp2.Events).To(BeEmpty())
		Expect(len(resp2.Statuses)).To(Equal(1))
		Expect(resp2.Statuses[0].State).To(Equal(ehsvc.EventLookupState_EVENT_LOOKUP_STATE_NOT_FOUND))
	})

	It("Gets all events", func() {
		ids := make([]string, 3)
		ids[0] = bus.Publish(events.UploadReady{
			ExecutingUser: &userv1beta1.User{
				Id: &userv1beta1.UserId{
					OpaqueId: "test-id",
				},
			},
			Failed:    false,
			Timestamp: utils.TimeToTS(time.Time{}),
		})
		ids[1] = bus.Publish(events.UserCreated{
			UserID: "another-id",
		})
		ids[2] = bus.Publish(events.UserDeleted{
			Executant: &userv1beta1.UserId{
				OpaqueId: "another-id",
			},
			UserID: "test-id",
		})

		time.Sleep(500 * time.Millisecond)

		resp := &ehsvc.GetEventsResponse{}
		err := eh.GetEventsForUser(context.Background(), &ehsvc.GetEventsForUserRequest{UserID: "test-id"}, resp)
		Expect(err).ToNot(HaveOccurred())
		Expect(resp).ToNot(BeNil())

		// Events don't always come back in the same order as they were sent, so we need to sort them and
		// do the same for the expected IDs as well.
		expectedIDs := []string{ids[0], ids[2]}
		sort.Strings(expectedIDs)
		var gotIDs []string
		for _, ev := range resp.Events {
			gotIDs = append(gotIDs, ev.Id)
		}
		sort.Strings(gotIDs)

		Expect(len(gotIDs)).To(Equal(len(expectedIDs)))
		Expect(gotIDs[0]).To(Equal(expectedIDs[0]))
		Expect(gotIDs[1]).To(Equal(expectedIDs[1]))
	})
})

type testBus chan events.Event

// failingReadStore wraps a store and forces read errors for the configured keys
type failingReadStore struct {
	microstore.Store
	fail map[string]bool
}

func (s *failingReadStore) Read(key string, opts ...microstore.ReadOption) ([]*microstore.Record, error) {
	if s.fail[key] {
		return nil, errors.New("simulated transient store failure")
	}
	return s.Store.Read(key, opts...)
}

func (tb testBus) Consume(_ string, _ ...microevents.ConsumeOption) (<-chan microevents.Event, error) {
	ch := make(chan microevents.Event)
	go func() {
		for ev := range tb {
			b, _ := json.Marshal(ev.Event)
			ch <- microevents.Event{
				Payload: b,
				Metadata: map[string]string{
					events.MetadatakeyEventID:   ev.ID,
					events.MetadatakeyEventType: ev.Type,
				},
			}
		}
	}()
	return ch, nil
}

func (tb testBus) Publish(e interface{}) string {
	ev := events.Event{
		ID:    uuid.New().String(),
		Type:  reflect.TypeOf(e).String(),
		Event: e,
	}

	tb <- ev
	return ev.ID
}
