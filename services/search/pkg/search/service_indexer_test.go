package search_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	gateway "github.com/cs3org/go-cs3apis/cs3/gateway/v1beta1"
	userv1beta1 "github.com/cs3org/go-cs3apis/cs3/identity/user/v1beta1"
	sprovider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	typesv1beta1 "github.com/cs3org/go-cs3apis/cs3/types/v1beta1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/owncloud/reva/v2/pkg/rgrpc/status"
	"github.com/owncloud/reva/v2/pkg/rgrpc/todo/pool"
	cs3mocks "github.com/owncloud/reva/v2/tests/cs3mocks/mocks"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	"github.com/owncloud/ocis/v2/services/search/pkg/config"
	"github.com/owncloud/ocis/v2/services/search/pkg/content"
	contentMocks "github.com/owncloud/ocis/v2/services/search/pkg/content/mocks"
	"github.com/owncloud/ocis/v2/services/search/pkg/engine"
	engineMocks "github.com/owncloud/ocis/v2/services/search/pkg/engine/mocks"
	"github.com/owncloud/ocis/v2/services/search/pkg/search"
)

var _ = Describe("Service space indexer", func() {
	var (
		datapath        string
		extractor       *contentMocks.Extractor
		gatewayClient   *cs3mocks.GatewayAPIClient
		gatewaySelector pool.Selectable[gateway.GatewayAPIClient]
		indexClient     *engineMocks.Engine
		logger          = log.NewLogger()
		user            = &userv1beta1.User{
			Id: &userv1beta1.UserId{
				OpaqueId: "user",
			},
		}
		ri = &sprovider.ResourceInfo{
			Id: &sprovider.ResourceId{
				StorageId: "storageid",
				SpaceId:   "spaceid",
				OpaqueId:  "opaqueid",
			},
			ParentId: &sprovider.ResourceId{
				StorageId: "storageid",
				SpaceId:   "spaceid",
				OpaqueId:  "parentopaqueid",
			},
			Path:  "./foo.pdf",
			Size:  12345,
			Mtime: &typesv1beta1.Timestamp{Seconds: 4000},
		}
	)

	BeforeEach(func() {
		var err error
		datapath, err = os.MkdirTemp("", "search-service-")
		Expect(err).ToNot(HaveOccurred())

		pool.RemoveSelector("GatewaySelector" + "com.owncloud.api.gateway")
		gatewayClient = &cs3mocks.GatewayAPIClient{}
		gatewaySelector = pool.GetSelector[gateway.GatewayAPIClient](
			"GatewaySelector",
			"com.owncloud.api.gateway",
			func(cc grpc.ClientConnInterface) gateway.GatewayAPIClient {
				return gatewayClient
			},
		)

		indexClient = &engineMocks.Engine{}
		extractor = &contentMocks.Extractor{}

		gatewayClient.On("Authenticate", mock.Anything, mock.Anything).Return(&gateway.AuthenticateResponse{
			Status: status.NewOK(context.Background()),
			Token:  "authtoken",
		}, nil)
	})

	AfterEach(func() {
		_ = os.RemoveAll(datapath)
	})

	It("does not scan any space on idle startup when nothing is pending", func() {
		cfg := &config.Config{
			Engine: config.Engine{Bleve: config.EngineBleve{Datapath: datapath}},
		}
		s := search.NewService(gatewaySelector, indexClient, extractor, logger, cfg)

		s.StartSpaceIndexer()
		Expect(s.ShutdownSpaceIndexer()).To(Succeed())

		gatewayClient.AssertNotCalled(GinkgoT(), "Stat", mock.Anything, mock.Anything)
		indexClient.AssertNotCalled(GinkgoT(), "Upsert", mock.Anything, mock.Anything)
	})

	It("recovers pending spaces on startup and clears them through a manual IndexSpace", func() {
		// Simulate a previous instance that accepted a change and was stopped
		// inside the debounce window.
		seeder, err := search.NewPersistentSpaceDebouncer(time.Hour, func(_ context.Context, _ *sprovider.StorageSpaceId) error {
			return nil
		}, datapath, logger)
		Expect(err).ToNot(HaveOccurred())
		seeder.Debounce(context.Background(), &sprovider.StorageSpaceId{OpaqueId: "storageid$spaceid!spaceid"})
		Expect(seeder.Close(context.Background())).To(Succeed())

		cfg := &config.Config{
			Engine: config.Engine{Bleve: config.EngineBleve{Datapath: datapath}},
			Events: config.Events{DebounceDuration: 60 * 60 * 1000}, // 1h, automatic runs must not interfere
		}
		s := search.NewService(gatewaySelector, indexClient, extractor, logger, cfg)
		s.StartSpaceIndexer() // loads the pending space without scanning it

		var upserts atomic.Int32
		gatewayClient.On("GetUserByClaim", mock.Anything, mock.Anything).Return(&userv1beta1.GetUserByClaimResponse{
			Status: status.NewOK(context.Background()),
			User:   user,
		}, nil)
		extractor.On("Extract", mock.Anything, mock.Anything, mock.Anything).Return(content.Document{}, nil)
		indexClient.On("Upsert", mock.Anything, mock.Anything).Run(func(_ mock.Arguments) {
			upserts.Add(1)
		}).Return(nil)
		indexClient.On("Lookup", mock.Anything).Return(nil, engine.ErrResourceNotFound)
		indexClient.On("DocCount").Return(uint64(1), nil)
		gatewayClient.On("Stat", mock.Anything, mock.MatchedBy(func(req *sprovider.StatRequest) bool {
			return req.Ref.ResourceId.StorageId == "storageid" &&
				req.Ref.ResourceId.OpaqueId == "spaceid" &&
				req.Ref.ResourceId.SpaceId == "spaceid"
		})).Return(&sprovider.StatResponse{
			Status: status.NewOK(context.Background()),
			Info:   ri,
		}, nil)

		// The manual rebuild covers the pending generation.
		Expect(s.IndexSpace(context.Background(), &sprovider.StorageSpaceId{OpaqueId: "storageid$spaceid!spaceid"})).
			To(Succeed())
		Expect(int(upserts.Load())).To(Equal(1))

		raw, err := os.ReadFile(filepath.Join(datapath, "pending_spaces.json"))
		Expect(err).ToNot(HaveOccurred())
		var data struct {
			Spaces map[string]json.RawMessage `json:"spaces"`
		}
		Expect(json.Unmarshal(raw, &data)).To(Succeed())
		Expect(data.Spaces).To(BeEmpty())

		// No automatic follow-up run is scheduled after the pending generation
		// was covered.
		Consistently(func() int { return int(upserts.Load()) }, "200ms", "20ms").Should(Equal(1))
		Expect(s.ShutdownSpaceIndexer()).To(Succeed())
	})
})
