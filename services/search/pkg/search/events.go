package search

import (
	"context"
	"time"

	provider "github.com/cs3org/go-cs3apis/cs3/storage/provider/v1beta1"
	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	"github.com/owncloud/ocis/v2/ocis-pkg/tracing"
	"github.com/owncloud/ocis/v2/services/search/pkg/config"
	"github.com/owncloud/reva/v2/pkg/autoprop"
	"github.com/owncloud/reva/v2/pkg/events"
	"github.com/owncloud/reva/v2/pkg/storagespace"
)

// spaceIndexerProvider is implemented by searchers that own a persistent
// SpaceDebouncer. It lets the event consumer reuse the same debouncer (and
// therefore pending generations and locks) as manual IndexSpace calls.
type spaceIndexerProvider interface {
	spaceIndexer() *SpaceDebouncer
}

// HandleEvents listens to the needed events,
// it handles the whole resource indexing livecycle.
func HandleEvents(s Searcher, bus events.Consumer, logger log.Logger, cfg *config.Config) error {
	evts := []events.Unmarshaller{
		events.ItemTrashed{},
		events.ItemRestored{},
		events.ItemMoved{},
		events.ContainerCreated{},
		events.FileTouched{},
		events.FileVersionRestored{},
		events.TagsAdded{},
		events.TagsRemoved{},
		events.SpaceRenamed{},
	}

	if cfg.Events.AsyncUploads {
		evts = append(evts, events.UploadReady{})
	} else {
		evts = append(evts, events.FileUploaded{})
	}

	ch, err := events.Consume(bus, "search", evts...)
	if err != nil {
		return err
	}

	if cfg.Events.NumConsumers == 0 {
		cfg.Events.NumConsumers = 1
	}

	getSpaceID := func(ref *provider.Reference) *provider.StorageSpaceId {
		return &provider.StorageSpaceId{
			OpaqueId: storagespace.FormatResourceID(
				&provider.ResourceId{
					StorageId: ref.GetResourceId().GetStorageId(),
					SpaceId:   ref.GetResourceId().GetSpaceId(),
				},
			),
		}
	}

	// Reuse the search service's persistent debouncer when available so that
	// event-triggered and manual rebuilds share pending generations, per-space
	// serialization and crash recovery. Searchers without a persistent data
	// path (e.g. in tests) fall back to the in-memory debouncer.
	var indexSpaceDebouncer *SpaceDebouncer
	if idxProvider, ok := s.(spaceIndexerProvider); ok {
		indexSpaceDebouncer = idxProvider.spaceIndexer()
	}
	if indexSpaceDebouncer == nil {
		indexSpaceDebouncer = NewSpaceDebouncer(time.Duration(cfg.Events.DebounceDuration)*time.Millisecond, func(ctx context.Context, id *provider.StorageSpaceId) {
			if err := s.IndexSpace(ctx, id); err != nil {
				logger.Error().Err(err).Interface("spaceID", id).Msg("error while indexing a space")
			}
		})
	} else {
		// Recover spaces that were still pending when a previous instance
		// stopped. Only persisted pending spaces are scheduled, an idle
		// startup never scans all spaces.
		indexSpaceDebouncer.Start()
	}

	// trace provider is available here, otherwise the search service should have crashed
	tp, _ := tracing.GetServiceTraceProvider(cfg.Tracing, cfg.Service.Name)

	for i := 0; i < cfg.Events.NumConsumers; i++ {
		go func(s Searcher, ch <-chan events.Event) {
			for event := range ch {
				e := event
				go func() {
					evCtx := context.Background()
					tracedEvCtx, span := events.TraceEventConsumer(evCtx, tp, e)
					tracedEvCtx = autoprop.SetMetaToContext(tracedEvCtx, e.ExtraInfo)
					defer span.End()

					logger.Debug().Interface("event", e).Msg("updating index")

					switch ev := e.Event.(type) {
					case events.ItemTrashed:
						s.TrashItem(tracedEvCtx, ev.ID)
						indexSpaceDebouncer.Debounce(tracedEvCtx, getSpaceID(ev.Ref))
					case events.ItemMoved:
						s.MoveItem(tracedEvCtx, ev.Ref)
						indexSpaceDebouncer.Debounce(tracedEvCtx, getSpaceID(ev.Ref))
					case events.ItemRestored:
						s.RestoreItem(tracedEvCtx, ev.Ref)
						indexSpaceDebouncer.Debounce(tracedEvCtx, getSpaceID(ev.Ref))
					case events.ContainerCreated:
						indexSpaceDebouncer.Debounce(tracedEvCtx, getSpaceID(ev.Ref))
					case events.FileTouched:
						indexSpaceDebouncer.Debounce(tracedEvCtx, getSpaceID(ev.Ref))
					case events.FileVersionRestored:
						indexSpaceDebouncer.Debounce(tracedEvCtx, getSpaceID(ev.Ref))
					case events.TagsAdded:
						s.UpdateTags(tracedEvCtx, ev.Ref)
					case events.TagsRemoved:
						s.UpdateTags(tracedEvCtx, ev.Ref)
					case events.FileUploaded:
						indexSpaceDebouncer.Debounce(tracedEvCtx, getSpaceID(ev.Ref))
					case events.UploadReady:
						if ev.Failed {
							return
						}
						indexSpaceDebouncer.Debounce(tracedEvCtx, getSpaceID(ev.FileRef))
					case events.SpaceRenamed:
						indexSpaceDebouncer.Debounce(tracedEvCtx, ev.ID)
					}
				}()
			}
		}(
			s,
			ch,
		)
	}

	return nil
}
