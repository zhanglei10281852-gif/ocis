package http

import (
	"context"

	"github.com/owncloud/ocis/v2/ocis-pkg/log"
	ehsvc "github.com/owncloud/ocis/v2/protogen/gen/ocis/services/eventhistory/v0"
	"github.com/owncloud/ocis/v2/services/sse/pkg/config"
	"github.com/owncloud/reva/v2/pkg/events"
	"go.opentelemetry.io/otel/trace"
)

// Option defines a single option function.
type Option func(o *Options)

// Options defines the available options for this package.
type Options struct {
	Logger           log.Logger
	Context          context.Context
	Config           *config.Config
	Consumer         events.Consumer
	RegisteredEvents []events.Unmarshaller
	HistoryClient    ehsvc.EventHistoryService
	TracerProvider   trace.TracerProvider
}

// newOptions initializes the available default options.
func newOptions(opts ...Option) Options {
	opt := Options{}

	for _, o := range opts {
		o(&opt)
	}

	return opt
}

// Logger provides a function to set the logger option.
func Logger(val log.Logger) Option {
	return func(o *Options) {
		o.Logger = val
	}
}

// Context provides a function to set the context option.
func Context(val context.Context) Option {
	return func(o *Options) {
		o.Context = val
	}
}

// Config provides a function to set the config option.
func Config(val *config.Config) Option {
	return func(o *Options) {
		o.Config = val
	}
}

// Consumer provides a function to configure the consumer
func Consumer(consumer events.Consumer) Option {
	return func(o *Options) {
		o.Consumer = consumer
	}
}

// RegisteredEvents provides a function to register events
func RegisteredEvents(evs []events.Unmarshaller) Option {
	return func(o *Options) {
		o.RegisteredEvents = evs
	}
}

// TracerProvider provides a function to set the TracerProvider option.
func TracerProvider(val trace.TracerProvider) Option {
	return func(o *Options) {
		o.TracerProvider = val
	}
}

// HistoryClient provides a function to set the eventhistory client option.
func HistoryClient(val ehsvc.EventHistoryService) Option {
	return func(o *Options) {
		o.HistoryClient = val
	}
}
