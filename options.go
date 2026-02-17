package kgo

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/client"
)

var (

	// DefaultCommitInterval specifies how fast send commit offsets to kafka
	DefaultCommitInterval = 5 * time.Second

	// DefaultStatsInterval specifies how fast check consumer lag
	DefaultStatsInterval = 30 * time.Second

	// DefaultSubscribeMaxInflight specifies how much messages keep inflight
	DefaultSubscribeMaxInflight = 100
)

type subscribeContextKey struct{}

// SubscribeContext set the context for broker.SubscribeOption
func SubscribeContext(ctx context.Context) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeContextKey{}, ctx)
}

type publishKey struct{}

// PublishKey set the kafka message key (broker option)
func PublishKey(key []byte) broker.PublishOption {
	return broker.SetPublishOption(publishKey{}, key)
}

// ClientPublishKey set the kafka message key (client option)
func ClientPublishKey(key []byte) client.PublishOption {
	return client.SetPublishOption(publishKey{}, key)
}

type optionsKey struct{}

// Options pass additional options to broker
func Options(opts ...kgo.Opt) broker.Option {
	return func(o *broker.Options) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		options, ok := o.Context.Value(optionsKey{}).([]kgo.Opt)
		if !ok {
			options = make([]kgo.Opt, 0, len(opts))
		}
		options = append(options, opts...)
		o.Context = context.WithValue(o.Context, optionsKey{}, options)
	}
}

// SubscribeOptions pass additional options to broker in Subscribe
func SubscribeOptions(opts ...kgo.Opt) broker.SubscribeOption {
	return func(o *broker.SubscribeOptions) {
		if o.Context == nil {
			o.Context = context.Background()
		}
		options, ok := o.Context.Value(optionsKey{}).([]kgo.Opt)
		if !ok {
			options = make([]kgo.Opt, 0, len(opts))
		}
		options = append(options, opts...)
		o.Context = context.WithValue(o.Context, optionsKey{}, options)
	}
}

type fatalOnErrorKey struct{}

func FatalOnError(b bool) broker.Option {
	return broker.SetOption(fatalOnErrorKey{}, b)
}

type clientIDKey struct{}

func ClientID(id string) broker.Option {
	return broker.SetOption(clientIDKey{}, id)
}

type groupKey struct{}

func Group(id string) broker.Option {
	return broker.SetOption(groupKey{}, id)
}

type commitIntervalKey struct{}

// CommitInterval specifies interval to send commits
func CommitInterval(td time.Duration) broker.Option {
	return broker.SetOption(commitIntervalKey{}, td)
}

type subscribeMaxInflightKey struct{}

// SubscribeMaxInFlight max queued messages
func SubscribeMaxInFlight(n int) broker.SubscribeOption {
	return broker.SetSubscribeOption(subscribeMaxInflightKey{}, n)
}

// SubscribeMaxInFlight max queued messages
func SubscribeFatalOnError(b bool) broker.SubscribeOption {
	return broker.SetSubscribeOption(fatalOnErrorKey{}, b)
}

type publishPromiseKey struct{}

// PublishPromise set the kafka promise func for Produce
func PublishPromise(fn func(*kgo.Record, error)) broker.PublishOption {
	return broker.SetPublishOption(publishPromiseKey{}, fn)
}

// ClientPublishKey set the kafka message key (client option)
func ClientPublishPromise(fn func(*kgo.Record, error)) client.PublishOption {
	return client.SetPublishOption(publishPromiseKey{}, fn)
}

type onRevokeKey struct{}

// OnRevoke sets a callback that is called when partitions are revoked
// (rebalance or shutdown). Use this to discard in-memory batch buffers
// that become stale when partitions move to another consumer.
func OnRevoke(fn func()) broker.Option {
	return broker.SetOption(onRevokeKey{}, fn)
}

type commitOnRevokeKey struct{}

// CommitOnRevoke controls whether marked offsets are committed when partitions
// are revoked (e.g. during rebalance or graceful shutdown). Default is true.
// Set to false for batch manual commit workflows where uncommitted batches
// must be re-delivered after a crash (at-least-once semantics).
func CommitOnRevoke(b bool) broker.Option {
	return broker.SetOption(commitOnRevokeKey{}, b)
}

type exposeLagKey struct{}

// ExposeLag enabled subscriber lag via [meter.Meter]
func ExposeLag(b bool) broker.Option {
	return broker.SetOption(exposeLagKey{}, b)
}
