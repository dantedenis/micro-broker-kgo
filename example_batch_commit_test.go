package kgo_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	kg "github.com/twmb/franz-go/pkg/kgo"
	kgo "go.unistack.org/micro-broker-kgo/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/codec"
)

type BatchProcessor struct {
	mu        sync.Mutex
	buf       []*broker.Message
	batchSize int
	flush     func(batch []*broker.Message) error
	commit    func() error
	timer     *time.Timer
	timeout   time.Duration
}

func NewBatchProcessor(batchSize int, timeout time.Duration, flush func([]*broker.Message) error, commit func() error) *BatchProcessor {
	bp := &BatchProcessor{
		buf:       make([]*broker.Message, 0, batchSize),
		batchSize: batchSize,
		flush:     flush,
		commit:    commit,
		timeout:   timeout,
	}
	bp.timer = time.AfterFunc(timeout, func() { bp.FlushAndCommit() })
	return bp
}

func (bp *BatchProcessor) Add(msg *broker.Message) {
	bp.mu.Lock()
	bp.buf = append(bp.buf, msg)
	needFlush := len(bp.buf) >= bp.batchSize
	bp.mu.Unlock()

	if needFlush {
		bp.FlushAndCommit()
	}
}

func (bp *BatchProcessor) FlushAndCommit() {
	bp.mu.Lock()
	if len(bp.buf) == 0 {
		bp.mu.Unlock()
		return
	}
	batch := bp.buf
	bp.buf = make([]*broker.Message, 0, bp.batchSize)
	bp.timer.Reset(bp.timeout)
	bp.mu.Unlock()

	if err := bp.flush(batch); err != nil {
		log.Printf("flush error (%d msgs): %v", len(batch), err)
		return
	}

	if err := bp.commit(); err != nil {
		log.Printf("commit error: %v", err)
	}
}

func (bp *BatchProcessor) Reset() {
	bp.mu.Lock()
	bp.buf = bp.buf[:0]
	bp.timer.Reset(bp.timeout)
	bp.mu.Unlock()
}

func (bp *BatchProcessor) Stop() {
	bp.timer.Stop()
}

func Example_batchManualCommit() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	const (
		batchSize    = 100
		flushTimeout = 5 * time.Second
	)

	var commitFn func() error

	flushFn := func(batch []*broker.Message) error {
		fmt.Printf("flushed %d messages\n", len(batch))
		return nil
	}

	bp := NewBatchProcessor(batchSize, flushTimeout, flushFn, func() error {
		return commitFn()
	})
	defer bp.Stop()

	b := kgo.NewBroker(
		broker.Addrs("localhost:9092"),
		broker.Codec(codec.NewCodec()),

		kgo.CommitInterval(24*time.Hour),

		kgo.CommitOnRevoke(false),

		kgo.OnRevoke(func() {
			bp.Reset()
			fmt.Println("rebalance: buffer discarded")
		}),

		kgo.Options(
			kg.ClientID("my-service"),
			kg.InstanceID("my-service-pod-0"),
		),
	)

	if err := b.Init(); err != nil {
		log.Fatal(err)
	}
	if err := b.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer b.Disconnect(context.Background()) //nolint:errcheck

	sub, err := b.Subscribe(ctx, "events-topic",
		func(ev broker.Event) error {
			bp.Add(ev.Message())
			return nil
		},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup("my-service-group"),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		log.Fatal(err)
	}

	kgoSub := sub.(*kgo.Subscriber)
	commitFn = func() error {
		return kgoSub.Client().CommitMarkedOffsets(ctx)
	}

	<-ctx.Done()

	bp.FlushAndCommit()
	_ = sub.Unsubscribe(context.Background())

}
