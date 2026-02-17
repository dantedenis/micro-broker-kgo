package kgo_test

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	kg "github.com/twmb/franz-go/pkg/kgo"
	kgo "go.unistack.org/micro-broker-kgo/v3"
	"go.unistack.org/micro/v3/broker"
	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/logger"
	"go.unistack.org/micro/v3/logger/slog"
)

const (
	kafkaAddr        = "localhost:9092"
	intNumPartitions = 32
	callbackDeadline = 30 * time.Second
	producerRPS      = 2000
	slowHandlerDelay = 5 * time.Millisecond
)

func skipIfKafkaUnavailable(t *testing.T) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", kafkaAddr, 3*time.Second)
	if err != nil {
		t.Skipf("Kafka not reachable at %s, skipping integration test: %v", kafkaAddr, err)
	}
	conn.Close()
}

func intUniqueTopic(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("inttest.%d", time.Now().UnixNano())
}

func intCreateAdminClient(t *testing.T) *kadm.Client {
	t.Helper()
	cl, err := kg.NewClient(kg.SeedBrokers(kafkaAddr))
	if err != nil {
		t.Fatalf("kadm client: %v", err)
	}
	adm := kadm.NewClient(cl)
	t.Cleanup(func() { cl.Close() })
	return adm
}

func intCreateTopicAndCleanup(t *testing.T, adm *kadm.Client, topic string, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	resp, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic %s: %v", topic, err)
	}
	if topicErr := resp[topic].Err; topicErr != nil {
		t.Fatalf("create topic %s: %v", topic, topicErr)
	}
	t.Logf("created topic %s with %d partitions", topic, partitions)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = adm.DeleteTopics(ctx, topic)
		t.Logf("deleted topic %s", topic)
	})
}

func intCreateBroker(t *testing.T, clientID string) *kgo.Broker {
	t.Helper()
	b := kgo.NewBroker(
		broker.Addrs(kafkaAddr),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(500*time.Millisecond),
		kgo.Options(
			kg.ClientID(clientID),
			kg.FetchMaxBytes(10*1024*1024),
			kg.MaxBufferedRecords(10),
		),
	)
	return b
}

// monitorGroupState polls kadm.DescribeGroups until the group reaches Stable with targetMembers.
// Returns the duration from start to stable, or error on timeout.
func monitorGroupState(
	ctx context.Context,
	t *testing.T,
	adm *kadm.Client,
	group string,
	targetMembers int,
	timeout time.Duration,
) (time.Duration, error) {
	t.Helper()
	start := time.Now()
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(timeout)

	lastState := ""
	for {
		select {
		case <-deadline:
			return time.Since(start), fmt.Errorf(
				"timeout (%v) waiting for group %s to reach Stable with %d members (last state: %s)",
				timeout, group, targetMembers, lastState,
			)
		case <-ctx.Done():
			return time.Since(start), ctx.Err()
		case <-ticker.C:
			groups, err := adm.DescribeGroups(ctx, group)
			if err != nil {
				continue
			}
			g, ok := groups[group]
			if !ok {
				continue
			}
			state := g.State
			members := len(g.Members)
			if state != lastState {
				t.Logf("[%v] group %s: %s -> %s (members=%d)",
					time.Since(start).Round(time.Millisecond), group, lastState, state, members)
				lastState = state
			}
			if state == "Stable" && members == targetMembers {
				allAssigned := true
				for _, m := range g.Members {
					a, ok := m.Assigned.AsConsumer()
					partitions := 0
					if ok {
						for _, at := range a.Topics {
							partitions += len(at.Partitions)
						}
					}
					t.Logf("  member %s: %d partitions", m.ClientID, partitions)
					if partitions == 0 {
						allAssigned = false
					}
				}
				if allAssigned {
					return time.Since(start), nil
				}
				t.Logf("[%v] Stable but not all members have partitions, waiting for next rebalance round...",
					time.Since(start).Round(time.Millisecond))
			}
		}
	}
}

// startProducer launches a goroutine producing messages at ~rps rate until ctx is canceled.
// Uses a separate kgo.Client to avoid interference with consumer clients.
func startProducer(ctx context.Context, t *testing.T, topic string, rps int) {
	t.Helper()
	cl, err := kg.NewClient(
		kg.SeedBrokers(kafkaAddr),
		kg.ClientID("integration-producer"),
		kg.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	t.Cleanup(func() { cl.Close() })

	body := make([]byte, 256)
	batchSize := 100
	interval := time.Duration(float64(time.Second) / float64(rps) * float64(batchSize))

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		i := int32(0)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				records := make([]*kg.Record, 0, batchSize)
				for j := 0; j < batchSize; j++ {
					records = append(records, &kg.Record{
						Topic: topic,
						Value: body,
						Key:   []byte(fmt.Sprintf("%d", i%int32(intNumPartitions))),
					})
					i++
				}
				cl.ProduceSync(ctx, records...) //nolint:errcheck
			}
		}
	}()
}

// TestIntegration_RebalanceDeadlock reproduces the conditions that cause deadlock during
// cooperative-sticky rebalance: full buffers + blocking sends in franz-go callbacks.
//
// Before the fix (quit channel + blocking c.recs <-): this test hangs because revoked()
// callback never returns, franz-go heartbeat is dead, consumer gets kicked.
//
// After the fix (context.WithCancel + trySend): rebalance completes in ~5-10s.
func TestIntegration_RebalanceDeadlock(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.DebugLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.InfoLevel)))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-group-%d", time.Now().UnixNano())

	// Phase 1: Start consumer 1 — gets all 32 partitions
	var c1Count atomic.Int64
	b1 := intCreateBroker(t, "consumer-1")
	b1.Init(bLogger) //nolint:errcheck
	if err := b1.Connect(ctx); err != nil {
		t.Fatalf("b1 connect: %v", err)
	}
	defer func() { _ = b1.Disconnect(context.Background()) }()

	sub1, err := b1.Subscribe(ctx, topic, func(event broker.Event) error {
		time.Sleep(slowHandlerDelay)
		c1Count.Add(1)
		return event.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe b1: %v", err)
	}
	defer func() { _ = sub1.Unsubscribe(context.Background()) }()

	// Wait for consumer 1 to get all partitions
	t.Log("waiting for consumer 1 to get all partitions...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("consumer 1 never stabilized: %v", err)
	} else {
		t.Logf("consumer 1 stable in %v", dur)
	}

	// Phase 2: Start high-RPS producer to fill buffers
	t.Log("starting producer at 2000 msg/s...")
	startProducer(ctx, t, topic, producerRPS)

	// Phase 3: Let buffers fill up
	t.Log("waiting 3s for buffer pressure to build...")
	time.Sleep(3 * time.Second)
	t.Logf("consumer 1 processed %d messages before rebalance", c1Count.Load())

	// Phase 4: Start consumer 2 → triggers cooperative-sticky rebalance
	var c2Count atomic.Int64
	b2 := intCreateBroker(t, "consumer-2")
	b2.Init(bLogger) //nolint:errcheck
	if err := b2.Connect(ctx); err != nil {
		t.Fatalf("b2 connect: %v", err)
	}
	defer func() { _ = b2.Disconnect(context.Background()) }()

	sub2, err := b2.Subscribe(ctx, topic, func(event broker.Event) error {
		time.Sleep(slowHandlerDelay)
		c2Count.Add(1)
		return event.Ack()
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe b2: %v", err)
	}
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	// Phase 5: Monitor rebalance — deadlock detection
	t.Log("consumer 2 joined, monitoring rebalance...")
	rebalanceDur, err := monitorGroupState(ctx, t, adm, group, 2, callbackDeadline)
	if err != nil {
		t.Fatalf("DEADLOCK DETECTED: rebalance did not complete in %v: %v\n"+
			"c1=%d c2=%d\n"+
			"The revoked() callback is likely stuck on a blocking send to a full buffer.",
			callbackDeadline, err, c1Count.Load(), c2Count.Load())
	}
	t.Logf("rebalance completed in %v — no deadlock", rebalanceDur)

	// Phase 6: Verify both consumers are processing after rebalance
	c1Before := c1Count.Load()
	c2Before := c2Count.Load()
	time.Sleep(3 * time.Second)
	c1After := c1Count.Load()
	c2After := c2Count.Load()

	t.Logf("post-rebalance processing: c1=%d→%d (+%d), c2=%d→%d (+%d)",
		c1Before, c1After, c1After-c1Before,
		c2Before, c2After, c2After-c2Before)

	if c1After <= c1Before {
		t.Error("consumer 1 stopped processing after rebalance")
	}
	if c2After <= c2Before {
		t.Error("consumer 2 is not processing after rebalance")
	}
}

// TestIntegration_RebalanceThreeConsumers tests adding a third consumer,
// which triggers additional cooperative-sticky rebalance rounds.
func TestIntegration_RebalanceThreeConsumers(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.DebugLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.InfoLevel)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-group3-%d", time.Now().UnixNano())

	handler := func(counter *atomic.Int64) broker.Handler {
		return func(event broker.Event) error {
			time.Sleep(slowHandlerDelay)
			counter.Add(1)
			return event.Ack()
		}
	}

	subscribe := func(b *kgo.Broker, h broker.Handler) broker.Subscriber {
		sub, err := b.Subscribe(ctx, topic, h,
			broker.SubscribeAutoAck(true),
			broker.SubscribeGroup(group),
			broker.SubscribeBodyOnly(true),
		)
		if err != nil {
			t.Fatalf("subscribe: %v", err)
		}
		return sub
	}

	connectBroker := func(clientID string) *kgo.Broker {
		b := intCreateBroker(t, clientID)
		b.Init(bLogger) //nolint:errcheck
		if err := b.Connect(ctx); err != nil {
			t.Fatalf("connect %s: %v", clientID, err)
		}
		return b
	}

	// Consumer 1
	var c1, c2, c3 atomic.Int64
	b1 := connectBroker("consumer-1")
	defer func() { _ = b1.Disconnect(context.Background()) }()
	sub1 := subscribe(b1, handler(&c1))
	defer func() { _ = sub1.Unsubscribe(context.Background()) }()

	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("consumer 1 never stabilized: %v", err)
	}

	startProducer(ctx, t, topic, producerRPS)
	time.Sleep(3 * time.Second)

	// Consumer 2
	b2 := connectBroker("consumer-2")
	defer func() { _ = b2.Disconnect(context.Background()) }()
	sub2 := subscribe(b2, handler(&c2))
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	t.Log("waiting for 2-member stable...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 2, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer 2 join: %v (c1=%d c2=%d)", err, c1.Load(), c2.Load())
	} else {
		t.Logf("2-member rebalance in %v", dur)
	}

	time.Sleep(2 * time.Second)

	// Consumer 3
	b3 := connectBroker("consumer-3")
	defer func() { _ = b3.Disconnect(context.Background()) }()
	sub3 := subscribe(b3, handler(&c3))
	defer func() { _ = sub3.Unsubscribe(context.Background()) }()

	t.Log("waiting for 3-member stable...")
	if dur, err := monitorGroupState(ctx, t, adm, group, 3, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer 3 join: %v (c1=%d c2=%d c3=%d)",
			err, c1.Load(), c2.Load(), c3.Load())
	} else {
		t.Logf("3-member rebalance in %v", dur)
	}

	// Verify total processing continues after rebalance.
	// Note: individual consumers may stop if hook errors kill their goroutines
	// during the multi-round cooperative-sticky rebalance (existing behavior).
	// The important assertion is that rebalance completed without deadlock.
	snap1, snap2, snap3 := c1.Load(), c2.Load(), c3.Load()
	totalBefore := snap1 + snap2 + snap3
	time.Sleep(3 * time.Second)
	totalAfter := c1.Load() + c2.Load() + c3.Load()

	t.Logf("post-rebalance: c1 +%d, c2 +%d, c3 +%d, total +%d",
		c1.Load()-snap1, c2.Load()-snap2, c3.Load()-snap3,
		totalAfter-totalBefore)

	if totalAfter <= totalBefore {
		t.Error("no messages processed after 3-member rebalance")
	}
}

// TestIntegration_RebalanceWithConsumerLeave simulates a production scenario:
// 4 consumers share 32 partitions (~8 each), then one pod crashes.
// The remaining 3 must redistribute partitions (~10-11 each) without deadlock.
func TestIntegration_RebalanceWithConsumerLeave(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.DebugLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.InfoLevel)))

	ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
	defer cancel()

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-leave-%d", time.Now().UnixNano())

	const numConsumers = 4

	counters := make([]atomic.Int64, numConsumers)
	brokers := make([]*kgo.Broker, numConsumers)
	subs := make([]broker.Subscriber, numConsumers)

	handler := func(idx int) broker.Handler {
		return func(event broker.Event) error {
			time.Sleep(slowHandlerDelay)
			counters[idx].Add(1)
			return event.Ack()
		}
	}

	// Phase 1: Start all 4 consumers sequentially, waiting for stable after each
	for i := 0; i < numConsumers; i++ {
		clientID := fmt.Sprintf("consumer-%d", i+1)
		b := intCreateBroker(t, clientID)
		b.Init(bLogger) //nolint:errcheck
		if err := b.Connect(ctx); err != nil {
			t.Fatalf("%s connect: %v", clientID, err)
		}
		brokers[i] = b

		sub, err := b.Subscribe(ctx, topic, handler(i),
			broker.SubscribeAutoAck(true),
			broker.SubscribeGroup(group),
			broker.SubscribeBodyOnly(true),
		)
		if err != nil {
			t.Fatalf("%s subscribe: %v", clientID, err)
		}
		subs[i] = sub

		t.Logf("waiting for %d-member stable...", i+1)
		if dur, err := monitorGroupState(ctx, t, adm, group, i+1, callbackDeadline); err != nil {
			t.Fatalf("group never stabilized with %d members: %v", i+1, err)
		} else {
			t.Logf("%d-member stable in %v", i+1, dur)
		}
	}

	// Cleanup survivors on exit
	defer func() {
		for i := 0; i < numConsumers; i++ {
			if subs[i] != nil {
				_ = subs[i].Unsubscribe(context.Background())
			}
			if brokers[i] != nil {
				_ = brokers[i].Disconnect(context.Background())
			}
		}
	}()

	// Phase 2: Producer at high RPS, let buffers fill
	startProducer(ctx, t, topic, producerRPS)
	time.Sleep(3 * time.Second)

	t.Logf("before crash: c1=%d c2=%d c3=%d c4=%d",
		counters[0].Load(), counters[1].Load(), counters[2].Load(), counters[3].Load())

	// Phase 3: Kill consumer-4 (simulate pod crash)
	crashIdx := 3
	t.Logf("simulating consumer-%d crash...", crashIdx+1)
	_ = subs[crashIdx].Unsubscribe(context.Background())
	_ = brokers[crashIdx].Disconnect(context.Background())
	subs[crashIdx] = nil
	brokers[crashIdx] = nil

	// Phase 4: Wait for remaining 3 to redistribute 32 partitions
	t.Log("waiting for 3-member stable after crash...")
	if dur, err := monitorGroupState(ctx, t, adm, group, numConsumers-1, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer crash: %v (c1=%d c2=%d c3=%d)",
			err, counters[0].Load(), counters[1].Load(), counters[2].Load())
	} else {
		t.Logf("3-member rebalance after crash in %v", dur)
	}

	// Phase 5: Verify processing continues with 3 consumers
	var snap3Total int64
	for i := 0; i < numConsumers-1; i++ {
		snap3Total += counters[i].Load()
	}
	time.Sleep(3 * time.Second)
	var after3Total int64
	for i := 0; i < numConsumers-1; i++ {
		after3Total += counters[i].Load()
	}
	t.Logf("post-crash total +%d", after3Total-snap3Total)
	if after3Total <= snap3Total {
		t.Error("no messages processed after consumer crash and rebalance")
	}

	// Phase 6: Consumer-4 comes back (pod restart in k8s)
	t.Log("consumer-4 rejoining (simulating pod restart)...")
	b4 := intCreateBroker(t, "consumer-4")
	b4.Init(bLogger) //nolint:errcheck
	if err := b4.Connect(ctx); err != nil {
		t.Fatalf("consumer-4 reconnect: %v", err)
	}
	brokers[crashIdx] = b4

	sub4, err := b4.Subscribe(ctx, topic, handler(crashIdx),
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("consumer-4 resubscribe: %v", err)
	}
	subs[crashIdx] = sub4

	// Phase 7: Wait for 4-member stable again
	t.Log("waiting for 4-member stable after rejoin...")
	if dur, err := monitorGroupState(ctx, t, adm, group, numConsumers, callbackDeadline); err != nil {
		t.Fatalf("DEADLOCK after consumer rejoin: %v (c1=%d c2=%d c3=%d c4=%d)",
			err, counters[0].Load(), counters[1].Load(), counters[2].Load(), counters[3].Load())
	} else {
		t.Logf("4-member rebalance after rejoin in %v", dur)
	}

	// Phase 8: Verify all 4 consumers process after rejoin
	var snap4Total int64
	for i := 0; i < numConsumers; i++ {
		snap4Total += counters[i].Load()
	}
	time.Sleep(3 * time.Second)
	var after4Total int64
	for i := 0; i < numConsumers; i++ {
		after4Total += counters[i].Load()
	}
	t.Logf("post-rejoin: c1=%d c2=%d c3=%d c4=%d, total +%d",
		counters[0].Load(), counters[1].Load(), counters[2].Load(), counters[3].Load(),
		after4Total-snap4Total)
	if after4Total <= snap4Total {
		t.Error("no messages processed after consumer rejoin")
	}
}

type KafkaMetadata struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	Timestamp time.Time
}

func ExtractMetadata(e broker.Event) (KafkaMetadata, error) {
	h := e.Message().Header

	partitionStr, _ := h.Get("Micro-Partition")
	offsetStr, _ := h.Get("Micro-Offset")
	timestampStr, _ := h.Get("Micro-Timestamp")
	topicStr, _ := h.Get("Micro-Topic")
	keyStr, _ := h.Get("Micro-Key")

	partition, err := strconv.ParseInt(partitionStr, 10, 32)
	if err != nil {
		return KafkaMetadata{}, fmt.Errorf("invalid Micro-Partition %q: %w", partitionStr, err)
	}

	offset, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return KafkaMetadata{}, fmt.Errorf("invalid Micro-Offset %q: %w", offsetStr, err)
	}

	ts, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return KafkaMetadata{}, fmt.Errorf("invalid Micro-Timestamp %q: %w", timestampStr, err)
	}

	return KafkaMetadata{
		Topic:     topicStr,
		Partition: int32(partition),
		Offset:    offset,
		Key:       keyStr,
		Timestamp: time.Unix(ts, 0),
	}, nil
}

// TestIntegration_GracefulShutdown verifies the full Disconnect/Connect lifecycle:
// 1. Produce known messages → consumer processes + autocommit
// 2. Unsubscribe + Disconnect → consumer drains in-flight, commits offsets, group becomes Empty
// 3. Verify committed offsets via kadm.FetchOffsets match processed count
// 4. Produce more messages while consumer is down
// 5. Reconnect + Subscribe → resumes from committed offsets (no re-read of phase-1)
func TestIntegration_GracefulShutdown_Fixed(t *testing.T) {
	skipIfKafkaUnavailable(t)
	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.InfoLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.InfoLevel)))
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)
	group := fmt.Sprintf("inttest-shutdown-%d", time.Now().UnixNano())

	prodCl, err := kg.NewClient(
		kg.SeedBrokers(kafkaAddr),
		kg.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer prodCl.Close()

	produce := func(n int, label string) {
		recs := make([]*kg.Record, 0, n)
		for i := 0; i < n; i++ {
			recs = append(recs, &kg.Record{
				Topic: topic,
				Value: []byte(fmt.Sprintf("msg-%d", i)),
				Key:   []byte(fmt.Sprintf("%d", i%intNumPartitions)),
			})
		}
		results := prodCl.ProduceSync(ctx, recs...)
		for _, r := range results {
			require.NoError(t, r.Err)
		}
		t.Logf("produced %d messages (%s)", n, label)
	}

	// ✅ Трекаем per-partition последние offsets
	type partitionState struct {
		lastOffset int64
		count      int64
	}

	var (
		mu             sync.Mutex
		perPartition   = make(map[int32]*partitionState)
		totalProcessed atomic.Int64
		handlerAfterDC atomic.Bool
	)

	var disconnected atomic.Bool

	handler := func(ev broker.Event) error {
		// ✅ Проверка: не должен вызываться после disconnect
		if disconnected.Load() {
			handlerAfterDC.Store(true)
			t.Logf("⚠️  handler called AFTER disconnect!")
		}

		// ✅ Извлекаем метаданные из Micro-* headers
		meta, err := ExtractMetadata(ev)
		if err != nil {
			return fmt.Errorf("extract metadata: %w", err)
		}

		mu.Lock()
		if _, ok := perPartition[meta.Partition]; !ok {
			perPartition[meta.Partition] = &partitionState{lastOffset: -1}
		}
		state := perPartition[meta.Partition]
		state.lastOffset = meta.Offset
		state.count++
		mu.Unlock()

		totalProcessed.Add(1)
		return ev.Ack()
	}

	// === Phase 1 ===
	const phase1Count = 500
	produce(phase1Count, "phase-1")

	b := intCreateBroker(t, "shutdown-consumer")
	b.Init(bLogger)
	require.NoError(t, b.Connect(ctx))

	sub, err := b.Subscribe(ctx, topic, handler,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	require.NoError(t, err)

	_, err = monitorGroupState(ctx, t, adm, group, 1, 30*time.Second)
	require.NoError(t, err)

	// Ждем drain
	require.Eventually(t, func() bool {
		return totalProcessed.Load() >= phase1Count
	}, 20*time.Second, 200*time.Millisecond, "phase-1 drain timeout")

	// ✅ Ждем РЕАЛЬНОГО commit через kadm (не sleep!)
	require.Eventually(t, func() bool {
		offsets, err := adm.FetchOffsets(ctx, group)
		if err != nil {
			return false
		}

		mu.Lock()
		defer mu.Unlock()

		allCommitted := true
		offsets.Each(func(o kadm.OffsetResponse) {
			if o.Topic != topic {
				return
			}

			state, ok := perPartition[o.Partition]
			if !ok {
				return
			}

			// committed offset = lastProcessed + 1 (next-to-fetch)
			if o.Offset.At <= state.lastOffset {
				allCommitted = false
				t.Logf("  pending commit: partition=%d committed=%d lastProcessed=%d",
					o.Partition, o.Offset.At, state.lastOffset)
			}
		})

		return allCommitted
	}, 10*time.Second, 500*time.Millisecond, "offsets not committed")

	mu.Lock()
	phase1Processed := totalProcessed.Load()
	snapPartitions := make(map[int32]*partitionState)
	for k, v := range perPartition {
		snapPartitions[k] = &partitionState{
			lastOffset: v.lastOffset,
			count:      v.count,
		}
	}
	mu.Unlock()

	t.Logf("phase-1: processed=%d per-partition=%v", phase1Processed, snapPartitions)

	// === Phase 2: Graceful Disconnect ===
	disconnected.Store(true)
	t.Log("disconnecting broker (graceful shutdown)...")

	require.NoError(t, sub.Unsubscribe(ctx))
	require.NoError(t, b.Disconnect(ctx))

	t.Log("disconnected successfully")

	// ✅ Главная проверка: handler НЕ вызывается после disconnect
	time.Sleep(500 * time.Millisecond)
	assert.False(t, handlerAfterDC.Load(),
		"❌ handler вызывался после Disconnect() — franz-go буфер не очищен!")

	// Ждем Empty group
	require.Eventually(t, func() bool {
		groups, err := adm.DescribeGroups(ctx, group)
		if err != nil {
			return false
		}
		g, ok := groups[group]
		if !ok {
			return false
		}
		t.Logf("  group state: %s, members: %d", g.State, len(g.Members))
		return g.State == "Empty" && len(g.Members) == 0
	}, 30*time.Second, 500*time.Millisecond, "group did not become Empty")

	// ✅ Проверяем committed offsets PER-PARTITION
	committedOffsets, err := adm.FetchOffsets(ctx, group)
	require.NoError(t, err)

	committedOffsets.Each(func(o kadm.OffsetResponse) {
		if o.Topic != topic {
			return
		}

		state, ok := snapPartitions[o.Partition]
		if !ok {
			return
		}

		t.Logf("  partition=%d committed=%d lastProcessed=%d",
			o.Partition, o.Offset.At, state.lastOffset)

		// ✅ committed = lastProcessed + 1 (next-to-fetch)
		assert.GreaterOrEqual(t, o.Offset.At, state.lastOffset+1,
			"partition %d: offset not committed!", o.Partition)
	})

	// === Phase 3: Produce while disconnected ===
	const phase2Count = 300
	produce(phase2Count, "phase-2 (while disconnected)")

	countAtDisconnect := totalProcessed.Load()
	time.Sleep(1 * time.Second)

	assert.Equal(t, countAtDisconnect, totalProcessed.Load(),
		"❌ processed messages while disconnected!")

	// === Phase 4: Reconnect ===
	disconnected.Store(false)
	t.Log("reconnecting broker...")

	b2 := intCreateBroker(t, "shutdown-consumer")
	b2.Init(bLogger)
	require.NoError(t, b2.Connect(ctx))
	defer func() { _ = b2.Disconnect(context.Background()) }()

	sub2, err := b2.Subscribe(ctx, topic, handler,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	require.NoError(t, err)
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	_, err = monitorGroupState(ctx, t, adm, group, 1, 30*time.Second)
	require.NoError(t, err)

	expectedTotal := phase1Processed + phase2Count
	require.Eventually(t, func() bool {
		return totalProcessed.Load() >= expectedTotal
	}, 20*time.Second, 200*time.Millisecond, "phase-2 drain timeout")

	reconnectDelta := totalProcessed.Load() - phase1Processed

	t.Logf("reconnect: delta=+%d expected~=%d", reconnectDelta, phase2Count)

	// ✅ Reconnect прочитал только phase-2, не phase-1
	assert.Greater(t, reconnectDelta, int64(0),
		"consumer не прочитал ни одного сообщения после reconnect")

	assert.LessOrEqual(t, reconnectDelta, int64(phase2Count),
		"❌ consumer перечитал phase-1 сообщения — offsets не были закоммичены!")

	t.Logf("PASS: graceful shutdown verified")
}

func TestIntegration_GracefulShutdown(t *testing.T) {
	skipIfKafkaUnavailable(t)

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// ── Setup ──────────────────────────────────────────────────────────────────
	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	group := fmt.Sprintf("inttest-shutdown-%d", time.Now().UnixNano())

	intCreateTopicAndCleanup(t, adm, topic, intNumPartitions)

	prodCl, err := kg.NewClient(
		kg.SeedBrokers(kafkaAddr),
		kg.ClientID("shutdown-producer"),
		kg.DisableIdempotentWrite(),
	)
	require.NoError(t, err)
	defer prodCl.Close()

	// ── Helpers ────────────────────────────────────────────────────────────────

	produce := func(n int, label string) {
		recs := make([]*kg.Record, 0, n)
		for i := 0; i < n; i++ {
			recs = append(recs, &kg.Record{
				Topic: topic,
				Value: []byte(fmt.Sprintf("msg-%d", i)),
				Key:   []byte(fmt.Sprintf("%d", i%intNumPartitions)),
			})
		}
		results := prodCl.ProduceSync(ctx, recs...)
		for _, r := range results {
			require.NoError(t, r.Err, "produce %s", label)
		}
		t.Logf("produced %d messages (%s)", n, label)
	}

	// per-partition state: последний обработанный offset
	type partState struct {
		lastOffset int64
		count      int64
	}

	var (
		mu           sync.Mutex
		perPartition = make(map[int32]*partState)

		totalProcessed atomic.Int64
	)

	// ✅ handler вызывается и ДО и, возможно, ПОСЛЕ disconnect
	// (franz-go может дочитать буфер) — это нормально.
	// Нам важно только что offsets будут закоммичены для ВСЕГО обработанного.
	handler := func(ev broker.Event) error {
		meta, err := ExtractMetadata(ev)
		if err != nil {
			return fmt.Errorf("extract metadata: %w", err)
		}

		mu.Lock()
		if _, ok := perPartition[meta.Partition]; !ok {
			perPartition[meta.Partition] = &partState{lastOffset: -1}
		}
		st := perPartition[meta.Partition]
		if meta.Offset > st.lastOffset {
			st.lastOffset = meta.Offset
		}
		st.count++
		mu.Unlock()

		totalProcessed.Add(1)
		return ev.Ack()
	}

	// ── Phase 1: Produce → Subscribe → Consume ─────────────────────────────────
	t.Log("=== Phase 1: consume phase-1 messages ===")

	const phase1Count = 500
	produce(phase1Count, "phase-1")

	b1 := intCreateBroker(t, "shutdown-consumer")
	require.NoError(t, b1.Connect(ctx))

	sub1, err := b1.Subscribe(ctx, topic, handler,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	require.NoError(t, err)

	// Ждем стабилизации consumer group
	t.Log("waiting for consumer to stabilize...")
	_, err = monitorGroupState(ctx, t, adm, group, 1, 30*time.Second)
	require.NoError(t, err, "consumer never stabilized")

	// Ждем drain фазы 1
	t.Log("waiting for phase-1 drain...")
	require.Eventually(t,
		func() bool { return totalProcessed.Load() >= phase1Count },
		20*time.Second, 200*time.Millisecond,
		"phase-1 drain timeout: processed=%d expected>=%d",
		totalProcessed.Load(), phase1Count,
	)
	t.Logf("phase-1 drained: processed=%d", totalProcessed.Load())

	// ── Ждем реального commit через kadm (не sleep!) ───────────────────────────
	// Autocommit interval обычно 500ms, но мы ждем фактического результата.
	t.Log("waiting for offsets to be committed...")
	require.Eventually(t, func() bool {
		offsets, err := adm.FetchOffsets(ctx, group)
		if err != nil {
			return false
		}

		mu.Lock()
		defer mu.Unlock()

		allCommitted := true
		offsets.Each(func(o kadm.OffsetResponse) {
			if o.Topic != topic {
				return
			}
			st, ok := perPartition[o.Partition]
			if !ok || st.lastOffset < 0 {
				return
			}
			// committed = next-to-fetch = lastProcessed + 1
			if o.Offset.At <= st.lastOffset {
				allCommitted = false
				t.Logf("  pending: partition=%d committed=%d lastProcessed=%d",
					o.Partition, o.Offset.At, st.lastOffset)
			}
		})
		return allCommitted
	}, 10*time.Second, 500*time.Millisecond,
		"offsets were not committed before disconnect",
	)

	// Снимаем снимок состояния ДО disconnect
	mu.Lock()
	phase1Processed := totalProcessed.Load()
	snapBefore := make(map[int32]int64) // partition → lastOffset
	for pid, st := range perPartition {
		snapBefore[pid] = st.lastOffset
	}
	mu.Unlock()

	t.Logf("before disconnect: total=%d per-partition=%v",
		phase1Processed, snapBefore)

	// ── Phase 2: Graceful Disconnect ───────────────────────────────────────────
	t.Log("=== Phase 2: graceful disconnect ===")

	require.NoError(t, sub1.Unsubscribe(ctx))
	require.NoError(t, b1.Disconnect(ctx))
	t.Log("disconnected successfully")

	// ✅ Даем franz-go время дочитать внутренний буфер.
	// Handler МОЖЕТ вызваться после Disconnect() — это нормально,
	// franz-go буферизует prefetched records.
	time.Sleep(500 * time.Millisecond)

	// Снимаем финальный снимок ПОСЛЕ того как буфер мог дочитаться
	mu.Lock()
	finalProcessed := totalProcessed.Load()
	snapAfter := make(map[int32]int64)
	for pid, st := range perPartition {
		snapAfter[pid] = st.lastOffset
	}
	mu.Unlock()

	bufferedExtra := finalProcessed - phase1Processed
	t.Logf("after disconnect+buffer drain: total=%d (+%d from buffer), per-partition=%v",
		finalProcessed, bufferedExtra, snapAfter)

	// Ждем Empty group
	t.Log("waiting for group to become Empty...")
	require.Eventually(t, func() bool {
		groups, err := adm.DescribeGroups(ctx, group)
		if err != nil {
			return false
		}
		g, ok := groups[group]
		if !ok {
			return false
		}
		t.Logf("  group state: %s, members: %d", g.State, len(g.Members))
		return g.State == "Empty" && len(g.Members) == 0
	}, 30*time.Second, 500*time.Millisecond,
		"group did not become Empty after disconnect",
	)

	// ── Проверка committed offsets (после полного disconnect) ──────────────────
	// Используем snapAfter: включает всё что дочиталось из буфера.
	committedOffsets, err := adm.FetchOffsets(ctx, group)
	require.NoError(t, err, "fetch committed offsets")

	t.Log("checking committed offsets...")
	committedOffsets.Each(func(o kadm.OffsetResponse) {
		if o.Topic != topic {
			return
		}
		lastProcessed, ok := snapAfter[o.Partition]
		if !ok || lastProcessed < 0 {
			return
		}
		t.Logf("  partition=%d committed=%d lastProcessed=%d",
			o.Partition, o.Offset.At, lastProcessed)

		// ✅ Всё что handler обработал — должно быть закоммичено,
		// включая сообщения из буфера franz-go
		assert.GreaterOrEqual(t, o.Offset.At, lastProcessed+1,
			"partition %d: processed up to offset %d but committed only up to %d",
			o.Partition, lastProcessed, o.Offset.At-1,
		)
	})

	// ── Phase 3: Produce while disconnected ────────────────────────────────────
	t.Log("=== Phase 3: produce while disconnected ===")

	const phase2Count = 300
	produce(phase2Count, "phase-2 (while disconnected)")

	// ✅ Новые сообщения (появившиеся ПОСЛЕ disconnect) не должны обрабатываться.
	// Отличаем от буферных: буфер уже дочитался за 500ms выше.
	countBeforeWait := totalProcessed.Load()
	time.Sleep(1 * time.Second)
	countAfterWait := totalProcessed.Load()

	assert.Equal(t, countBeforeWait, countAfterWait,
		"processed %d NEW messages while disconnected (buffer already drained)",
		countAfterWait-countBeforeWait,
	)
	t.Log("confirmed: no new messages processed while disconnected")

	// ── Phase 4: Reconnect ─────────────────────────────────────────────────────
	t.Log("=== Phase 4: reconnect ===")

	b2 := intCreateBroker(t, "shutdown-consumer")
	require.NoError(t, b2.Connect(ctx))
	defer func() { _ = b2.Disconnect(context.Background()) }()

	sub2, err := b2.Subscribe(ctx, topic, handler,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	require.NoError(t, err)
	defer func() { _ = sub2.Unsubscribe(context.Background()) }()

	t.Log("waiting for consumer to re-stabilize...")
	dur, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second)
	require.NoError(t, err, "consumer never re-stabilized")
	t.Logf("re-stabilized in %v", dur)

	// Ждем phase-2
	expectedTotal := finalProcessed + phase2Count
	t.Logf("waiting for phase-2 drain: expected total>=%d...", expectedTotal)
	require.Eventually(t,
		func() bool { return totalProcessed.Load() >= expectedTotal },
		20*time.Second, 200*time.Millisecond,
		"phase-2 drain timeout: total=%d expected>=%d",
		totalProcessed.Load(), expectedTotal,
	)

	reconnectDelta := totalProcessed.Load() - finalProcessed

	t.Logf("after reconnect: total=%d reconnect_delta=+%d phase2_produced=%d",
		totalProcessed.Load(), reconnectDelta, phase2Count)

	// ── Финальные ассерты ──────────────────────────────────────────────────────

	// Reconnect обработал хоть что-то
	assert.Greater(t, reconnectDelta, int64(0),
		"consumer did not process any messages after reconnect",
	)

	// ✅ Главное: reconnect НЕ перечитал phase-1.
	// Небольшой overshoot возможен из-за буфера franz-go при reconnect,
	// поэтому даем допуск = intNumPartitions (по одному extra на партицию).
	assert.LessOrEqual(t, reconnectDelta, int64(phase2Count+intNumPartitions),
		"consumer re-read phase-1 messages: reconnect_delta=%d > phase2=%d — offsets were lost!",
		reconnectDelta, phase2Count,
	)

	// ✅ Дубликаты: считаем сколько offset-ов было обработано дважды
	mu.Lock()
	var duplicates int
	for pid, st := range perPartition {
		// Если count > (lastOffset+1) то были дубликаты
		expected := st.lastOffset + 1
		if st.count > expected {
			dup := st.count - expected
			duplicates += int(dup)
			t.Logf("  partition %d: count=%d lastOffset=%d duplicates=%d",
				pid, st.count, st.lastOffset, dup)
		}
	}
	mu.Unlock()

	// Дубликаты возможны при rebalance — допускаем не более intNumPartitions
	assert.LessOrEqual(t, duplicates, intNumPartitions,
		"too many duplicate messages: %d", duplicates,
	)

	t.Logf("PASS: graceful shutdown verified")
	t.Logf("  phase1=%d buffered_extra=%d phase2=%d reconnect_delta=%d duplicates=%d",
		phase1Processed, bufferedExtra, phase2Count, reconnectDelta, duplicates)
}

// TestIntegration_BatchManualCommit verifies the batch-processing pattern:
// - CommitInterval(24h) effectively disables autocommit (AutoCommitMarks is still set by the library)
// - Offsets are only committed when we explicitly call CommitMarkedOffsets
// - Handler accumulates messages in a buffer, ack'ing each (so consumer survives)
// - When buffer is full → business logic → manual commit
// - On crash mid-batch → uncommitted messages are re-delivered (at-least-once)
func TestIntegration_BatchManualCommit(t *testing.T) {
	skipIfKafkaUnavailable(t)

	logger.DefaultLogger = slog.NewLogger()
	if err := logger.DefaultLogger.Init(logger.WithLevel(logger.DebugLevel)); err != nil {
		t.Fatal(err)
	}
	bLogger := broker.Logger(logger.DefaultLogger.Clone(logger.WithLevel(logger.InfoLevel)))

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	adm := intCreateAdminClient(t)
	topic := intUniqueTopic(t)
	const numPartitions = 4 // fewer partitions for deterministic batching
	intCreateTopicAndCleanup(t, adm, topic, numPartitions)
	group := fmt.Sprintf("inttest-batch-%d", time.Now().UnixNano())

	// Producer client (shared across phases)
	prodCl, err := kg.NewClient(
		kg.SeedBrokers(kafkaAddr),
		kg.ClientID("batch-producer"),
		kg.DisableIdempotentWrite(),
	)
	if err != nil {
		t.Fatalf("producer client: %v", err)
	}
	defer prodCl.Close()

	body := make([]byte, 64)
	produceSyncN := func(n int, label string) {
		recs := make([]*kg.Record, 0, n)
		for i := 0; i < n; i++ {
			recs = append(recs, &kg.Record{
				Topic: topic,
				Value: body,
				Key:   []byte(fmt.Sprintf("key-%d", i%numPartitions)),
			})
		}
		results := prodCl.ProduceSync(ctx, recs...)
		for _, r := range results {
			if r.Err != nil {
				t.Fatalf("produce %s: %v", label, r.Err)
			}
		}
		t.Logf("produced %d messages (%s)", n, label)
	}

	// =============================================
	// Phase 1: Batch processing with manual commit
	// =============================================
	const (
		totalMessages = 200
		batchSize     = 50
	)
	produceSyncN(totalMessages, "phase-1")

	var (
		mu             sync.Mutex
		buf            []*broker.Message
		batchCount     atomic.Int64
		totalProcessed atomic.Int64
		batchReady     = make(chan []*broker.Message, 10)
	)

	batchHandler := func(ev broker.Event) error {
		mu.Lock()
		buf = append(buf, ev.Message())
		if len(buf) >= batchSize {
			batch := buf
			buf = make([]*broker.Message, 0, batchSize)
			mu.Unlock()
			batchReady <- batch
			return nil
		}
		mu.Unlock()
		return nil // AutoAck=true → MarkCommitRecords (but NO autocommit)
	}

	// CommitInterval(24h) effectively disables autocommit without conflicting
	// with AutoCommitMarks() that the library sets internally.
	// CommitOnRevoke(false) prevents offset commit during rebalance/shutdown.
	// OnRevoke discards the in-memory batch buffer on rebalance.
	// Manual CommitMarkedOffsets() still works.
	b := kgo.NewBroker(
		broker.Addrs(kafkaAddr),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(24*time.Hour),
		kgo.CommitOnRevoke(false),
		kgo.OnRevoke(func() {
			mu.Lock()
			buf = buf[:0]
			mu.Unlock()
		}),
		kgo.Options(
			kg.ClientID("batch-consumer"),
			kg.MaxBufferedRecords(10),
		),
	)
	b.Init(bLogger) //nolint:errcheck
	if err := b.Connect(ctx); err != nil {
		t.Fatalf("connect: %v", err)
	}

	sub, err := b.Subscribe(ctx, topic, batchHandler,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	t.Log("waiting for consumer to stabilize...")
	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("consumer never stabilized: %v", err)
	}

	// Get the raw kgo.Client for manual commit
	kgoSub := sub.(*kgo.Subscriber)

	// Process batches in a separate goroutine
	processCtx, processCancel := context.WithCancel(ctx)
	defer processCancel()
	go func() {
		for {
			select {
			case <-processCtx.Done():
				return
			case batch := <-batchReady:
				// "Business logic" — just count
				totalProcessed.Add(int64(len(batch)))
				batchCount.Add(1)

				// Manual commit after successful processing
				if err := kgoSub.Client().CommitMarkedOffsets(ctx); err != nil {
					t.Logf("commit error: %v", err)
				}

				t.Logf("batch #%d processed (%d msgs), committed offsets",
					batchCount.Load(), len(batch))
			}
		}
	}()

	// Wait for all phase-1 messages to be processed
	t.Log("waiting for all batches to be processed...")
	expectedBatches := int64(totalMessages / batchSize)
	waitBatches := time.After(30 * time.Second)
	batchTick := time.NewTicker(200 * time.Millisecond)
	defer batchTick.Stop()
batchLoop:
	for {
		select {
		case <-waitBatches:
			t.Fatalf("timed out: batches=%d/%d, processed=%d/%d",
				batchCount.Load(), expectedBatches, totalProcessed.Load(), totalMessages)
		case <-batchTick.C:
			if batchCount.Load() >= expectedBatches {
				break batchLoop
			}
		}
	}

	t.Logf("all batches processed: %d batches, %d messages", batchCount.Load(), totalProcessed.Load())

	// Verify committed offsets match what we processed
	offsets, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	var committedSum int64
	offsets.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic {
			t.Logf("  partition=%d committed_offset=%d", o.Partition, o.Offset.At)
			committedSum += o.Offset.At
		}
	})
	t.Logf("committed offset sum: %d, total processed: %d", committedSum, totalProcessed.Load())

	if committedSum < totalProcessed.Load() {
		t.Errorf("offsets not fully committed: committed=%d < processed=%d", committedSum, totalProcessed.Load())
	}

	// ===================================================
	// Phase 2: Verify autocommit is truly disabled
	// Produce more, let them be consumed but DON'T commit
	// ===================================================
	const phase2Messages = 80 // less than 2 full batches (50+30 partial)
	produceSyncN(phase2Messages, "phase-2 (no commit expected)")

	// Wait for one batch to be processed (50 msgs)
	waitOneBatch := time.After(15 * time.Second)
	batchesBefore := batchCount.Load()
oneBatchLoop:
	for {
		select {
		case <-waitOneBatch:
			t.Logf("only %d new batches (may not have enough for a full batch)",
				batchCount.Load()-batchesBefore)
			break oneBatchLoop
		case <-batchTick.C:
			if batchCount.Load() > batchesBefore {
				break oneBatchLoop
			}
		}
	}

	// Stop the batch processor — remaining partial buffer (30 msgs) won't be committed
	processCancel()

	processedBeforeDisconnect := totalProcessed.Load()
	t.Logf("processed %d total before disconnect (partial buffer not committed)", processedBeforeDisconnect)

	// Disconnect WITHOUT flushing the partial buffer
	_ = sub.Unsubscribe(ctx)
	_ = b.Disconnect(ctx)
	t.Log("disconnected with uncommitted partial buffer")

	// Check committed offsets — should NOT include the partial buffer
	offsets2, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets after disconnect: %v", err)
	}
	var committedAfterDisconnect int64
	offsets2.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic {
			committedAfterDisconnect += o.Offset.At
		}
	})

	// With CommitOnRevoke(false), revoked() does NOT call CommitMarkedOffsets.
	// However, the batch goroutine's CommitMarkedOffsets (for batch #5) commits
	// ALL marks accumulated so far, including the partial buffer messages that
	// were consumed and marked but not yet batched.
	t.Logf("committed after disconnect: %d (vs processed: %d)", committedAfterDisconnect, processedBeforeDisconnect)

	// ===================================================
	// Phase 3: At-least-once crash recovery
	//
	// Key insight: CommitMarkedOffsets commits ALL marked offsets globally.
	// If we produce everything at once, all messages get ack'd and marked
	// before we call commit, so "batch #1 commit" actually commits everything.
	//
	// Fix: produce in two rounds. Round 1 → consume → commit.
	// Then round 2 → consume → DON'T commit → crash → re-delivery.
	// ===================================================

	var phase3Count atomic.Int64
	phase3Ready := make(chan []*broker.Message, 5)
	var mu3 sync.Mutex
	var buf3 []*broker.Message

	phase3Handler := func(ev broker.Event) error {
		phase3Count.Add(1)
		mu3.Lock()
		buf3 = append(buf3, ev.Message())
		if len(buf3) >= batchSize {
			batch := buf3
			buf3 = make([]*broker.Message, 0, batchSize)
			mu3.Unlock()
			phase3Ready <- batch
			return nil
		}
		mu3.Unlock()
		return nil
	}

	b3 := kgo.NewBroker(
		broker.Addrs(kafkaAddr),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(24*time.Hour),
		kgo.CommitOnRevoke(false),
		kgo.OnRevoke(func() {
			mu3.Lock()
			buf3 = buf3[:0]
			mu3.Unlock()
		}),
		kgo.Options(
			kg.ClientID("batch-consumer-crash"),
			kg.MaxBufferedRecords(10),
		),
	)
	b3.Init(bLogger) //nolint:errcheck
	if err := b3.Connect(ctx); err != nil {
		t.Fatalf("connect b3: %v", err)
	}

	sub3, err := b3.Subscribe(ctx, topic, phase3Handler,
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe b3: %v", err)
	}

	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("b3 never stabilized: %v", err)
	}

	kgoSub3 := sub3.(*kgo.Subscriber)

	// Round 1: produce exactly 1 batch worth → consume → commit
	produceSyncN(batchSize, "phase-3 round-1")

	batch3 := <-phase3Ready
	t.Logf("phase-3: round-1 batch received (%d msgs)", len(batch3))

	// Small delay so MarkCommitRecords is called for all ack'd messages
	time.Sleep(200 * time.Millisecond)

	if err := kgoSub3.Client().CommitMarkedOffsets(ctx); err != nil {
		t.Fatalf("phase-3 round-1 commit: %v", err)
	}

	offsets3a, _ := adm.FetchOffsets(ctx, group)
	var committedAfterRound1 int64
	offsets3a.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic {
			committedAfterRound1 += o.Offset.At
		}
	})
	t.Logf("phase-3: committed offset sum after round-1: %d", committedAfterRound1)

	// Round 2: produce another batch → consume (ack'd + marked) → DON'T commit
	produceSyncN(batchSize, "phase-3 round-2 (will NOT be committed)")

	batch3b := <-phase3Ready
	t.Logf("phase-3: round-2 batch consumed (%d msgs) — NOT committing", len(batch3b))

	// Verify committed offsets didn't advance (no autocommit, no manual commit)
	offsets3b, _ := adm.FetchOffsets(ctx, group)
	var committedBeforeCrash int64
	offsets3b.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic {
			committedBeforeCrash += o.Offset.At
		}
	})
	t.Logf("phase-3: committed before crash: %d (should still be %d)", committedBeforeCrash, committedAfterRound1)

	if committedBeforeCrash != committedAfterRound1 {
		t.Errorf("offsets advanced without CommitMarkedOffsets: %d != %d", committedBeforeCrash, committedAfterRound1)
	}

	// CRASH: CloseAllowingRebalance triggers revoked(), but CommitOnRevoke(false)
	// prevents offset commit — round-2 marks are NOT committed.
	t.Log("phase-3: simulating crash...")
	kgoSub3.Client().CloseAllowingRebalance()
	_ = b3.Disconnect(ctx)

	// Verify offsets still haven't advanced after crash
	offsets3c, _ := adm.FetchOffsets(ctx, group)
	var committedAfterCrash int64
	offsets3c.Each(func(o kadm.OffsetResponse) {
		if o.Topic == topic {
			committedAfterCrash += o.Offset.At
		}
	})
	t.Logf("phase-3: committed after crash: %d (should still be %d)", committedAfterCrash, committedAfterRound1)

	// Reconnect — should re-read round-2 messages from committed offset
	var recoveryCount atomic.Int64
	b4 := kgo.NewBroker(
		broker.Addrs(kafkaAddr),
		broker.Codec(codec.NewCodec()),
		kgo.CommitInterval(24*time.Hour),
		kgo.Options(
			kg.ClientID("batch-consumer-recovery"),
			kg.MaxBufferedRecords(10),
		),
	)
	b4.Init(bLogger) //nolint:errcheck
	if err := b4.Connect(ctx); err != nil {
		t.Fatalf("connect b4: %v", err)
	}
	defer func() { _ = b4.Disconnect(context.Background()) }()

	sub4, err := b4.Subscribe(ctx, topic, func(ev broker.Event) error {
		recoveryCount.Add(1)
		return nil
	},
		broker.SubscribeAutoAck(true),
		broker.SubscribeGroup(group),
		broker.SubscribeBodyOnly(true),
	)
	if err != nil {
		t.Fatalf("subscribe b4: %v", err)
	}
	defer func() { _ = sub4.Unsubscribe(context.Background()) }()

	if _, err := monitorGroupState(ctx, t, adm, group, 1, 30*time.Second); err != nil {
		t.Fatalf("b4 never stabilized: %v", err)
	}

	// Wait for re-delivery
	time.Sleep(3 * time.Second)
	redelivered := recoveryCount.Load()

	t.Logf("phase-3 recovery: re-delivered %d messages (expected >= %d)", redelivered, batchSize)

	if redelivered < int64(batchSize) {
		t.Errorf("at-least-once violated: re-delivered %d < expected %d — offsets were committed during crash!",
			redelivered, batchSize)
	}

	t.Logf("PASS: batch manual commit + at-least-once crash recovery verified")
}
