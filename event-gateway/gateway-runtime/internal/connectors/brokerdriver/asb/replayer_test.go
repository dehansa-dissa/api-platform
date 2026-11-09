package asb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/wso2/api-platform/event-gateway/gateway-runtime/internal/connectors"
)

const maxEntityNameLength = 260

func assertValidSubscriptionName(t *testing.T, label, name string) {
	t.Helper()
	if name == "" || len(name) > maxSubscriptionNameLength {
		t.Fatalf("%s = %q is %d characters, outside the 1-%d ASB limit",
			label, name, len(name), maxSubscriptionNameLength)
	}
}

const testTopic = "1c2f3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60"

func TestSnapshotQueueName_DeterministicAndSuffixed(t *testing.T) {
	got := snapshotQueueName(testTopic)
	want := testTopic + "-snapshot"
	if got != want {
		t.Fatalf("snapshotQueueName() = %q, want %q", got, want)
	}
	if len(got) > maxEntityNameLength {
		t.Fatalf("snapshotQueueName() length %d exceeds ASB limit %d", len(got), maxEntityNameLength)
	}
}

func TestLeaderLockQueueName_DeterministicAndSuffixed(t *testing.T) {
	got := leaderLockQueueName(testTopic)
	want := testTopic + "-leader-lock"
	if got != want {
		t.Fatalf("leaderLockQueueName() = %q, want %q", got, want)
	}
}

func TestSnapshotAndLockQueueNames_NeverCollide(t *testing.T) {
	snapshot := snapshotQueueName(testTopic)
	lock := leaderLockQueueName(testTopic)
	if snapshot == lock {
		t.Fatalf("snapshot and lock queue names must never collide, both were %q", snapshot)
	}
}

func TestCacheSubscriptionName_DeterministicAndBounded(t *testing.T) {
	a := cacheSubscriptionName("runtime-instance-1")
	b := cacheSubscriptionName("runtime-instance-1")
	if a != b {
		t.Fatalf("cacheSubscriptionName must be deterministic for the same runtimeID, got %q and %q", a, b)
	}
	assertValidSubscriptionName(t, "cacheSubscriptionName()", a)

	other := cacheSubscriptionName("runtime-instance-2")
	if a == other {
		t.Fatalf("cacheSubscriptionName should differ across distinct runtimeIDs, both were %q", a)
	}
}

func TestSubscriptionNames_BoundedRegardlessOfInputLength(t *testing.T) {
	longID := ""
	for i := 0; i < 500; i++ {
		longID += "x"
	}
	for name, got := range map[string]string{
		"cacheSubscriptionName": cacheSubscriptionName(longID),
	} {
		assertValidSubscriptionName(t, name+"() with a long input", got)
	}
}

func newTestCompactedState() *compactedTopicState {
	return &compactedTopicState{
		topic: testTopic,
		cache: make(map[string][]byte),
		dirty: make(chan struct{}, 1),
	}
}

func takeDirtySignal(s *compactedTopicState) bool {
	select {
	case <-s.dirty:
		return true
	default:
		return false
	}
}

func TestApplyToCache_UpsertMarksDirty(t *testing.T) {
	s := newTestCompactedState()

	s.applyToCache(&connectors.Message{Key: []byte("topic:cb"), Value: []byte(`{"active":true}`)})

	if got := string(s.cache["topic:cb"]); got != `{"active":true}` {
		t.Fatalf("expected value to be cached, got %q", got)
	}
	if !takeDirtySignal(s) {
		t.Fatal("expected an upsert to mark the cache dirty so the leader snapshots it")
	}
}

func TestApplyToCache_TombstoneDeletesAndMarksDirty(t *testing.T) {
	s := newTestCompactedState()
	s.cache["topic:cb"] = []byte("existing")
	s.drainDirty()

	s.applyToCache(&connectors.Message{Key: []byte("topic:cb"), Value: nil})

	if _, present := s.cache["topic:cb"]; present {
		t.Fatal("expected a tombstone to remove the key from the cache")
	}
	if !takeDirtySignal(s) {
		t.Fatal("expected a tombstone to mark the cache dirty — an unsubscribe must reach the snapshot too")
	}
}

func TestMarkDirty_CoalescesBurstAndNeverBlocks(t *testing.T) {
	s := newTestCompactedState()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 1000; i++ {
			s.markDirty()
		}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("markDirty blocked; it must never block the live tail")
	}

	if !takeDirtySignal(s) {
		t.Fatal("expected a pending signal after the burst")
	}
	if takeDirtySignal(s) {
		t.Fatal("expected the burst to coalesce into exactly one pending signal")
	}
}

func TestDrainDirty_ClearsPendingSignal(t *testing.T) {
	s := newTestCompactedState()
	s.markDirty()

	s.drainDirty()

	if takeDirtySignal(s) {
		t.Fatal("expected drainDirty to clear the pending signal")
	}
}

func TestDrainDirty_ReturnsWhenNothingPending(t *testing.T) {
	s := newTestCompactedState()

	returned := make(chan struct{})
	go func() {
		defer close(returned)
		s.drainDirty()
		s.drainDirty()
	}()

	select {
	case <-returned:
	case <-time.After(2 * time.Second):
		t.Fatal("drainDirty did not return on an empty channel; the leader loop would hang here")
	}
}

func TestMarkDirty_SurvivesUntilLeadershipIsAcquired(t *testing.T) {
	s := newTestCompactedState()

	s.applyToCache(&connectors.Message{Key: []byte("k"), Value: []byte("v")})

	if !takeDirtySignal(s) {
		t.Fatal("expected the dirty flag to persist until a leader consumes it")
	}
}

func testLeaderTimings() leaderTimings {
	return leaderTimings{
		snapshotInterval: 10 * time.Second,
		renewInterval:    10 * time.Second,
		debounce:         5 * time.Millisecond,
		renewMaxAttempts: 2,
		renewRetryDelay:  5 * time.Millisecond,
	}
}

// failNTimes returns a renew func that fails with err its first n calls, then
// succeeds, recording how many times it was called.
func failNTimes(n int, err error) (func(context.Context) error, *int) {
	calls := 0
	return func(context.Context) error {
		calls++
		if calls <= n {
			return err
		}
		return nil
	}, &calls
}

type publishRecorder struct {
	mu     sync.Mutex
	calls  int
	first  chan struct{}
	closed bool
	err    error
}

func newPublishRecorder() *publishRecorder {
	return &publishRecorder{first: make(chan struct{})}
}

func (p *publishRecorder) publish(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if !p.closed {
		p.closed = true
		close(p.first)
	}
	return p.err
}

func (p *publishRecorder) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func neverFails(context.Context) error { return nil }

func noReconcile(context.Context) {}

func TestLeaderLoop_PublishesPromptlyOnCacheChange(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, testLeaderTimings(), neverFails, rec.publish, noReconcile)
	}()

	s.applyToCache(&connectors.Message{Key: []byte("topic:cb"), Value: []byte("v")})

	select {
	case <-rec.first:
	case <-time.After(2 * time.Second):
		t.Fatal("no snapshot written within 2s of a cache change; the change-triggered path is not working")
	}

	cancel()
	<-done
}

func TestLeaderLoop_PublishesChangeThatPredatesLeadership(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()

	s.applyToCache(&connectors.Message{Key: []byte("k"), Value: []byte("v")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, testLeaderTimings(), neverFails, rec.publish, noReconcile)
	}()

	select {
	case <-rec.first:
	case <-time.After(2 * time.Second):
		t.Fatal("a change made before leadership was acquired was not snapshotted on becoming leader")
	}

	cancel()
	<-done
}

func TestLeaderLoop_CoalescesBurstIntoFewWrites(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()

	timings := testLeaderTimings()
	timings.debounce = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish, noReconcile)
	}()

	const changes = 100
	for i := 0; i < changes; i++ {
		s.applyToCache(&connectors.Message{Key: []byte("k"), Value: []byte("v")})
	}

	select {
	case <-rec.first:
	case <-time.After(2 * time.Second):
		t.Fatal("expected at least one snapshot after the burst")
	}
	time.Sleep(200 * time.Millisecond)

	cancel()
	<-done

	if got := rec.count(); got >= changes {
		t.Fatalf("debounce did not coalesce: %d writes for %d changes", got, changes)
	}
}

func TestRenewWithRetry_SucceedsWithoutRetryingWhenHealthy(t *testing.T) {
	s := newTestCompactedState()
	renew, calls := failNTimes(0, nil)

	if err := s.renewWithRetry(context.Background(), testLeaderTimings(), renew); err != nil {
		t.Fatalf("renewWithRetry returned error: %v", err)
	}
	if *calls != 1 {
		t.Fatalf("expected a healthy renewal to be attempted once, got %d attempts", *calls)
	}
}

func TestRenewWithRetry_RidesOutTransientLinkFailure(t *testing.T) {
	s := newTestCompactedState()
	renew, calls := failNTimes(1, errors.New("context deadline exceeded"))

	if err := s.renewWithRetry(context.Background(), testLeaderTimings(), renew); err != nil {
		t.Fatalf("expected renewal to recover after a transient failure, got error: %v", err)
	}
	if *calls != 2 {
		t.Fatalf("expected 2 attempts (one failure then success), got %d", *calls)
	}
}

func TestRenewWithRetry_ConcedesAfterTheAttemptLimit(t *testing.T) {
	s := newTestCompactedState()
	renew, calls := failNTimes(1<<30, errors.New("namespace unreachable"))

	if err := s.renewWithRetry(context.Background(), testLeaderTimings(), renew); err == nil {
		t.Fatal("expected renewWithRetry to concede once attempts are exhausted")
	}
	if *calls != 2 {
		t.Fatalf("expected exactly 2 attempts, got %d", *calls)
	}
}

func TestRenewWithRetry_ConcedesImmediatelyWhenLockIsLost(t *testing.T) {
	s := newTestCompactedState()
	renew, calls := failNTimes(1<<30, &azservicebus.Error{Code: azservicebus.CodeLockLost})

	if err := s.renewWithRetry(context.Background(), testLeaderTimings(), renew); err == nil {
		t.Fatal("expected a lost lock to end the leadership term")
	}
	if *calls != 1 {
		t.Fatalf("expected a lost lock to concede without retrying, got %d attempts", *calls)
	}
}

func TestRenewWithRetry_StopsOnContextCancel(t *testing.T) {
	s := newTestCompactedState()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	renew, _ := failNTimes(1<<30, errors.New("namespace unreachable"))
	if err := s.renewWithRetry(ctx, testLeaderTimings(), renew); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLeaderLoop_KeepsLeadershipThroughTransientRenewalFailure(t *testing.T) {
	s := newTestCompactedState()

	timings := testLeaderTimings()
	timings.renewInterval = 10 * time.Millisecond

	renew, _ := failNTimes(1, errors.New("context deadline exceeded"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, renew, neverFails, noReconcile)
	}()

	select {
	case <-done:
		t.Fatal("loop surrendered leadership on a transient renewal failure that recovered")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	<-done
}

func TestLeaderLoop_StepsDownWhenLockRenewalFails(t *testing.T) {
	s := newTestCompactedState()

	timings := testLeaderTimings()
	timings.renewInterval = 10 * time.Millisecond

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(context.Background(), timings,
			func(context.Context) error { return errors.New("lock lost") },
			neverFails,
			noReconcile,
		)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not surrender leadership after a failed lock renewal")
	}
}

func TestLeaderLoop_SurvivesPublishFailure(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()
	rec.err = errors.New("transient azure failure")

	s.applyToCache(&connectors.Message{Key: []byte("k"), Value: []byte("v")})
	s.drainDirty()

	timings := testLeaderTimings()
	timings.snapshotInterval = 15 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish, noReconcile)
	}()

	select {
	case <-rec.first:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a first (failing) publish attempt")
	}
	time.Sleep(100 * time.Millisecond)

	select {
	case <-done:
		t.Fatal("loop exited on a publish failure; it should retry on the next trigger")
	default:
	}

	if got := rec.count(); got < 2 {
		t.Fatalf("expected the loop to retry after a failed write, got %d attempts", got)
	}

	cancel()
	<-done
}

func TestLeaderLoop_ReturnsOnContextCancel(t *testing.T) {
	s := newTestCompactedState()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, testLeaderTimings(), neverFails, neverFails, noReconcile)
	}()

	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop did not return promptly on context cancellation")
	}
}

func TestLeaderLoop_ReturnsWhenCancelledDuringDebounce(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()

	timings := testLeaderTimings()
	timings.debounce = 3 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish, noReconcile)
	}()

	s.applyToCache(&connectors.Message{Key: []byte("k"), Value: []byte("v")})
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("loop blocked in the debounce window instead of honouring cancellation")
	}

	if got := rec.count(); got != 1 {
		t.Fatalf("expected the debounced change to be flushed on shutdown, got %d writes", got)
	}
}

func TestRunLeaderElection_WaitsRatherThanContendingWhileUnseeded(t *testing.T) {
	// A snapshot written from an unseeded cache erases every subscriber this
	// instance never saw, so it must not contend. The driver is nil, so contending
	// would panic — surviving is the proof it did not.
	//
	// It waits rather than returning: the live tail retries seeding, and an
	// instance that gave up here stayed demoted for the life of the process, which
	// on a single-instance deployment means no snapshot is ever written again.
	s := newTestCompactedState()
	if s.seeded {
		t.Fatal("test premise broken: expected a fresh state to be unseeded")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderElection(ctx)
	}()

	select {
	case <-done:
		t.Fatal("stopped waiting while unseeded; a seed completing later could never promote this instance")
	case <-time.After(200 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("waiting for a seed ignored cancellation; shutdown would block")
	}
}

func TestLeaderLoop_SkipsIntervalPublishWhenNothingChanged(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()

	timings := testLeaderTimings()
	timings.snapshotInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish, noReconcile)
	}()

	time.Sleep(150 * time.Millisecond) // ~15 ticks
	cancel()
	<-done

	if got := rec.count(); got != 0 {
		t.Fatalf("expected no writes on a quiet topic, got %d — an idle leader would rewrite the whole snapshot every tick", got)
	}
}

func TestLeaderLoop_IntervalPublishesOnceThenStops(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()

	s.applyToCache(&connectors.Message{Key: []byte("k"), Value: []byte("v")})
	s.drainDirty()

	timings := testLeaderTimings()
	timings.snapshotInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish, noReconcile)
	}()

	time.Sleep(150 * time.Millisecond) // ~15 ticks, one pending change
	cancel()
	<-done

	if got := rec.count(); got != 1 {
		t.Fatalf("expected the pending change to be written exactly once, got %d", got)
	}
}

func TestLeaderLoop_ShutdownFlushSkippedWhenNothingPending(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()

	timings := testLeaderTimings()
	timings.snapshotInterval = time.Hour

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish, noReconcile)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()
	<-done

	if got := rec.count(); got != 0 {
		t.Fatalf("expected no shutdown write with nothing pending, got %d", got)
	}
}

func TestMarkPublished_ReportsHowLongTheChangeWasUnpublished(t *testing.T) {
	s := newTestCompactedState()

	s.applyToCache(&connectors.Message{Key: []byte("topic:cb"), Value: []byte(`{"active":true}`)})
	time.Sleep(15 * time.Millisecond)

	exposure, subscribers := s.markPublished(s.cacheVersion())

	if exposure < 10*time.Millisecond {
		t.Fatalf("expected the reported window to cover the wait, got %s", exposure)
	}
	if subscribers != 1 {
		t.Fatalf("expected the snapshot to hold 1 subscriber, got %d", subscribers)
	}
}

func TestMarkPublished_ClockStartsAtTheOldestUnpublishedChange(t *testing.T) {
	s := newTestCompactedState()

	s.applyToCache(&connectors.Message{Key: []byte("first"), Value: []byte("a")})
	time.Sleep(15 * time.Millisecond)
	s.applyToCache(&connectors.Message{Key: []byte("second"), Value: []byte("b")})

	exposure, _ := s.markPublished(s.cacheVersion())

	if exposure < 10*time.Millisecond {
		t.Fatalf("window measured from the newest change, not the oldest: got %s", exposure)
	}
}

func TestMarkPublished_WindowResetsAfterAWrite(t *testing.T) {
	s := newTestCompactedState()

	s.applyToCache(&connectors.Message{Key: []byte("k"), Value: []byte("v")})
	if exposure, _ := s.markPublished(s.cacheVersion()); exposure == 0 {
		t.Fatal("expected a non-zero window for a change that was actually pending")
	}

	if exposure, _ := s.markPublished(s.cacheVersion()); exposure != 0 {
		t.Fatalf("expected no window when the cache and snapshot already agree, got %s", exposure)
	}
}

func TestSeedPredatesSubscription_DetectsTheStartupWindow(t *testing.T) {
	created := time.Now()
	cases := map[string]struct {
		snapshotReadAt, subscriptionCreatedAt time.Time
		wantBehind                            bool
		wantWindow                            time.Duration
	}{
		"snapshot older than the subscription": {
			snapshotReadAt: created.Add(-8 * time.Second), subscriptionCreatedAt: created,
			wantBehind: true, wantWindow: 8 * time.Second,
		},
		"snapshot newer than the subscription": {
			snapshotReadAt: created.Add(2 * time.Second), subscriptionCreatedAt: created,
		},
		"empty snapshot queue": {
			subscriptionCreatedAt: created,
		},
		"subscription never opened": {
			snapshotReadAt: created,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			s := &compactedTopicState{
				snapshotReadAt:        tc.snapshotReadAt,
				subscriptionCreatedAt: tc.subscriptionCreatedAt,
			}

			behind, window := s.seedPredatesSubscription()
			if behind != tc.wantBehind {
				t.Fatalf("behind = %v, want %v", behind, tc.wantBehind)
			}
			if window != tc.wantWindow {
				t.Errorf("window = %s, want %s", window, tc.wantWindow)
			}
		})
	}
}

func TestRunLeaderElection_HoldBackHonoursCancellation(t *testing.T) {
	s := &compactedTopicState{topic: "cache-holdback", seeded: true, electionHoldBack: time.Hour}

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	s.runLeaderElection(ctx)

	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("returned after %s; the hold-back ignored cancellation", elapsed)
	}
}

func TestWaitUntilSeeded_ProceedsImmediatelyWhenAlreadySeeded(t *testing.T) {
	s := newTestCompactedState()
	s.seeded = true

	start := time.Now()
	if !s.waitUntilSeeded(context.Background()) {
		t.Fatal("reported a failure for a cache that was already seeded")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waited %s on an already-seeded cache; the hold-back would be paid twice", elapsed)
	}
}

func TestSeedFromSnapshot_RefusesToSeedOverLiveChanges(t *testing.T) {
	s := newTestCompactedState()
	s.applyToCache(&connectors.Message{Key: []byte("topic:cb"), Value: []byte(`{"state":"active"}`)})

	err := s.seedFromSnapshot(context.Background())
	if !errors.Is(err, errCacheAlreadyLive) {
		t.Fatalf("seedFromSnapshot() error = %v, want errCacheAlreadyLive", err)
	}
	if got := len(s.cache); got != 1 {
		t.Fatalf("cache holds %d keys, want 1 — the live change was not preserved", got)
	}
}

func TestShouldSeedLeaderSession_AsksTheQueueNotTheError(t *testing.T) {
	cases := map[string]struct {
		active   int32
		probeErr error
		want     bool
	}{
		"empty queue needs an anchor":       {0, nil, true},
		"anchor already present":            {1, nil, false},
		"several anchors already present":   {2, nil, false},
		"probe failed, seed to stay usable": {0, errors.New("probe unavailable"), true},
		"probe failed despite an anchor":    {1, errors.New("probe unavailable"), true},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			if got := shouldSeedLeaderSession(tc.active, tc.probeErr); got != tc.want {
				t.Fatalf("shouldSeedLeaderSession(%d, %v) = %v, want %v",
					tc.active, tc.probeErr, got, tc.want)
			}
		})
	}
}

func queueRuntimePropertiesXML(activeCount int) string {
	return `<entry xmlns="http://www.w3.org/2005/Atom">` +
		`<title type="text">lock-queue</title>` +
		`<content type="application/xml">` +
		`<QueueDescription xmlns="http://schemas.microsoft.com/netservices/2010/10/servicebus/connect" ` +
		`xmlns:i="http://www.w3.org/2001/XMLSchema-instance">` +
		`<LockDuration>PT1M</LockDuration>` +
		`<RequiresSession>true</RequiresSession>` +
		`<MaxDeliveryCount>10</MaxDeliveryCount>` +
		// Required: the SDK parses these as RFC3339 and errors on an empty value.
		`<AccessedAt>2026-10-05T12:00:00Z</AccessedAt>` +
		`<CreatedAt>2026-10-05T11:00:00Z</CreatedAt>` +
		`<UpdatedAt>2026-10-05T11:00:00Z</UpdatedAt>` +
		fmt.Sprintf(`<MessageCount>%d</MessageCount>`, activeCount) +
		`<CountDetails xmlns:d2p1="http://schemas.microsoft.com/netservices/2011/06/servicebus">` +
		fmt.Sprintf(`<d2p1:ActiveMessageCount>%d</d2p1:ActiveMessageCount>`, activeCount) +
		`<d2p1:DeadLetterMessageCount>0</d2p1:DeadLetterMessageCount>` +
		`</CountDetails>` +
		`</QueueDescription></content></entry>`
}

func TestSeedLeaderSession_ExistingAnchorIsNotReseeded(t *testing.T) {
	// The leak: an anchor is never consumed, so a second one stays forever. The
	// queue's own count is the only reliable signal, because an acquisition can
	// fail as a held session, a Service Bus timeout, or its own context deadline,
	// and only the first two are distinguishable from the error.
	sender := &fakeSender{}
	driver, _ := mockAdmin(t, func(req *http.Request) (*http.Response, error) {
		body := queueRuntimePropertiesXML(1)
		return &http.Response{
			StatusCode:    http.StatusOK,
			Status:        "200 OK",
			Header:        http.Header{"Content-Type": []string{"application/atom+xml"}},
			Body:          io.NopCloser(strings.NewReader(body)),
			ContentLength: int64(len(body)),
			Request:       req,
		}, nil
	})
	driver.publisher = testPublisher(sender)
	s := &compactedTopicState{
		topic:     testTopic,
		lockQueue: leaderLockQueueName(testTopic),
		driver:    driver,
	}

	// The election loop calls this on every failed attempt, a few seconds apart.
	for i := 0; i < 20; i++ {
		s.seedLeaderSessionBestEffort(context.Background())
	}

	if len(sender.sent) != 0 {
		t.Fatalf("sent %d anchors while one already existed, want 0 — every extra anchor "+
			"stays in the lock queue permanently", len(sender.sent))
	}
}

func TestSeedLeaderSession_UnreadableQueueStillSeeds(t *testing.T) {
	sender := &fakeSender{}
	driver, _ := mockAdmin(t, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("probe unavailable")
	})
	driver.publisher = testPublisher(sender)
	s := &compactedTopicState{
		topic:     testTopic,
		lockQueue: leaderLockQueueName(testTopic),
		driver:    driver,
	}

	s.seedLeaderSessionBestEffort(context.Background())

	if len(sender.sent) != 1 {
		t.Fatalf("sent %d anchors, want 1 — an unreadable queue must not stop the seed, "+
			"or a transient admin failure leaves the topic permanently leaderless", len(sender.sent))
	}
	if got := sender.sent[0]; got.SessionID == nil || *got.SessionID != leaderSessionID {
		t.Fatalf("anchor session = %v, want %q — without it the message creates no session",
			got.SessionID, leaderSessionID)
	}
	if got := sender.sent[0]; got.MessageID == nil || *got.MessageID != leaderSeedMessageID {
		t.Fatalf("anchor message id = %v, want %q — duplicate detection is the backstop when "+
			"two instances probe an empty queue at the same moment", got.MessageID, leaderSeedMessageID)
	}
}

// fakeSnapshotReceiver replays scripted peek and receive batches, so the paging
// and the mark boundary can be exercised without a namespace.
type fakeSnapshotReceiver struct {
	peekPages [][]*azservicebus.ReceivedMessage
	peekFrom  []int64 // FromSequenceNumber per call; 0 when unset
	peekErr   error

	recvBatches [][]*azservicebus.ReceivedMessage
	recvSizes   []int
	recvErr     error

	completed []int64
	abandoned []int64
}

func snapshotMessage(seq int64) *azservicebus.ReceivedMessage {
	return &azservicebus.ReceivedMessage{SequenceNumber: &seq, Body: []byte("{}")}
}

func (f *fakeSnapshotReceiver) PeekMessages(_ context.Context, _ int,
	options *azservicebus.PeekMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	if f.peekErr != nil {
		return nil, f.peekErr
	}
	var from int64
	if options != nil && options.FromSequenceNumber != nil {
		from = *options.FromSequenceNumber
	}
	f.peekFrom = append(f.peekFrom, from)
	if len(f.peekPages) == 0 {
		return nil, nil
	}
	page := f.peekPages[0]
	f.peekPages = f.peekPages[1:]
	return page, nil
}

func (f *fakeSnapshotReceiver) ReceiveMessages(_ context.Context, maxMessages int,
	_ *azservicebus.ReceiveMessagesOptions) ([]*azservicebus.ReceivedMessage, error) {
	if f.recvErr != nil {
		return nil, f.recvErr
	}
	f.recvSizes = append(f.recvSizes, maxMessages)
	if len(f.recvBatches) == 0 {
		return nil, nil
	}
	batch := f.recvBatches[0]
	f.recvBatches = f.recvBatches[1:]
	return batch, nil
}

func (f *fakeSnapshotReceiver) CompleteMessage(_ context.Context, msg *azservicebus.ReceivedMessage,
	_ *azservicebus.CompleteMessageOptions) error {
	f.completed = append(f.completed, *msg.SequenceNumber)
	return nil
}

func (f *fakeSnapshotReceiver) AbandonMessage(_ context.Context, msg *azservicebus.ReceivedMessage,
	_ *azservicebus.AbandonMessageOptions) error {
	f.abandoned = append(f.abandoned, *msg.SequenceNumber)
	return nil
}

func TestPeekAllSnapshots_PagesPastTheFirstWindow(t *testing.T) {
	// A fixed window would stop at the first page and seed from seq 2, which is
	// precisely how a failed cleanup turns into a silently stale restore.
	f := &fakeSnapshotReceiver{peekPages: [][]*azservicebus.ReceivedMessage{
		{snapshotMessage(1), snapshotMessage(2)},
		{snapshotMessage(3)},
	}}

	newest, seqs, err := peekAllSnapshots(context.Background(), f)
	if err != nil {
		t.Fatalf("peekAllSnapshots: %v", err)
	}
	if *newest.SequenceNumber != 3 {
		t.Errorf("newest snapshot is seq %d, want 3: the page beyond the first window was not read",
			*newest.SequenceNumber)
	}
	if len(seqs) != 3 {
		t.Errorf("found %d sequence numbers, want 3", len(seqs))
	}
	if want := []int64{0, 3, 4}; !slices.Equal(f.peekFrom, want) {
		t.Errorf("peeked from %v, want %v: the cursor must advance past the last sequence seen", f.peekFrom, want)
	}
}

func TestPeekAllSnapshots_ReportsNothingForAnEmptyQueue(t *testing.T) {
	newest, seqs, err := peekAllSnapshots(context.Background(), &fakeSnapshotReceiver{})
	if err != nil {
		t.Fatalf("peekAllSnapshots: %v", err)
	}
	if newest != nil || len(seqs) != 0 {
		t.Errorf("got newest=%v seqs=%v, want nothing for an empty queue", newest, seqs)
	}
}

func TestSettleStaleSnapshots_CollectsEveryCopyBelowTheMark(t *testing.T) {
	// The convergence property: one pass clears whatever earlier passes left,
	// so the queue returns to a single message rather than drifting upward.
	f := &fakeSnapshotReceiver{recvBatches: [][]*azservicebus.ReceivedMessage{
		{snapshotMessage(1), snapshotMessage(2), snapshotMessage(3)},
		{snapshotMessage(4), snapshotMessage(5)},
	}}

	deleted, err := settleStaleSnapshots(context.Background(), f, 5, 5)
	if err != nil {
		t.Fatalf("settleStaleSnapshots: %v", err)
	}
	if deleted != 5 {
		t.Errorf("deleted %d, want 5", deleted)
	}
	if want := []int64{1, 2, 3, 4, 5}; !slices.Equal(f.completed, want) {
		t.Errorf("completed %v, want %v", f.completed, want)
	}
	if len(f.abandoned) != 0 {
		t.Errorf("abandoned %v, want none: every copy was below the mark", f.abandoned)
	}
}

func TestSettleStaleSnapshots_NeverDeletesAboveTheMark(t *testing.T) {
	// count is deliberately an overestimate, as it is on a retry. The mark, not
	// the count, is what protects the newest snapshot from being deleted.
	f := &fakeSnapshotReceiver{recvBatches: [][]*azservicebus.ReceivedMessage{
		{snapshotMessage(1), snapshotMessage(2), snapshotMessage(3)},
	}}

	deleted, err := settleStaleSnapshots(context.Background(), f, 2, 3)
	if err != nil {
		t.Fatalf("settleStaleSnapshots: %v", err)
	}
	if deleted != 2 {
		t.Errorf("deleted %d, want 2", deleted)
	}
	if want := []int64{1, 2}; !slices.Equal(f.completed, want) {
		t.Errorf("completed %v, want %v", f.completed, want)
	}
	if want := []int64{3}; !slices.Equal(f.abandoned, want) {
		t.Errorf("abandoned %v, want %v: the newest snapshot must be released, never settled", f.abandoned, want)
	}
}

func TestSettleStaleSnapshots_SurfacesAReceiveFailure(t *testing.T) {
	// The failure has to reach the caller, or cleanup is never retried and the
	// extra copies stay until the next subscription.
	f := &fakeSnapshotReceiver{recvErr: errors.New("link detached")}

	deleted, err := settleStaleSnapshots(context.Background(), f, 5, 5)
	if err == nil {
		t.Fatal("a receive failure returned no error, so the cleanup would never be retried")
	}
	if deleted != 0 {
		t.Errorf("deleted %d, want 0", deleted)
	}
}

func TestLeaderLoop_ReconcilesWhenThereIsNothingToPublish(t *testing.T) {
	// An unchanged topic never publishes again, so the ticker is the only thing
	// that can collect copies a failed cleanup left behind.
	s := newTestCompactedState()
	rec := newPublishRecorder()

	reconciled := make(chan struct{}, 1)
	reconcile := func(context.Context) {
		select {
		case reconciled <- struct{}{}:
		default:
		}
	}

	timings := testLeaderTimings()
	timings.snapshotInterval = 10 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish, reconcile)
	}()

	select {
	case <-reconciled:
	case <-time.After(2 * time.Second):
		t.Fatal("the ticker never reconciled an idle topic, so stale snapshots would persist indefinitely")
	}
	if rec.count() != 0 {
		t.Errorf("published %d snapshots for an unchanged cache, want 0", rec.count())
	}

	cancel()
	<-done
}

func TestPeekAllSnapshots_ReportsMessagesItCannotPageFrom(t *testing.T) {
	// Paging advances off sequence numbers, so a batch carrying none used to make
	// the loop re-read itself without end.
	f := &fakeSnapshotReceiver{peekPages: [][]*azservicebus.ReceivedMessage{
		{{Body: []byte("{}")}},
	}}

	_, _, err := peekAllSnapshots(context.Background(), f)
	if err == nil {
		t.Fatal("peekAllSnapshots() returned no error for an unpageable batch")
	}
	if !strings.Contains(err.Error(), "no sequence number") {
		t.Errorf("error = %q, want it to name the missing sequence number", err)
	}
}

func TestPeekAllSnapshots_StopsOnACancelledContext(t *testing.T) {
	f := &fakeSnapshotReceiver{peekPages: [][]*azservicebus.ReceivedMessage{
		{snapshotMessage(1)},
	}}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, _, err := peekAllSnapshots(ctx, f); !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want context.Canceled", err)
	}
}

// roundTripSnapshot encodes a cache and decodes it back, so a test asserts on
// what a restarting instance would actually read.
func roundTripSnapshot(t *testing.T, cache map[string][]byte) (map[string][]byte, []byte) {
	t.Helper()
	payload, err := encodeSnapshot(cache)
	if err != nil {
		t.Fatalf("encodeSnapshot returned error: %v", err)
	}
	got := make(map[string][]byte)
	if err := decodeSnapshot(payload, func(key string, value []byte) {
		got[key] = value
	}); err != nil {
		t.Fatalf("decodeSnapshot returned error: %v", err)
	}
	return got, payload
}

func TestSnapshotRoundTrip_WritesJSONValuesWithoutBase64(t *testing.T) {
	value := []byte(`{"callback_url":"https://example.com/hook","state":"active"}`)
	cache := map[string][]byte{"topic:https://example.com/hook": value}

	got, payload := roundTripSnapshot(t, cache)

	// The point of the encoding: the record appears as itself, not re-encoded.
	if !bytes.Contains(payload, value) {
		t.Fatalf("expected the snapshot to hold the record verbatim, got %s", payload)
	}
	if !bytes.Equal(got["topic:https://example.com/hook"], value) {
		t.Fatalf("value did not round-trip: got %s, want %s",
			got["topic:https://example.com/hook"], value)
	}
}

func TestSnapshotRoundTrip_PreservesValuesThatAreNotJSONObjects(t *testing.T) {
	cache := map[string][]byte{
		"plain":    []byte("v"),
		"empty":    {},
		"array":    []byte(`["not","an","object"]`),
		"unparsed": []byte(`{truncated`),
	}

	got, _ := roundTripSnapshot(t, cache)

	if len(got) != len(cache) {
		t.Fatalf("expected %d keys to survive, got %d", len(cache), len(got))
	}
	for key, want := range cache {
		if !bytes.Equal(got[key], want) {
			t.Errorf("key %q did not round-trip: got %q, want %q", key, got[key], want)
		}
	}
}

func TestDecodeSnapshot_ReadsTheBase64FormatItReplaced(t *testing.T) {
	value := []byte(`{"state":"active"}`)
	legacy, err := json.Marshal(map[string][]byte{"topic:cb": value})
	if err != nil {
		t.Fatalf("failed to build a legacy snapshot: %v", err)
	}

	got := make(map[string][]byte)
	if err := decodeSnapshot(legacy, func(key string, v []byte) { got[key] = v }); err != nil {
		t.Fatalf("decodeSnapshot returned error: %v", err)
	}
	if !bytes.Equal(got["topic:cb"], value) {
		t.Fatalf("expected the base64 value to be decoded, got %q, want %q", got["topic:cb"], value)
	}
}

func TestEncodeSnapshot_HoldsMoreSubscribersThanBase64Would(t *testing.T) {
	cache := make(map[string][]byte)
	for i := 0; i < 200; i++ {
		callback := fmt.Sprintf("https://tenant%03d.example.com/webhooks/receive", i)
		cache["login-success:"+callback] = []byte(fmt.Sprintf(
			`{"id":"sub-%03d","topic":"login-success","callback_url":%q,"state":"active"}`, i, callback))
	}

	payload, err := encodeSnapshot(cache)
	if err != nil {
		t.Fatalf("encodeSnapshot returned error: %v", err)
	}
	base64Form, err := json.Marshal(cache)
	if err != nil {
		t.Fatalf("failed to build the base64 form: %v", err)
	}

	if len(payload) >= len(base64Form) {
		t.Fatalf("expected the snapshot to be smaller than the base64 form, got %d vs %d",
			len(payload), len(base64Form))
	}
}

func TestPublishSnapshot_SendsTheCompactEncoding(t *testing.T) {
	// Covers the write path, not just the codec: a snapshot encoded compactly but
	// sent some other way would pass every test above. The peek fails on a
	// cancelled context, which publishSnapshot tolerates by deferring cleanup, so
	// the send still happens and the body can be asserted on.
	value := []byte(`{"callback_url":"https://example.com/hook","state":"active"}`)
	sender := &fakeSender{}
	driver, _ := mockAdmin(t, func(*http.Request) (*http.Response, error) {
		return nil, errors.New("admin calls are not part of this path")
	})
	driver.publisher = testPublisher(sender)
	s := &compactedTopicState{
		topic:         testTopic,
		snapshotQueue: snapshotQueueName(testTopic),
		driver:        driver,
		cache:         map[string][]byte{"topic:https://example.com/hook": value},
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.publishSnapshot(ctx); err != nil {
		t.Fatalf("publishSnapshot returned error: %v", err)
	}

	if len(sender.sent) != 1 {
		t.Fatalf("sent %d snapshots, want 1", len(sender.sent))
	}
	if !bytes.Contains(sender.sent[0].Body, value) {
		t.Fatalf("the snapshot was not sent in the compact encoding, got %s", sender.sent[0].Body)
	}
}
