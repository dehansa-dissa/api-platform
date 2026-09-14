package asb

import (
	"context"
	"errors"
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

func TestSyncSubscriptionName_BoundedForRealisticConsumerID(t *testing.T) {
	consumerID := "3f8a1c72-9b4e-4d21-a5f6-7e0c1d2b3a49-1c2f3a4b5c6d"
	if len("sync-"+consumerID) <= maxSubscriptionNameLength {
		t.Fatalf("test premise broken: unhashed name is only %d chars, expected it to exceed %d",
			len("sync-"+consumerID), maxSubscriptionNameLength)
	}

	assertValidSubscriptionName(t, "syncSubscriptionName()", syncSubscriptionName(consumerID))
}

func TestSyncSubscriptionName_DeterministicAndDistinct(t *testing.T) {
	a := syncSubscriptionName("runtime-1-api-a")
	if a != syncSubscriptionName("runtime-1-api-a") {
		t.Fatal("syncSubscriptionName must be deterministic for the same consumerID")
	}
	if a == syncSubscriptionName("runtime-1-api-b") {
		t.Fatal("syncSubscriptionName should differ across distinct consumerIDs")
	}
}

func TestSyncAndCacheSubscriptionNames_NeverCollide(t *testing.T) {
	id := "runtime-instance-1"
	if syncSubscriptionName(id) == cacheSubscriptionName(id) {
		t.Fatal("sync and cache subscription names must never collide for the same identity")
	}
}

func TestSubscriptionNames_BoundedRegardlessOfInputLength(t *testing.T) {
	longID := ""
	for i := 0; i < 500; i++ {
		longID += "x"
	}
	for name, got := range map[string]string{
		"cacheSubscriptionName": cacheSubscriptionName(longID),
		"syncSubscriptionName":  syncSubscriptionName(longID),
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

func TestLeaderLoop_PublishesPromptlyOnCacheChange(t *testing.T) {
	s := newTestCompactedState()
	rec := newPublishRecorder()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, testLeaderTimings(), neverFails, rec.publish)
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
		s.runLeaderLoop(ctx, testLeaderTimings(), neverFails, rec.publish)
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
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish)
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

// The regression this whole change exists for: Azure force-detaches an idle
// AMQP link every 10 minutes, so a renewal fails while the SDK rebuilds it.
// Conceding on that single error surrendered leadership on a ~10-minute cycle.
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

// Bounded deliberately: a dropped connection releases the session lock
// server-side, so every further renewal is guaranteed to fail and each one
// delays the re-election that actually restores a leader.
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

// A lost lock is not a transport failure: the session is provably gone and
// another instance may already hold it, so retrying risks two leaders.
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
		s.runLeaderLoop(ctx, timings, renew, neverFails)
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

	// A pending change is what the interval exists to rescue. Draining the
	// dirty signal leaves only the interval path able to fire, so the retries
	// counted below are the interval's, not the change trigger's.
	s.applyToCache(&connectors.Message{Key: []byte("k"), Value: []byte("v")})
	s.drainDirty()

	timings := testLeaderTimings()
	timings.snapshotInterval = 15 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish)
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
		s.runLeaderLoop(ctx, testLeaderTimings(), neverFails, neverFails)
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
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish)
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

func TestRunLeaderElection_RefusesLeadershipWhenCacheIsUnseeded(t *testing.T) {
	s := newTestCompactedState()
	if s.seeded {
		t.Fatal("test premise broken: expected a fresh state to be unseeded")
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		s.runLeaderElection(context.Background())
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("unseeded instance contended for leadership; it would overwrite the snapshot with a partial cache")
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
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish)
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
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish)
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
		s.runLeaderLoop(ctx, timings, neverFails, rec.publish)
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
