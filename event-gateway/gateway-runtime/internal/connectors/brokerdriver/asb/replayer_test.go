package asb

import "testing"

// event-gateway topic names are always a 64-character SHA-256 hex digest
// (binding.JoinNormalizedTopic) — this is the realistic input shape for every
// name-derivation helper below.
const testTopic = "1c2f3a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60"

func TestSnapshotQueueName_DeterministicAndSuffixed(t *testing.T) {
	got, err := snapshotQueueName(testTopic)
	if err != nil {
		t.Fatalf("snapshotQueueName returned error: %v", err)
	}
	want := testTopic + "-snapshot"
	if got != want {
		t.Fatalf("snapshotQueueName() = %q, want %q", got, want)
	}
	if len(got) > maxEntityNameLength {
		t.Fatalf("snapshotQueueName() length %d exceeds ASB limit %d", len(got), maxEntityNameLength)
	}
}

func TestLeaderLockQueueName_DeterministicAndSuffixed(t *testing.T) {
	got, err := leaderLockQueueName(testTopic)
	if err != nil {
		t.Fatalf("leaderLockQueueName returned error: %v", err)
	}
	want := testTopic + "-leader-lock"
	if got != want {
		t.Fatalf("leaderLockQueueName() = %q, want %q", got, want)
	}
}

func TestSnapshotAndLockQueueNames_NeverCollide(t *testing.T) {
	snapshot, _ := snapshotQueueName(testTopic)
	lock, _ := leaderLockQueueName(testTopic)
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
	if err := validateSubscriptionName(a); err != nil {
		t.Fatalf("cacheSubscriptionName() = %q is not a valid ASB subscription name: %v", a, err)
	}

	other := cacheSubscriptionName("runtime-instance-2")
	if a == other {
		t.Fatalf("cacheSubscriptionName should differ across distinct runtimeIDs, both were %q", a)
	}
}

// TestSyncSubscriptionName_BoundedForRealisticConsumerID guards the exact case
// that motivated hashing: SyncWatcher composes consumerID as
// "<runtimeID>-<apiID>", where an auto-generated runtimeID is a 36-character
// UUID and apiID is a 12-character truncated hash. Prefixed with "sync-", the
// unhashed form is 54 characters — over ASB's 50-character limit — which made
// Watch fail and silently disabled cross-instance subscription sync.
func TestSyncSubscriptionName_BoundedForRealisticConsumerID(t *testing.T) {
	consumerID := "3f8a1c72-9b4e-4d21-a5f6-7e0c1d2b3a49-1c2f3a4b5c6d"
	if len("sync-"+consumerID) <= maxSubscriptionNameLength {
		t.Fatalf("test premise broken: unhashed name is only %d chars, expected it to exceed %d",
			len("sync-"+consumerID), maxSubscriptionNameLength)
	}

	got := syncSubscriptionName(consumerID)
	if err := validateSubscriptionName(got); err != nil {
		t.Fatalf("syncSubscriptionName() = %q is not a valid ASB subscription name: %v", got, err)
	}
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

// Watch and the live-tail cache both subscribe to the same sync topic, so
// their names must never collide for the same underlying identity.
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
		if err := validateSubscriptionName(got); err != nil {
			t.Fatalf("%s() with a long input produced an invalid name %q: %v", name, got, err)
		}
	}
}
